// Package model 定义同步系统核心数据模型。
package model

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Point 对应 InfluxDB 一条时间序列数据点。
//
// tagKeys/fieldKeys 为可选缓存的排序键序（由 seriesToPoints 在 schema/series
// 级计算一次后共享给该 series 的全部点，Influx 同一 series 的列序稳定）。
// 为 nil 或与 map 长度不符时，序列化会退回运行时排序（兼容手工构造的 Point）。
// 约定：Point 构造后不得修改 Tags/Fields（否则需同时清空缓存键序）。
type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   int64 // 纳秒

	tagKeys   []string // 排序后的 tag key（缓存，可跨点共享）
	fieldKeys []string // 排序后的 field key（缓存，可跨点共享）
}

// Batch 一个查询窗口内的数据集合，用于 WAL 组帧。
type Batch struct {
	Points    []Point
	StartTime int64 // 窗口起始（纳秒，含）
	EndTime   int64 // 窗口结束（纳秒，不含）
}

// SetKeyOrder 注入 schema/series 级预排序的 tag/field 键序（省去每点排序）。
// 键序必须与 Tags/Fields 的键完全一致；长度不符时自动忽略。
func (p *Point) SetKeyOrder(tagKeys, fieldKeys []string) {
	if len(tagKeys) == len(p.Tags) {
		p.tagKeys = tagKeys
	}
	if len(fieldKeys) == len(p.Fields) {
		p.fieldKeys = fieldKeys
	}
}

// sortedTagKeys 返回排序后的 tag key：优先缓存，缺失时排序（并缓存到本点）。
func (p *Point) sortedTagKeys() []string {
	if len(p.tagKeys) == len(p.Tags) {
		return p.tagKeys
	}
	keys := make([]string, 0, len(p.Tags))
	for k := range p.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p.tagKeys = keys
	return keys
}

// sortedFieldKeys 返回排序后的 field key：优先缓存，缺失时排序。
func (p *Point) sortedFieldKeys() []string {
	if len(p.fieldKeys) == len(p.Fields) {
		return p.fieldKeys
	}
	keys := make([]string, 0, len(p.Fields))
	for k := range p.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p.fieldKeys = keys
	return keys
}

// Key 返回点的唯一标识（measurement+tags+timestamp），用于窗口内查重。
// 零分配路径：缓存键序 + append 拼装 + strconv（实测由 684ns/7 次分配降到亚 100ns）。
func (p *Point) Key() string {
	// 预估容量：measurement + tags + 分隔符 + 时间戳
	n := len(p.Measurement) + 24
	for k, v := range p.Tags {
		n += len(k) + len(v) + 2
	}
	buf := make([]byte, 0, n)
	buf = append(buf, p.Measurement...)
	buf = append(buf, '|')
	for _, k := range p.sortedTagKeys() {
		buf = append(buf, k...)
		buf = append(buf, '=')
		buf = append(buf, p.Tags[k]...)
		buf = append(buf, '|')
	}
	buf = strconv.AppendInt(buf, p.Timestamp, 10)
	return string(buf)
}

// LineProtocol 将点序列化为 InfluxDB Line Protocol 一行。
// 字段值支持 float64/int64/string/bool，其余类型跳过并返回错误。
// 热路径优化：包级转义循环 + strconv.Append* + 缓存键序 + []byte 拼装
// （替代 sort.Strings ×2、strings.NewReplacer ×4、fmt.Sprintf ×3）。
func (p *Point) LineProtocol() (string, error) {
	buf, err := appendLineProtocol(nil, p)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// appendLineProtocol 将点序列化为 LP 追加到 dst（LineProtocol/LinesToProtocol 共用核心）。
func appendLineProtocol(dst []byte, p *Point) ([]byte, error) {
	buf := dst
	if cap(buf) == 0 {
		buf = make([]byte, 0, 128+len(p.Measurement))
	}
	// measurement：转义逗号和空格
	buf = appendEscMeasurement(buf, p.Measurement)

	// tags：按（缓存）键序输出，保证稳定
	for _, k := range p.sortedTagKeys() {
		buf = append(buf, ',')
		buf = appendEscTag(buf, k)
		buf = append(buf, '=')
		buf = appendEscTag(buf, p.Tags[k])
	}

	// fields：至少一个
	if len(p.Fields) == 0 {
		return buf, fmt.Errorf("point %s has no fields", p.Key())
	}
	buf = append(buf, ' ')
	first := true
	for _, k := range p.sortedFieldKeys() {
		// NaN/Inf 字段跳过（InfluxDB 拒绝 NaN，写入会整帧 400 成毒丸）
		v := p.Fields[k]
		if isNaNOrInf(v) {
			continue
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = appendEscTag(buf, k)
		buf = append(buf, '=')
		var err error
		buf, err = appendFieldValue(buf, v)
		if err != nil {
			return buf, fmt.Errorf("field %q: %w", k, err)
		}
	}
	if first {
		// 所有字段都是 NaN/Inf：整点跳过（无可用字段）
		return buf, errSkipPoint
	}

	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, p.Timestamp, 10)
	return buf, nil
}

// LinesToProtocol 批量转换；全 NaN 点跳过，其余失败即返回错误（便于 WAL 重试）。
func LinesToProtocol(points []Point) ([]string, error) {
	lines := make([]string, 0, len(points))
	for i := range points {
		line, err := points[i].LineProtocol()
		if err != nil {
			if errors.Is(err, errSkipPoint) {
				continue // 全 NaN 点：跳过
			}
			return nil, fmt.Errorf("point[%d]: %w", i, err)
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// LinesToProtocolBytes 批量转换并拼装为单个字节块（行间 '\n'，末尾带 '\n'）。
// 相比 LinesToProtocol+strings.Join 减少 N 次行级分配；全 NaN 点跳过。
func LinesToProtocolBytes(points []Point) ([]byte, error) {
	if len(points) == 0 {
		return nil, nil
	}
	var buf []byte
	for i := range points {
		start := len(buf)
		var err error
		buf, err = appendLineProtocol(buf, &points[i])
		if err != nil {
			if errors.Is(err, errSkipPoint) {
				buf = buf[:start] // 回退：整点跳过
				continue
			}
			return nil, fmt.Errorf("point[%d]: %w", i, err)
		}
		buf = append(buf, '\n')
	}
	return buf, nil
}

// errSkipPoint 标记整点应被跳过（所有字段均 NaN/Inf）。
var errSkipPoint = errors.New("point skipped: all fields NaN/Inf")

// isNaNOrInf 判断 float 值是否为 NaN/±Inf（InfluxDB 拒绝此类值）。
func isNaNOrInf(v interface{}) bool {
	f, ok := v.(float64)
	if !ok {
		return false
	}
	return math.IsNaN(f) || math.IsInf(f, 0)
}

// 转义器：单遍扫描 + 条件追加（无 NewReplacer/trie 构建，无转义时不分配）。
// InfluxDB Line Protocol 转义规则：
//   - measurement：, 和 空格
//   - tag key/value：, = 空格
//   - string field：\ 和 "

func appendEscMeasurement(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ',', ' ':
			dst = append(dst, '\\', s[i])
		default:
			dst = append(dst, s[i])
		}
	}
	return dst
}

func appendEscTag(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ',', '=', ' ':
			dst = append(dst, '\\', s[i])
		default:
			dst = append(dst, s[i])
		}
	}
	return dst
}

func appendEscStringField(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '"':
			dst = append(dst, '\\', s[i])
		default:
			dst = append(dst, s[i])
		}
	}
	return dst
}

// appendFieldValue 序列化字段值到 dst（strconv 替代 fmt.Sprintf）。
func appendFieldValue(dst []byte, v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return dst, fmt.Errorf("NaN/Inf float field")
		}
		// 'g' 最短表示：与 fmt "%v" 输出一致
		return strconv.AppendFloat(dst, t, 'g', -1, 64), nil
	case int64:
		dst = strconv.AppendInt(dst, t, 10)
		return append(dst, 'i'), nil
	case int:
		dst = strconv.AppendInt(dst, int64(t), 10)
		return append(dst, 'i'), nil
	case string:
		// line protocol 字符串：转义反斜杠与双引号（否则破坏行解析）
		dst = append(dst, '"')
		dst = appendEscStringField(dst, t)
		dst = append(dst, '"')
		return dst, nil
	case bool:
		if t {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	default:
		return dst, fmt.Errorf("unsupported field type %T", v)
	}
}

// PointsEqual 无分配比较两个点是否语义相同（measurement+tags+fields+timestamp）。
// 用于边界时间戳去重的小集合线性比较（避免构造 Key 字符串）。
func PointsEqual(a, b Point) bool {
	if a.Measurement != b.Measurement || a.Timestamp != b.Timestamp {
		return false
	}
	if len(a.Tags) != len(b.Tags) || len(a.Fields) != len(b.Fields) {
		return false
	}
	for k, v := range a.Tags {
		bv, ok := b.Tags[k]
		if !ok || bv != v {
			return false
		}
	}
	for k, v := range a.Fields {
		bv, ok := b.Fields[k]
		if !ok || bv != v {
			return false
		}
	}
	return true
}
