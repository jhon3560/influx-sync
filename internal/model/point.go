// Package model 定义同步系统核心数据模型。
package model

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Point 对应 InfluxDB 一条时间序列数据点。
type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   int64 // 纳秒
}

// Batch 一个查询窗口内的数据集合，用于 WAL 组帧。
type Batch struct {
	Points    []Point
	StartTime int64 // 窗口起始（纳秒，含）
	EndTime   int64 // 窗口结束（纳秒，不含）
}

// Key 返回点的唯一标识（measurement+tags+timestamp），用于窗口内查重。
func (p *Point) Key() string {
	var sb strings.Builder
	sb.WriteString(p.Measurement)
	sb.WriteByte('|')
	keys := make([]string, 0, len(p.Tags))
	for k := range p.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(p.Tags[k])
		sb.WriteByte('|')
	}
	sb.WriteString(fmt.Sprintf("%d", p.Timestamp))
	return sb.String()
}

// LineProtocol 将点序列化为 InfluxDB Line Protocol 一行。
// 字段值支持 float64/int64/string/bool，其余类型跳过并返回错误。
func (p *Point) LineProtocol() (string, error) {
	var sb strings.Builder
	sb.Grow(128)
	// measurement：转义逗号和空格
	sb.WriteString(escapeMeasurement(p.Measurement))

	// tags：按 key 排序保证输出稳定
	tagKeys := make([]string, 0, len(p.Tags))
	for k := range p.Tags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		sb.WriteByte(',')
		sb.WriteString(escapeTag(k))
		sb.WriteByte('=')
		sb.WriteString(escapeTag(p.Tags[k]))
	}

	// fields：至少一个
	if len(p.Fields) == 0 {
		return "", fmt.Errorf("point %s has no fields", p.Key())
	}
	fieldKeys := make([]string, 0, len(p.Fields))
	for k := range p.Fields {
		fieldKeys = append(fieldKeys, k)
	}
	sort.Strings(fieldKeys)
	sb.WriteByte(' ')
	first := true
	for _, k := range fieldKeys {
		// NaN/Inf 字段跳过（InfluxDB 拒绝 NaN，写入会整帧 400 成毒丸）
		if isNaNOrInf(p.Fields[k]) {
			continue
		}
		if !first {
			sb.WriteByte(',')
		}
		first = false
		sb.WriteString(escapeTag(k))
		sb.WriteByte('=')
		if err := writeFieldValue(&sb, p.Fields[k]); err != nil {
			return "", fmt.Errorf("field %q: %w", k, err)
		}
	}
	if first {
		// 所有字段都是 NaN/Inf：整点跳过（无可用字段）
		return "", errSkipPoint
	}

	sb.WriteByte(' ')
	sb.WriteString(fmt.Sprintf("%d", p.Timestamp))
	return sb.String(), nil
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

func escapeMeasurement(s string) string {
	r := strings.NewReplacer(",", `\,`, " ", `\ `)
	return r.Replace(s)
}

func escapeTag(s string) string {
	r := strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `)
	return r.Replace(s)
}

func writeFieldValue(sb *strings.Builder, v interface{}) error {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return fmt.Errorf("NaN/Inf float field")
		}
		sb.WriteString(fmt.Sprintf("%v", t))
	case int64:
		sb.WriteString(fmt.Sprintf("%di", t))
	case int:
		sb.WriteString(fmt.Sprintf("%di", t))
	case string:
		// line protocol 字符串：转义反斜杠与双引号（否则破坏行解析）
		sb.WriteByte('"')
		sb.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(t))
		sb.WriteByte('"')
	case bool:
		if t {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	default:
		return fmt.Errorf("unsupported field type %T", v)
	}
	return nil
}
