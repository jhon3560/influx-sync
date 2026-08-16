// Package model Line Protocol 行解析（A4 fast-path 用）。
//
// 用途：源库 SUBSCRIPTION 推送的批次是客户端原始写入的 Line Protocol 文本。
// fast-path 需要从每行提取 measurement、tag 键值对与时间戳（用于去重集登记与
// measurement 过滤），但**不需要**解析字段值——行本身原样透传，不做重建。
// 解析失败/不满足条件的行由慢路径（Poller）兜底，绝不影响正确性。
package model

import (
	"sort"
	"strconv"
)

// ParseLine 解析一行 Line Protocol，返回：
//   - meas：measurement（反转义后）
//   - tags：tag 键值对（反转义后，保持行内出现顺序）
//   - ts：行尾时间戳（int64，未做精度/范围校验——由调用方决定策略）
//   - ok：解析成功与否。失败的行应整行跳过（由轮询慢路径兜底）。
//
// 解析策略：
//   - 引号字符串字段内的空格/逗号/等号/反斜杠均被正确跳过（跟踪引号与转义态）；
//   - 时间戳 = 最后一个未被引号包裹的空格之后的纯数字 token；
//   - measurement 以首个未转义逗号或空格结束；tag 段以首个未转义空格结束；
//   - `\x` 一律解为字面量 x（LP 转义规则）。
func ParseLine(line []byte) (meas string, tags [][2]string, ts int64, ok bool) {
	// 去掉行尾 \r（HTTP 推送可能带 CRLF）
	for len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) == 0 {
		return "", nil, 0, false
	}
	// 1. 找所有未转义、未被引号包裹的空格位置
	spaces := make([]int, 0, 8)
	inQuote := false
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++ // 跳过转义字符
		case '"':
			inQuote = !inQuote
		case ' ':
			if !inQuote {
				spaces = append(spaces, i)
			}
		}
	}
	if len(spaces) == 0 {
		return "", nil, 0, false // 无字段/时间戳分隔
	}
	// 2. 时间戳 = 最后一个空格之后的 token，必须为纯数字
	tsSep := spaces[len(spaces)-1]
	tsTok := line[tsSep+1:]
	if len(tsTok) == 0 {
		return "", nil, 0, false
	}
	for _, c := range tsTok {
		if c < '0' || c > '9' {
			return "", nil, 0, false
		}
	}
	n, err := strconv.ParseInt(string(tsTok), 10, 64)
	if err != nil {
		return "", nil, 0, false
	}
	// 3. 前缀 = measurement[,tags]
	prefix := line[:tsSep]
	// measurement 结束位置：首个未转义逗号（有 tag）或首个未转义空格（无 tag，= 字段分隔）
	mEnd := len(prefix)
	escaped := false
	for i := 0; i < len(prefix); i++ {
		if escaped {
			escaped = false
			continue
		}
		if prefix[i] == '\\' {
			escaped = true
			continue
		}
		if prefix[i] == ',' || prefix[i] == ' ' {
			mEnd = i
			break
		}
	}
	meas = unescape(prefix[:mEnd])
	if meas == "" {
		return "", nil, 0, false
	}
	// 4. tag 段：mEnd 之后到首个未转义空格（字段分隔）之间
	tagSection := prefix[mEnd:]
	fieldsSep := -1
	escaped = false
	for i := 0; i < len(tagSection); i++ {
		if escaped {
			escaped = false
			continue
		}
		if tagSection[i] == '\\' {
			escaped = true
			continue
		}
		if tagSection[i] == ' ' {
			fieldsSep = i
			break
		}
	}
	if fieldsSep >= 0 {
		tagSection = tagSection[:fieldsSep]
	}
	if len(tagSection) > 0 {
		if tagSection[0] != ',' {
			return "", nil, 0, false // 异常结构：tag 段不以逗号开始
		}
		tagSection = tagSection[1:]
	}
	if len(tagSection) == 0 {
		return meas, nil, n, true
	}
	// 按未转义逗号切分 tag，每个 tag 按首个未转义等号切 key/value
	var pairs [][2]string
	start := 0
	escaped = false
	for i := 0; i <= len(tagSection); i++ {
		end := i == len(tagSection)
		if !end {
			if escaped {
				escaped = false
				continue
			}
			if tagSection[i] == '\\' {
				escaped = true
				continue
			}
			if tagSection[i] != ',' {
				continue
			}
		}
		kv := tagSection[start:i]
		if len(kv) > 0 {
			k, v, ok := splitTagKV(kv)
			if !ok {
				return "", nil, 0, false
			}
			pairs = append(pairs, [2]string{unescape(k), unescape(v)})
		}
		start = i + 1
	}
	if len(pairs) == 0 {
		return "", nil, 0, false
	}
	return meas, pairs, n, true
}

// splitTagKV 按首个未转义等号切分 tag 的 key=value。
func splitTagKV(kv []byte) (k, v []byte, ok bool) {
	escaped := false
	for i := 0; i < len(kv); i++ {
		if escaped {
			escaped = false
			continue
		}
		if kv[i] == '\\' {
			escaped = true
			continue
		}
		if kv[i] == '=' {
			if i == 0 || i == len(kv)-1 {
				return nil, nil, false
			}
			return kv[:i], kv[i+1:], true
		}
	}
	return nil, nil, false
}

// unescape 反转义 LP 元数据（`\x` → 字面量 x）。
func unescape(s []byte) string {
	needCopy := false
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			needCopy = true
			break
		}
	}
	if !needCopy {
		return string(s)
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			out = append(out, s[i])
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

// SeriesKey 规范化 series 标识（measurement + 排序 tags，不含时间戳）。
// 格式与 Point.Key() 的 len:value 长度前缀一致（V1.7.1 起），保证轮询路径
// （由 Point 构造）与 fast-path（由 ParseLine 构造）生成相同的去重键。
func SeriesKey(measurement string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return appendSeriesKey(measurement, keys, func(k string) string { return tags[k] })
}

// SeriesKeyFromPairs 由 ParseLine 输出的 tag 对构造规范化 series 标识
// （与 SeriesKey 输出完全一致：按 key 排序）。
func SeriesKeyFromPairs(measurement string, tags [][2]string) string {
	keys := make([]string, 0, len(tags))
	for i := range tags {
		keys = append(keys, tags[i][0])
	}
	sort.Strings(keys)
	idx := make(map[string]string, len(tags))
	for i := range tags {
		idx[tags[i][0]] = tags[i][1]
	}
	return appendSeriesKey(measurement, keys, func(k string) string { return idx[k] })
}

func appendSeriesKey(measurement string, sortedKeys []string, val func(string) string) string {
	n := len(measurement) + 8
	for _, k := range sortedKeys {
		n += len(k) + len(val(k)) + 8
	}
	buf := make([]byte, 0, n)
	buf = appendLenPrefix(buf, measurement)
	buf = append(buf, '|')
	for _, k := range sortedKeys {
		buf = appendLenPrefix(buf, k)
		buf = append(buf, '=')
		buf = appendLenPrefix(buf, val(k))
		buf = append(buf, '|')
	}
	return string(buf)
}
