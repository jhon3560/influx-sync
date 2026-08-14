// Package influx 实现 InfluxDB 1.x HTTP 客户端（查询 + 批量写入）。
package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"influx-sync/internal/model"
)

// Config InfluxDB 连接配置。
type Config struct {
	URL      string `yaml:"url"`      // 如 http://127.0.0.1:8086
	Database string `yaml:"database"` // 库名
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Timeout  string `yaml:"timeout"` // HTTP 超时，如 5s
}

// Client InfluxDB 1.x HTTP 客户端。
type Client struct {
	cfg     Config
	http    *http.Client
	timeout time.Duration
	// schema 自适应缓存：measurement -> tag 集合 + 字段类型（1 小时过期）
	schemaMu      sync.Mutex
	schemaCache   map[string]*schemaEntry
	schemaFlights map[string]*schemaCall // single-flight：并发去重 schema 发现
}

// schemaEntry 一个 measurement 的 schema 定义。degraded=true 表示元查询
// 失败后的降级条目（类型推断兜底），用短 TTL 负缓存，到期重试发现。
type schemaEntry struct {
	tags      map[string]bool   // tag key 集合
	fieldType map[string]string // field key -> 类型（float/integer/string/boolean）
	fetchedAt time.Time
	degraded  bool
}

// fresh 判断条目是否仍在有效期内（降级条目短 TTL）。
func (e *schemaEntry) fresh() bool {
	ttl := schemaCacheTTL
	if e.degraded {
		ttl = schemaDegradeTTL
	}
	return time.Since(e.fetchedAt) < ttl
}

// schemaCall 一次进行中的 schema 发现（single-flight）。
type schemaCall struct {
	done  chan struct{}
	entry *schemaEntry
	err   error
}

const (
	schemaCacheTTL   = time.Hour        // 成功发现的 schema 缓存
	schemaDegradeTTL = 30 * time.Second // 失败降级负缓存（N5：短 TTL 后重试，不永久停摆）
)

// WriteHTTPError 写库 HTTP 错误（带状态码，供错误分类器 typed 判断，
// 替代解析错误文案）。
type WriteHTTPError struct {
	StatusCode int
	Body       string
}

// Error 保持历史错误文案格式（日志/测试兼容）。
func (e *WriteHTTPError) Error() string {
	return fmt.Sprintf("influx: write http %d: %s", e.StatusCode, truncate(e.Body, 512))
}

// NewClient 创建客户端。timeout 为空时默认 10s（作为无 ctx 截止时间时的兜底）。
func NewClient(cfg Config) (*Client, error) {
	if cfg.URL == "" || cfg.Database == "" {
		return nil, fmt.Errorf("influx: url and database required")
	}
	d := 10 * time.Second
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("influx: bad timeout %q: %w", cfg.Timeout, err)
		}
		d = parsed
	}
	// 传输调优：MaxIdleConnsPerHost ≥ 并行查询数（poller_parallel 默认 4），
	// 避免每轮查询 2 条连接被丢弃重建；payload 已 gzip，关闭自动解压。
	// Timeout 不再作为全局硬上限（大窗口回填会假失败）：由调用方按窗口动态
	// 给 ctx 截止时间，本字段兜底 15 分钟防泄漏。
	tr := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &Client{
		cfg:           cfg,
		http:          &http.Client{Transport: tr, Timeout: 15 * time.Minute},
		timeout:       d,
		schemaCache:   make(map[string]*schemaEntry),
		schemaFlights: make(map[string]*schemaCall),
	}, nil
}

// do 执行请求；ctx 无截止时间时用 fallback 兜底（防无超时挂死）。
func (c *Client) do(req *http.Request, fallback time.Duration) (*http.Response, error) {
	ctx := req.Context()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fallback)
		defer cancel()
		req = req.WithContext(ctx)
	}
	return c.http.Do(req)
}

// queryResult Influx 1.x /query 响应结构。
type queryResult struct {
	Results []struct {
		Series []struct {
			Name    string          `json:"name"`
			Columns []string        `json:"columns"`
			Values  [][]interface{} `json:"values"`
		} `json:"series"`
		Err string `json:"error"`
	} `json:"results"`
	Err string `json:"error"`
}

// QueryOptions 查询选项。
type QueryOptions struct {
	Measurements []string // 要同步的 measurement 列表；为空则同步所有
	Limit        int      // 单次查询行数上限，默认 10000
	MaxPages     int      // 单窗口分页上限，默认 1000
	Offset       int      // 查询偏移（同 ts 稠密行分页用）
	TagColumns   []string // 显式指定 tag 列（覆盖自动发现；空=自动 SHOW TAG KEYS）
}

// queryTimeout 按窗口大小给动态超时（大窗口回填/慢库不假失败）。
// 30s 下限 + 2×窗口秒数，10 分钟封顶。
func queryTimeout(start, end int64) time.Duration {
	winSec := (end - start) / 1e9
	if winSec < 0 {
		winSec = 0
	}
	d := 30*time.Second + 2*time.Duration(winSec)*time.Second
	if d > 10*time.Minute {
		d = 10 * time.Minute
	}
	return d
}

// pointSet 边界去重集合：小集合用零分配的 PointsEqual 线性比较，
// 大集合自动切换 Key 映射（大集合才付出 Key 字符串构造成本）。
type pointSet struct {
	small []model.Point
	big   map[string]struct{}
}

const pointSetLinearLimit = 8

func (s *pointSet) add(p model.Point) {
	if len(s.small) < pointSetLinearLimit {
		s.small = append(s.small, p)
		return
	}
	if s.big == nil {
		s.big = make(map[string]struct{}, pointSetLinearLimit*4)
		for _, q := range s.small {
			s.big[q.Key()] = struct{}{}
		}
		s.small = nil
	}
	s.big[p.Key()] = struct{}{}
}

func (s *pointSet) contains(p model.Point) bool {
	for _, q := range s.small {
		if model.PointsEqual(q, p) {
			return true
		}
	}
	if s.big != nil {
		_, ok := s.big[p.Key()]
		return ok
	}
	return false
}

// QueryRange 查询 [start, end) 时间窗口内所有点（ns 精度）。
// 内部按 LIMIT 分页推进，避免一次拉爆源库；返回的点按时间升序。
//
// 去重只覆盖分页边界时间戳（跨页同 timestamp 的行才可能重复返回）——
// 相比全窗口 map 去重，省掉每点一次 Key() 构造与全量 map 内存。
func (c *Client) QueryRange(ctx context.Context, start, end int64, opt QueryOptions) ([]model.Point, error) {
	if opt.Limit <= 0 {
		opt.Limit = 10000
	}
	if opt.MaxPages <= 0 {
		opt.MaxPages = 1000
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout(start, end))
	defer cancel()
	var points []model.Point
	var carried pointSet // 上一页边界 ts 的行（与新页开头重叠的部分）
	for from := start; ; {
		if from >= end {
			break
		}
		rows, err := c.queryOnce(ctx, from, end, opt)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		lastTS := rows[len(rows)-1].Timestamp
		// 去重只覆盖分页边界 ts（上一页末尾 = 本页开头 from）
		for _, r := range rows {
			if r.Timestamp >= end { // 防御：正常不会发生
				continue
			}
			if r.Timestamp == from && from != start && carried.contains(r) {
				continue
			}
			points = append(points, r)
		}
		// 重建携带集：本页末尾 ts 的行，用于下一页开头去重
		carried.reset()
		for _, r := range rows {
			if r.Timestamp == lastTS {
				carried.add(r)
			}
		}
		if len(rows) < opt.Limit {
			break // 未取满，窗口结束
		}
		if lastTS == from {
			// 同一纳秒内行数超过 LIMIT：用 OFFSET 分页拉全该 ts 的行后推进
			// （真实场景：批量导入工具可能共用时间戳；不处理会永久卡死）
			offset := 0
			dense := make(map[string]struct{})
			for _, r := range rows {
				dense[r.Key()] = struct{}{}
			}
			for {
				big, err := c.queryOnce(ctx, lastTS, lastTS+1, QueryOptions{
					Limit: opt.Limit * 10, Offset: offset,
				})
				if err != nil {
					return nil, fmt.Errorf("influx: fetch dense ts %d: %w", lastTS, err)
				}
				added := 0
				for _, r := range big {
					if r.Timestamp != lastTS {
						continue
					}
					if _, dup := dense[r.Key()]; dup {
						continue
					}
					dense[r.Key()] = struct{}{}
					points = append(points, r)
					added++
				}
				offset += len(big)
				if len(big) < opt.Limit*10 {
					break // 该 ts 已拿全
				}
				if offset > 10_000_000 {
					return nil, fmt.Errorf("influx: ts %d has >10M rows, abort", lastTS)
				}
			}
			from = lastTS + 1
			continue
		}
		from = lastTS // 从边界 ts 继续，边界行由 carried 去重
		if opt.MaxPages > 0 {
			opt.MaxPages--
			if opt.MaxPages == 0 {
				return nil, fmt.Errorf("influx: query exceeded max pages for window [%d,%d)", start, end)
			}
		}
	}
	return points, nil
}

// reset 清空去重集合（复用内存）。
func (s *pointSet) reset() {
	s.small = s.small[:0]
	s.big = nil
}

// queryOnce 执行单次 SELECT 查询并解析。
func (c *Client) queryOnce(ctx context.Context, start, end int64, opt QueryOptions) ([]model.Point, error) {
	q, err := querySQL(start, end, opt)
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/query?db=%s&epoch=ns&q=%s", c.cfg.URL, url.QueryEscape(c.cfg.Database), url.QueryEscape(q))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("influx: build query: %w", err)
	}
	c.setAuth(req)
	resp, err := c.do(req, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("influx: query %s [%d,%d): %w", opt.Measurements, start, end, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("influx: read query response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("influx: query http %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	var qr queryResult
	// UseNumber：时间戳（epoch=ns）超出 float64 精确范围（2^53），
	// 用 json.Number 保留纳秒精度（否则 ±256ns 误差）
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&qr); err != nil {
		return nil, fmt.Errorf("influx: parse query response: %w", err)
	}
	if qr.Err != "" {
		return nil, fmt.Errorf("influx: query error: %s", qr.Err)
	}
	var points []model.Point
	for _, res := range qr.Results {
		if res.Err != "" {
			return nil, fmt.Errorf("influx: query result error: %s", res.Err)
		}
		for _, s := range res.Series {
			schema, err := c.ensureSchema(ctx, s.Name, opt.TagColumns)
			if err != nil {
				return nil, fmt.Errorf("influx: discover schema of %s: %w", s.Name, err)
			}
			pts, err := seriesToPoints(s.Name, s.Columns, s.Values, schema)
			if err != nil {
				return nil, fmt.Errorf("influx: parse series %s: %w", s.Name, err)
			}
			points = append(points, pts...)
		}
	}
	return points, nil
}

// ensureSchema 获取 measurement 的 schema 定义（tag keys + field 类型），带缓存。
// 显式指定 tagColumns 时跳过 tag 自动发现。
// single-flight：同一 measurement 的并发发现合并为一次 HTTP 往返。
// N5：元查询失败**不传播错误、不停摆同步**——降级为类型推断兜底（v1.3.1
// 行为），负缓存短 TTL（30s）后自动重试发现；成功条目缓存 1 小时。
func (c *Client) ensureSchema(ctx context.Context, measurement string, tagColumns []string) (*schemaEntry, error) {
	c.schemaMu.Lock()
	if e, ok := c.schemaCache[measurement]; ok && e.fresh() {
		c.schemaMu.Unlock()
		return e, nil
	}
	if call, ok := c.schemaFlights[measurement]; ok {
		// 已有并发发现在途：等待其完成（或 ctx 取消）
		c.schemaMu.Unlock()
		select {
		case <-call.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		c.schemaMu.Lock()
		if e, ok := c.schemaCache[measurement]; ok && e.fresh() {
			c.schemaMu.Unlock()
			return e, nil
		}
		c.schemaMu.Unlock()
		return nil, fmt.Errorf("influx: schema discovery of %s failed", measurement)
	}
	call := &schemaCall{done: make(chan struct{})}
	c.schemaFlights[measurement] = call
	c.schemaMu.Unlock()

	entry, err := c.fetchSchema(ctx, measurement, tagColumns)

	c.schemaMu.Lock()
	delete(c.schemaFlights, measurement)
	if err == nil {
		entry.fetchedAt = time.Now()
		c.schemaCache[measurement] = entry
	} else if prev, ok := c.schemaCache[measurement]; ok && !prev.degraded {
		// N8：复用上一份成功 schema（即使已过期）——降级期类型保持正确，
		// 避免未知列被写成 string field → 发现恢复后类型冲突成毒丸。
		// 短 TTL 负缓存：30s 后重试发现。
		prev.fetchedAt = time.Now()
		prev.degraded = true
		c.schemaCache[measurement] = prev
		entry = prev
		zap.L().Warn("influx: schema discovery failed, reusing last good schema (retry in 30s)",
			zap.String("measurement", measurement), zap.Error(err))
	} else {
		// 无历史可复用：降级为类型推断兜底 + 短 TTL 负缓存（到期重试，不向调用方传播）
		entry.degraded = true
		entry.fetchedAt = time.Now()
		c.schemaCache[measurement] = entry
		zap.L().Warn("influx: schema discovery failed, degraded to type inference (retry in 30s)",
			zap.String("measurement", measurement), zap.Error(err))
	}
	call.entry, call.err = entry, nil
	close(call.done)
	c.schemaMu.Unlock()
	return entry, nil
}

// fetchSchema 执行 SHOW TAG KEYS / SHOW FIELD KEYS（不持锁，不缓存）。
// 失败时返回已发现的部分条目 + error（调用方降级为类型推断兜底）。
func (c *Client) fetchSchema(ctx context.Context, measurement string, tagColumns []string) (*schemaEntry, error) {
	e := &schemaEntry{tags: map[string]bool{}, fieldType: map[string]string{}}
	if len(tagColumns) > 0 {
		for _, k := range tagColumns {
			e.tags[k] = true
		}
	}
	var errs []error
	// SHOW TAG KEYS
	if len(tagColumns) == 0 {
		if rows, err := c.queryMeta(ctx, fmt.Sprintf(`SHOW TAG KEYS FROM %q`, measurement)); err != nil {
			errs = append(errs, fmt.Errorf("show tag keys: %w", err))
		} else {
			for _, row := range rows {
				if len(row) > 0 {
					if k, ok := row[0].(string); ok {
						e.tags[k] = true
					}
				}
			}
		}
	}
	// SHOW FIELD KEYS（含类型）
	if rows, err := c.queryMeta(ctx, fmt.Sprintf(`SHOW FIELD KEYS FROM %q`, measurement)); err != nil {
		errs = append(errs, fmt.Errorf("show field keys: %w", err))
	} else {
		for _, row := range rows {
			if len(row) >= 2 {
				if k, ok := row[0].(string); ok {
					if t, ok := row[1].(string); ok {
						e.fieldType[k] = t
					}
				}
			}
		}
	}
	if len(errs) > 0 {
		return e, fmt.Errorf("influx: schema discovery of %s: %v", measurement, errs)
	}
	return e, nil
}

// queryMeta 执行 SHOW 类元查询，返回行数据。
func (c *Client) queryMeta(ctx context.Context, q string) ([][]interface{}, error) {
	u := fmt.Sprintf("%s/query?db=%s&q=%s", c.cfg.URL, url.QueryEscape(c.cfg.Database), url.QueryEscape(q))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	c.setAuth(req)
	resp, err := c.do(req, 15*time.Second)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	var qr queryResult
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&qr); err != nil {
		return nil, err
	}
	if qr.Err != "" {
		return nil, fmt.Errorf("%s", qr.Err)
	}
	var rows [][]interface{}
	for _, res := range qr.Results {
		if res.Err != "" {
			return nil, fmt.Errorf("%s", res.Err)
		}
		for _, s := range res.Series {
			rows = append(rows, s.Values...)
		}
	}
	return rows, nil
}

func querySQL(start, end int64, opt QueryOptions) (string, error) {
	var sb strings.Builder
	if len(opt.Measurements) > 0 {
		quoted := make([]string, 0, len(opt.Measurements))
		for _, m := range opt.Measurements {
			if !validMeasurement(m) {
				return "", fmt.Errorf("influx: invalid measurement name %q (allow [A-Za-z0-9_./-])", m)
			}
			quoted = append(quoted, `"`+m+`"`)
		}
		sb.WriteString("SELECT * FROM " + strings.Join(quoted, ","))
	} else {
		sb.WriteString("SELECT * FROM /.*/")
	}
	fmt.Fprintf(&sb, " WHERE time >= %dns AND time < %dns LIMIT %d", start, end, opt.Limit)
	if opt.Offset > 0 {
		fmt.Fprintf(&sb, " OFFSET %d", opt.Offset)
	}
	return sb.String(), nil
}

// validMeasurement 校验 measurement 名（Influx 合法名通常含字母数字下划线点斜杠）
var measurementRe = regexp.MustCompile(`^[A-Za-z0-9_./-]+$`)

func validMeasurement(m string) bool {
	return m != "" && measurementRe.MatchString(m)
}

// seriesToPoints 将 Influx series 行转换为 Point。
// 列分类依据发现的 schema：tag keys → Tag；其余按字段类型 → Field（integer→int64）。
// 键序（tag/field 排序）在 series 级计算一次，注入全部点共享（组帧/查重零排序）。
func seriesToPoints(name string, columns []string, values [][]interface{}, schema *schemaEntry) ([]model.Point, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	timeIdx := -1
	for i, c := range columns {
		if c == "time" {
			timeIdx = i
			break
		}
	}
	if timeIdx < 0 {
		return nil, fmt.Errorf("no time column in %s", name)
	}
	// series 级预排序键序（Influx 同 series 列序稳定，缓存一次全行复用）
	tagCols := make([]string, 0, len(columns))
	fieldCols := make([]string, 0, len(columns))
	for _, col := range columns {
		if col == "time" {
			continue
		}
		if schema != nil && schema.tags[col] {
			tagCols = append(tagCols, col)
		} else {
			fieldCols = append(fieldCols, col)
		}
	}
	sort.Strings(tagCols)
	sort.Strings(fieldCols)

	points := make([]model.Point, 0, len(values))
	for _, row := range values {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("row/column mismatch: %d vs %d", len(row), len(columns))
		}
		ts, ok := tsToInt64(row[timeIdx])
		if !ok {
			return nil, fmt.Errorf("bad timestamp %v", row[timeIdx])
		}
		p := model.Point{Measurement: name, Timestamp: int64(ts), Tags: map[string]string{}, Fields: map[string]interface{}{}}
		for i, col := range columns {
			if i == timeIdx || row[i] == nil {
				continue
			}
			// 1. 已知 tag → 字符串
			if schema != nil && schema.tags[col] {
				s, ok := row[i].(string)
				if !ok {
					return nil, fmt.Errorf("tag %s of %s is not string: %v", col, name, row[i])
				}
				p.Tags[col] = s
				continue
			}
			// 2. 已知字段类型 → 精确转换（integer 保持整数语义）
			if schema != nil {
				if ft, ok := schema.fieldType[col]; ok {
					switch ft {
					case "integer":
						p.Fields[col] = toInt64(row[i])
					case "float":
						p.Fields[col] = toFloat64(row[i])
					case "boolean":
						p.Fields[col] = toBool(row[i])
					case "string":
						p.Fields[col] = row[i].(string)
					}
					continue
				}
			}
			// 3. 兜底：按 JSON 值类型
			switch v := row[i].(type) {
			case json.Number:
				// 整数语义优先（时间戳/整数字段），否则 float
				if iv, err := v.Int64(); err == nil {
					p.Fields[col] = iv
				} else if fv, err := v.Float64(); err == nil {
					p.Fields[col] = fv
				}
			case float64:
				p.Fields[col] = v
			case bool:
				p.Fields[col] = v
			case string:
				// 无 schema 信息时字符串保守处理为 string field（避免把真实 string field 误当 tag）
				p.Fields[col] = v
			default:
				return nil, fmt.Errorf("unsupported value type %T for column %s", row[i], col)
			}
		}
		if len(p.Fields) == 0 {
			return nil, fmt.Errorf("row at %v has no fields", row[timeIdx])
		}
		p.SetKeyOrder(tagCols, fieldCols)
		points = append(points, p)
	}
	return points, nil
}

// tsToInt64 将 JSON 时间戳（json.Number 或 float64）转换为 int64 纳秒。
func tsToInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			f, ferr := t.Float64()
			if ferr != nil {
				return 0, false
			}
			return int64(f), true
		}
		return n, true
	case float64:
		return int64(t), true
	}
	return 0, false
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case json.Number:
		n, err := t.Int64()
		if err == nil {
			return n
		}
		f, _ := t.Float64()
		return int64(f)
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	}
	return 0
}

func toBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		return t == "true"
	}
	return false
}

// WriteLines 批量写入 Line Protocol（HTTP /write, precision=ns）。
// 保留 []string 接口（测试/兼容），内部走 WriteRaw。
func (c *Client) WriteLines(ctx context.Context, lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	return c.WriteRaw(ctx, []byte(strings.Join(lines, "\n")))
}

// WriteRaw 直接写入原始 Line Protocol 字节（P5：省掉拆行→拼串往返，
// 解压出的 payload 本身就是合法 LP）。失败返回 *WriteHTTPError（4xx/5xx）。
func (c *Client) WriteRaw(ctx context.Context, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	u := fmt.Sprintf("%s/write?db=%s&precision=ns", c.cfg.URL, url.QueryEscape(c.cfg.Database))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("influx: build write: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	c.setAuth(req)
	resp, err := c.do(req, c.timeout)
	if err != nil {
		return fmt.Errorf("influx: write %d bytes: %w", len(raw), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &WriteHTTPError{StatusCode: resp.StatusCode, Body: string(msg)}
	}
	return nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.cfg.Username != "" {
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
