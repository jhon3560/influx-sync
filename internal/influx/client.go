// Package influx 实现 InfluxDB 1.x HTTP 客户端（查询 + 批量写入）。
package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

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
	schemaMu    sync.Mutex
	schemaCache map[string]*schemaEntry
}

// schemaEntry 一个 measurement 的 schema 定义。
type schemaEntry struct {
	tags      map[string]bool   // tag key 集合
	fieldType map[string]string // field key -> 类型（float/integer/string/boolean）
	fetchedAt time.Time
}

const schemaCacheTTL = time.Hour

// NewClient 创建客户端。timeout 为空时默认 10s。
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
	return &Client{
		cfg:         cfg,
		http:        &http.Client{Timeout: d},
		timeout:     d,
		schemaCache: make(map[string]*schemaEntry),
	}, nil
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

// QueryRange 查询 [start, end) 时间窗口内所有点（ns 精度）。
// 内部按 LIMIT 分页推进，避免一次拉爆源库；返回的点按时间升序。
func (c *Client) QueryRange(ctx context.Context, start, end int64, opt QueryOptions) ([]model.Point, error) {
	if opt.Limit <= 0 {
		opt.Limit = 10000
	}
	if opt.MaxPages <= 0 {
		opt.MaxPages = 1000
	}
	// 窗口内查重（跨分页边界同 timestamp 的行可能重复返回）
	seen := make(map[string]struct{})
	var points []model.Point
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
		for _, r := range rows {
			if r.Timestamp >= end { // 防御：正常不会发生
				continue
			}
			if _, dup := seen[r.Key()]; dup {
				continue
			}
			seen[r.Key()] = struct{}{}
			points = append(points, r)
		}
		if len(rows) < opt.Limit {
			break // 未取满，窗口结束
		}
		if lastTS == from {
			// 同一纳秒内行数超过 LIMIT：用 OFFSET 分页拉全该 ts 的行后推进
			// （真实场景：批量导入工具可能共用时间戳；不处理会永久卡死）
			offset := 0
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
					if _, dup := seen[r.Key()]; dup {
						continue
					}
					seen[r.Key()] = struct{}{}
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
		from = lastTS // 从边界 ts 继续，边界行由 seen 去重
		if opt.MaxPages > 0 {
			opt.MaxPages--
			if opt.MaxPages == 0 {
				return nil, fmt.Errorf("influx: query exceeded max pages for window [%d,%d)", start, end)
			}
		}
	}
	return points, nil
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
	resp, err := c.http.Do(req)
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
	if err := json.Unmarshal(body, &qr); err != nil {
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
// 显式指定 tagColumns 时跳过自动发现；发现失败降级为旧行为（字符串→tag）。
func (c *Client) ensureSchema(ctx context.Context, measurement string, tagColumns []string) (*schemaEntry, error) {
	c.schemaMu.Lock()
	defer c.schemaMu.Unlock()
	if e, ok := c.schemaCache[measurement]; ok && time.Since(e.fetchedAt) < schemaCacheTTL {
		return e, nil
	}
	e := &schemaEntry{tags: map[string]bool{}, fieldType: map[string]string{}}
	if len(tagColumns) > 0 {
		for _, k := range tagColumns {
			e.tags[k] = true
		}
	}
	// SHOW TAG KEYS
	if len(tagColumns) == 0 {
		if rows, err := c.queryMeta(ctx, fmt.Sprintf(`SHOW TAG KEYS FROM %q`, measurement)); err == nil {
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
	if rows, err := c.queryMeta(ctx, fmt.Sprintf(`SHOW FIELD KEYS FROM %q`, measurement)); err == nil {
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
	e.fetchedAt = time.Now()
	c.schemaCache[measurement] = e
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
	resp, err := c.http.Do(req)
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
	if err := json.Unmarshal(body, &qr); err != nil {
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
	points := make([]model.Point, 0, len(values))
	for _, row := range values {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("row/column mismatch: %d vs %d", len(row), len(columns))
		}
		ts, ok := row[timeIdx].(float64)
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
		points = append(points, p)
	}
	return points, nil
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
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
func (c *Client) WriteLines(ctx context.Context, lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	body := strings.Join(lines, "\n")
	u := fmt.Sprintf("%s/write?db=%s&precision=ns", c.cfg.URL, url.QueryEscape(c.cfg.Database))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("influx: build write: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("influx: write %d lines: %w", len(lines), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("influx: write http %d: %s", resp.StatusCode, truncate(string(msg), 512))
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
