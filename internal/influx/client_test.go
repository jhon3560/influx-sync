package influx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// newFakeInflux 返回模拟 InfluxDB 服务：记录查询 Q 与写入 body。
func newFakeInflux(t *testing.T) (*httptest.Server, *atomic.Int64, *atomic.Int64, *syncMap) {
	t.Helper()
	var queryCount atomic.Int64
	var writeCount atomic.Int64
	written := &syncMap{m: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/query":
			queryCount.Add(1)
			q := r.URL.Query().Get("q")
			// 元查询：schema 发现
			if strings.HasPrefix(q, "SHOW TAG KEYS") {
				fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[["plant"]]}]}]}`)
				return
			}
			if strings.HasPrefix(q, "SHOW FIELD KEYS") {
				fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
				return
			}
			// 生成模拟数据：每行 (tag plant=A0x) value=ts
			// 解析 q 中的 time >= Xns AND time < Yns
			var start, end int64
			fmt.Sscanf(q, "SELECT * FROM /.*/ WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
			if start == 0 && end == 0 {
				fmt.Sscanf(q, "SELECT * FROM \"m\" WHERE time >= %dns AND time < %dns LIMIT %d", &start, &end)
			}
			var rows [][]interface{}
			for ts := start; ts < end; ts += 1000 {
				rows = append(rows, []interface{}{float64(ts), "A01", float64(ts) / 1000})
			}
			// 模拟 LIMIT 截断：若超过 10 行只返回 10 行
			limit := 10
			if len(rows) > limit {
				rows = rows[:limit]
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":%s}]}]}`, toJSON(rows))
		case "/write":
			writeCount.Add(1)
			buf := make([]byte, 1<<20)
			n, _ := r.Body.Read(buf)
			written.put(string(buf[:n]))
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &queryCount, &writeCount, written
}

type syncMap struct {
	m map[string]string
}

func (s *syncMap) put(v string) { s.m[v] = v }
func (s *syncMap) get() string {
	var parts []string
	for k := range s.m {
		parts = append(parts, k)
	}
	return strings.Join(parts, "\n")
}

func toJSON(rows [][]interface{}) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('[')
		for j, v := range row {
			if j > 0 {
				sb.WriteByte(',')
			}
			switch t := v.(type) {
			case float64:
				fmt.Fprintf(&sb, "%v", t)
			case string:
				fmt.Fprintf(&sb, "%q", t)
			}
		}
		sb.WriteByte(']')
	}
	sb.WriteByte(']')
	return sb.String()
}

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(Config{URL: srv.URL, Database: "power", Timeout: "3s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestQueryRangeBasic(t *testing.T) {
	srv, _, _, _ := newFakeInflux(t)
	c := newTestClient(t, srv)
	pts, err := c.QueryRange(context.Background(), 0, 5000, QueryOptions{})
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(pts) != 5 {
		t.Fatalf("got %d points, want 5", len(pts))
	}
	if pts[0].Timestamp != 0 || pts[4].Timestamp != 4000 {
		t.Fatalf("bad timestamps: %v", pts[0].Timestamp)
	}
	if pts[0].Tags["plant"] != "A01" {
		t.Fatalf("bad tag: %+v", pts[0].Tags)
	}
	if _, ok := pts[0].Fields["value"]; !ok {
		t.Fatalf("bad fields: %+v", pts[0].Fields)
	}
}

func TestQueryRangePagination(t *testing.T) {
	// 窗口 0~30000，fake 每次最多返回 10 行（LIMIT=10），应分页取全 30 行且无重复
	srv, _, _, _ := newFakeInflux(t)
	c := newTestClient(t, srv)
	pts, err := c.QueryRange(context.Background(), 0, 30000, QueryOptions{Limit: 10, MaxPages: 100})
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(pts) != 30 {
		t.Fatalf("got %d points, want 30", len(pts))
	}
	// 无重复
	seen := map[int64]bool{}
	for _, p := range pts {
		if seen[p.Timestamp] {
			t.Fatalf("duplicate ts %d", p.Timestamp)
		}
		seen[p.Timestamp] = true
	}
}

func TestQueryRangeWithMeasurements(t *testing.T) {
	srv, _, _, _ := newFakeInflux(t)
	c := newTestClient(t, srv)
	_, err := c.QueryRange(context.Background(), 0, 1000, QueryOptions{Measurements: []string{"m"}})
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
}

func TestQueryHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	if _, err := c.QueryRange(context.Background(), 0, 100, QueryOptions{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestQueryInfluxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[{"error":"boom"}]}`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	if _, err := c.QueryRange(context.Background(), 0, 100, QueryOptions{}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestWriteLines(t *testing.T) {
	srv, _, writeCount, written := newFakeInflux(t)
	c := newTestClient(t, srv)
	lines := []string{"m,plant=A01 value=1 1", "m,plant=A01 value=2 2"}
	if err := c.WriteLines(context.Background(), lines); err != nil {
		t.Fatalf("WriteLines: %v", err)
	}
	if writeCount.Load() != 1 {
		t.Fatalf("write count=%d", writeCount.Load())
	}
	if got := written.get(); !strings.Contains(got, "m,plant=A01 value=1 1") {
		t.Fatalf("written body=%q", got)
	}
}

func TestWriteEmpty(t *testing.T) {
	srv, _, writeCount, _ := newFakeInflux(t)
	c := newTestClient(t, srv)
	if err := c.WriteLines(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if writeCount.Load() != 0 {
		t.Fatal("should not write empty")
	}
}

func TestWriteHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "partial write: field type conflict")
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	err := c.WriteLines(context.Background(), []string{"bad"})
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("expected 400 error, got %v", err)
	}
}

func TestClientValidation(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := NewClient(Config{URL: "x", Database: "d", Timeout: "nonsense"}); err == nil {
		t.Fatal("expected timeout parse error")
	}
}

func TestQuerySQLEncoding(t *testing.T) {
	sql, err := querySQL(100, 200, QueryOptions{Measurements: []string{"a.b-c", "c_d"}, Limit: 500})
	if err != nil {
		t.Fatalf("querySQL: %v", err)
	}
	if !strings.Contains(sql, `"a.b-c"`) || !strings.Contains(sql, `"c_d"`) || !strings.Contains(sql, "LIMIT 500") {
		t.Fatalf("sql=%q", sql)
	}
	if !strings.Contains(sql, "time >= 100ns AND time < 200ns") {
		t.Fatalf("sql=%q", sql)
	}
	_ = url.QueryEscape
}

// schemaRichServer 模拟含 string field 与 integer field 的真实 schema。
func schemaRichServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		switch {
		case strings.HasPrefix(q, "SHOW TAG KEYS"):
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["tagKey"],"values":[["plant"],["device"]]}]}]}`)
		case strings.HasPrefix(q, "SHOW FIELD KEYS"):
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"],["status","integer"],["note","string"]]}]}]}`)
		default:
			// 模拟查询返回：plant/device 是字符串（tag），value 数字，status 数字，note 字符串
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","device","value","status","note"],"values":[[1,"A01","INV1",220.5,7,"ok"],[2,"A02","INV2",221.0,8,"warn"]]}]}]}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSchemaAdaptation(t *testing.T) {
	// 真实 schema：string field（note）不能误当 tag；integer field（status）保持整数语义
	srv := schemaRichServer(t)
	c := newTestClient(t, srv)
	pts, err := c.QueryRange(context.Background(), 0, 100, QueryOptions{Measurements: []string{"m"}})
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(pts) != 2 {
		t.Fatalf("points=%d", len(pts))
	}
	p := pts[0]
	// plant/device 是 tag
	if p.Tags["plant"] != "A01" || p.Tags["device"] != "INV1" {
		t.Fatalf("tags=%+v", p.Tags)
	}
	// note 是 string field，不是 tag
	if _, isTag := p.Tags["note"]; isTag {
		t.Fatal("note must not be a tag")
	}
	if p.Fields["note"] != "ok" {
		t.Fatalf("note field=%v", p.Fields["note"])
	}
	// status 是 integer field，保持整数语义
	if v, ok := p.Fields["status"].(int64); !ok || v != 7 {
		t.Fatalf("status must be int64(7), got %v (%T)", p.Fields["status"], p.Fields["status"])
	}
	if v, ok := p.Fields["value"].(float64); !ok || v != 220.5 {
		t.Fatalf("value must be float64, got %v", p.Fields["value"])
	}
	// 序列化为 line protocol：integer 带 i 后缀，string field 带引号
	line, err := p.LineProtocol()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "status=7i") || !strings.Contains(line, `note="ok"`) {
		t.Fatalf("line=%q", line)
	}
}

func TestSchemaFallbackNoMeta(t *testing.T) {
	// 元查询失败时降级：字符串保守为 field，不误当 tag
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[[1,"A01",9.9]]}]}]}`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	pts, err := c.QueryRange(context.Background(), 0, 10, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 1 {
		t.Fatalf("points=%d", len(pts))
	}
	// 无 schema 信息：plant 作为 string field（保守，避免误当 tag 导致基数爆炸）
	if _, ok := pts[0].Fields["plant"]; !ok {
		t.Fatalf("plant should be field in fallback mode: %+v", pts[0].Tags)
	}
}

func TestTagColumnsOverride(t *testing.T) {
	// 显式指定 tag 列：跳过自动发现
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if strings.HasPrefix(q, "SHOW TAG KEYS") {
			t.Fatal("must not query tag schema when tag columns given")
		}
		if strings.HasPrefix(q, "SHOW FIELD KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["fieldKey","fieldType"],"values":[["value","float"]]}]}]}`)
			return
		}
		fmt.Fprint(w, `{"results":[{"series":[{"name":"m","columns":["time","plant","value"],"values":[[1,"A01",9.9]]}]}]}`)
	}))
	defer srv.Close()
	c := newTestClient(t, srv)
	pts, err := c.QueryRange(context.Background(), 0, 10, QueryOptions{TagColumns: []string{"plant"}})
	if err != nil {
		t.Fatal(err)
	}
	if pts[0].Tags["plant"] != "A01" {
		t.Fatalf("plant should be tag: %+v", pts[0].Tags)
	}
}

func TestQuerySQLInjectionGuard(t *testing.T) {
	// 恶意 measurement 名必须被拒绝（防配置注入 InfluxQL）
	bad := []string{`m"; DROP DATABASE power; --`, `m" OR 1=1`, `"`, `a;b`, `m\`}
	for _, m := range bad {
		if _, err := querySQL(0, 1, QueryOptions{Measurements: []string{m}}); err == nil {
			t.Fatalf("measurement %q must be rejected", m)
		}
	}
	// 合法名通过
	good := []string{"telemetry", "alarm_1", "a.b/c-d"}
	for _, m := range good {
		if _, err := querySQL(0, 1, QueryOptions{Measurements: []string{m}}); err != nil {
			t.Fatalf("measurement %q must pass: %v", m, err)
		}
	}
}
