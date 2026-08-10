package monitor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsRender(t *testing.T) {
	m := New()
	m.SetCursor(100)
	m.SetWALPending(3)
	m.SetWALBytes(4096)
	m.IncSend()
	m.IncAckOk()
	m.IncAckFail()
	m.IncDLQ()
	out := string(m.Render())
	for _, want := range []string{
		"sync_cursor_ns 100",
		"wal_pending 3",
		"wal_size_bytes 4096",
		"send_total 1",
		"ack_success 1",
		"ack_fail 1",
		"dlq_total 1",
		"sync_delay_seconds ",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}

func TestMetricsDelay(t *testing.T) {
	m := New()
	m.SetCursor(time.Now().UnixNano() - 5_000_000_000) // 5s 前
	out := string(m.Render())
	if !strings.Contains(out, "sync_delay_seconds 5\n") {
		t.Fatalf("unexpected delay:\n%s", out)
	}
}

func TestMetricsHTTP(t *testing.T) {
	m := New()
	m.IncSend()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(m.Render())
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "send_total 1") {
		t.Fatalf("body=%s", body)
	}
}

func TestMetricsAuth(t *testing.T) {
	m := New()
	auth := &Auth{Username: "prom", Password: "s3cret"}
	srv := httptest.NewServer(m.authMiddleware(auth, m.Handler()))
	defer srv.Close()

	// 无凭据 → 401
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth: status=%d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate header")
	}

	// 错误凭据 → 401
	req, _ := http.NewRequest("GET", srv.URL+"/metrics", nil)
	req.SetBasicAuth("prom", "wrong")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad pass: status=%d, want 401", resp.StatusCode)
	}

	// 正确凭据 → 200 + 指标内容
	req, _ = http.NewRequest("GET", srv.URL+"/metrics", nil)
	req.SetBasicAuth("prom", "s3cret")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("good auth: status=%d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "send_total") {
		t.Fatalf("metrics body missing: %s", string(body[:100]))
	}
}

func TestMetricsNoAuthCompat(t *testing.T) {
	// auth 为空：不启用认证（兼容旧部署）
	m := New()
	srv := httptest.NewServer(m.authMiddleware(nil, m.Handler()))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("no-auth compat: status=%d", resp.StatusCode)
	}
	// 空用户名同样视为不启用
	srv2 := httptest.NewServer(m.authMiddleware(&Auth{Username: "", Password: "x"}, m.Handler()))
	defer srv2.Close()
	resp, err = srv2.Client().Get(srv2.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty username: status=%d", resp.StatusCode)
	}
}
