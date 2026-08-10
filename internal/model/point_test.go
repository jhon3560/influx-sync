package model

import (
	"reflect"
	"testing"
)

func TestPointLineProtocol(t *testing.T) {
	p := Point{
		Measurement: "power_measure",
		Tags:        map[string]string{"plant": "A001", "point": "P001"},
		Fields:      map[string]interface{}{"value": 220.5, "quality": int64(1)},
		Timestamp:   1720000000000000000,
	}
	line, err := p.LineProtocol()
	if err != nil {
		t.Fatalf("LineProtocol error: %v", err)
	}
	want := `power_measure,plant=A001,point=P001 quality=1i,value=220.5 1720000000000000000`
	if line != want {
		t.Fatalf("got %q, want %q", line, want)
	}
}

func TestPointLineProtocolFieldTypes(t *testing.T) {
	cases := []struct {
		val  interface{}
		want string
	}{
		{float64(1.5), "v=1.5"},
		{int64(7), "v=7i"},
		{"str", `v="str"`},
		{true, "v=true"},
		{false, "v=false"},
	}
	for _, c := range cases {
		p := Point{Measurement: "m", Fields: map[string]interface{}{"v": c.val}, Timestamp: 1}
		line, err := p.LineProtocol()
		if err != nil {
			t.Fatalf("val %v: %v", c.val, err)
		}
		if line != "m "+c.want+" 1" {
			t.Fatalf("val %v: got %q", c.val, line)
		}
	}
}

func TestPointLineProtocolErrors(t *testing.T) {
	// 无字段
	p := Point{Measurement: "m", Timestamp: 1}
	if _, err := p.LineProtocol(); err == nil {
		t.Fatal("expected error for no fields")
	}
	// 不支持的类型
	p = Point{Measurement: "m", Fields: map[string]interface{}{"v": []byte("x")}, Timestamp: 1}
	if _, err := p.LineProtocol(); err == nil {
		t.Fatal("expected error for unsupported field type")
	}
}

func TestPointLineProtocolEscaping(t *testing.T) {
	p := Point{
		Measurement: "m,1",
		Tags:        map[string]string{"k,2": "v 3", "k=4": "x"},
		Fields:      map[string]interface{}{`f"5`: "a\"b"},
		Timestamp:   2,
	}
	line, err := p.LineProtocol()
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	want := `m\,1,k\,2=v\ 3,k\=4=x f"5="a\"b" 2`
	if line != want {
		t.Fatalf("got %q, want %q", line, want)
	}
}

func TestPointKey(t *testing.T) {
	p1 := Point{Measurement: "m", Tags: map[string]string{"a": "1", "b": "2"}, Timestamp: 5}
	p2 := Point{Measurement: "m", Tags: map[string]string{"b": "2", "a": "1"}, Timestamp: 5}
	p3 := Point{Measurement: "m", Tags: map[string]string{"a": "1", "b": "2"}, Timestamp: 6}
	if p1.Key() != p2.Key() {
		t.Fatal("same point keys differ")
	}
	if p1.Key() == p3.Key() {
		t.Fatal("different points have same key")
	}
}

func TestLinesToProtocol(t *testing.T) {
	lines, err := LinesToProtocol([]Point{
		{Measurement: "m", Fields: map[string]interface{}{"v": float64(1)}, Timestamp: 1},
		{Measurement: "m", Fields: map[string]interface{}{"v": float64(2)}, Timestamp: 2},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines", len(lines))
	}
	_ = reflect.DeepEqual // keep import if unused later
}
