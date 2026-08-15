package model

import (
	"reflect"
	"testing"
)

func TestParseLineBasic(t *testing.T) {
	meas, tags, ts, ok := ParseLine([]byte("telemetry,plant=A001,point=P0001 value=1.5,status=\"ok\" 1720000000000000000"))
	if !ok {
		t.Fatal("parse failed")
	}
	if meas != "telemetry" || ts != 1720000000000000000 {
		t.Fatalf("meas=%q ts=%d", meas, ts)
	}
	want := [][2]string{{"plant", "A001"}, {"point", "P0001"}}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags=%v want %v", tags, want)
	}
}

func TestParseLineNoTags(t *testing.T) {
	meas, tags, ts, ok := ParseLine([]byte("m value=1i 42"))
	if !ok || meas != "m" || ts != 42 || len(tags) != 0 {
		t.Fatalf("meas=%q tags=%v ts=%d ok=%v", meas, tags, ts, ok)
	}
}

func TestParseLineEscapes(t *testing.T) {
	// measurement 转义逗号空格；tag 值转义等号逗号空格
	meas, tags, _, ok := ParseLine([]byte(`my\,meas,tag\ k=v\=a\,b 123`))
	if !ok {
		t.Fatal("parse failed")
	}
	if meas != "my,meas" {
		t.Fatalf("meas=%q", meas)
	}
	want := [][2]string{{"tag k", "v=a,b"}}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags=%v want %v", tags, want)
	}
}

func TestParseLineQuotedFieldWithSpacesCommasEquals(t *testing.T) {
	// 引号字符串字段内含空格/逗号/等号/转义引号，不得干扰 tag 与 ts 解析
	line := []byte(`m,t=v1 s="a b,c=d \"q\" \\ x",n=2 999`)
	meas, tags, ts, ok := ParseLine(line)
	if !ok || meas != "m" || ts != 999 {
		t.Fatalf("meas=%q ts=%d ok=%v", meas, ts, ok)
	}
	want := [][2]string{{"t", "v1"}}
	if !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags=%v", tags)
	}
}

func TestParseLineQuotedFieldEndsWithSpace(t *testing.T) {
	meas, _, ts, ok := ParseLine([]byte(`m s="x " 123`))
	if !ok || meas != "m" || ts != 123 {
		t.Fatalf("meas=%q ts=%d ok=%v", meas, ts, ok)
	}
}

func TestParseLineNoTimestamp(t *testing.T) {
	if _, _, _, ok := ParseLine([]byte("m value=1")); ok {
		t.Fatal("line without ts must fail")
	}
	if _, _, _, ok := ParseLine([]byte(`m s="a b"`)); ok {
		t.Fatal("quoted last field without ts must fail")
	}
}

func TestParseLineBadTimestamp(t *testing.T) {
	if _, _, _, ok := ParseLine([]byte("m value=1 x")); ok {
		t.Fatal("non-numeric ts must fail")
	}
	if _, _, _, ok := ParseLine([]byte("m value=1 12.5")); ok {
		t.Fatal("float ts must fail")
	}
}

func TestParseLineCRLF(t *testing.T) {
	meas, _, ts, ok := ParseLine([]byte("m value=1 123\r"))
	if !ok || meas != "m" || ts != 123 {
		t.Fatalf("meas=%q ts=%d ok=%v", meas, ts, ok)
	}
}

func TestParseLineEmptyOrGarbage(t *testing.T) {
	for _, l := range []string{"", "   ", "m", "m,", ",t=v 1"} {
		if _, _, _, ok := ParseLine([]byte(l)); ok {
			t.Fatalf("line %q must fail", l)
		}
	}
}

func TestSeriesKeyConsistency(t *testing.T) {
	// fast-path 解析构造的键必须与轮询路径 Point 构造的键完全一致
	meas, tags, ts, ok := ParseLine([]byte(`telemetry,plant=A001,point=P0001 value=1 1720000000000000000`))
	if !ok {
		t.Fatal("parse failed")
	}
	keyFromPairs := SeriesKeyFromPairs(meas, tags)
	p := Point{
		Measurement: meas,
		Tags:        map[string]string{"plant": "A001", "point": "P0001"},
		Fields:      map[string]interface{}{"value": float64(1)},
		Timestamp:   ts,
	}
	keyFromPoint := SeriesKey(p.Measurement, p.Tags)
	if keyFromPairs != keyFromPoint {
		t.Fatalf("keys differ: %q vs %q", keyFromPairs, keyFromPoint)
	}
	// 且与 Point.Key() 的 series 前缀一致
	full := p.Key()
	if len(full) != len(keyFromPoint)+len("1720000000000000000") || full[:len(keyFromPoint)] != keyFromPoint {
		t.Fatalf("Key()=%q must start with series key %q", full, keyFromPoint)
	}
}
