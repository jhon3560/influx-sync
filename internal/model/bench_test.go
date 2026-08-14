package model

import (
	"fmt"
	"math/rand"
	"testing"
)

// 审计实测基准的回归守卫（P1）：组帧热路径优化后若回退，这里会大幅劣化。
// 审计基线：现状 LineProtocol 329ms/万点、747MB 分配（85 次分配/点）；
// 优化目标：<10ms/万点、<10MB 分配（6 次分配/点）。
func benchPoints(n int) []Point {
	points := make([]Point, 0, n)
	for i := 0; i < n; i++ {
		points = append(points, Point{
			Measurement: "power_measure",
			Tags:        map[string]string{"plant": fmt.Sprintf("A%02d", i%50), "point": fmt.Sprintf("P%03d", i%200), "unit": "1"},
			Fields: map[string]interface{}{
				"value":   rand.Float64() * 1000,
				"quality": int64(i % 100),
				"online":  i%2 == 0,
			},
			Timestamp: 1720000000000000000 + int64(i),
		})
	}
	return points
}

func BenchmarkLineProtocol(b *testing.B) {
	points := benchPoints(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &points[i%len(points)]
		if _, err := p.LineProtocol(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLinesToProtocolBytes(b *testing.B) {
	points := benchPoints(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := LinesToProtocolBytes(points); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPointKey(b *testing.B) {
	points := benchPoints(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = points[i%len(points)].Key()
	}
}

func BenchmarkPointsEqual(b *testing.B) {
	points := benchPoints(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PointsEqual(points[i%len(points)], points[(i+1)%len(points)])
	}
}
