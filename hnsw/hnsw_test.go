package hnsw

import (
	"math/rand"
	"testing"
)

func TestHNSWBasic(t *testing.T) {
	// 创建索引
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
		DistanceFunc:   L2Distance,
		Seed:           42,
	}

	index := NewHNSW(config)

	// 添加一些向量
	numVectors := 1000
	vectors := make([][]float32, numVectors)

	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector

		id, err := index.Add(vector)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}

		if id != i {
			t.Errorf("Expected ID %d, got %d", i, id)
		}
	}

	// 测试搜索
	query := vectors[0]
	k := 10
	results, err := index.Search(query, k, 0)

	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != k {
		t.Errorf("Expected %d results, got %d", k, len(results))
	}

	// 第一个结果应该是查询向量本身（距离为0）
	if results[0].ID != 0 {
		t.Errorf("Expected first result to be ID 0, got %d", results[0].ID)
	}

	if results[0].Distance > 0.0001 {
		t.Errorf("Expected distance ~0, got %f", results[0].Distance)
	}

	t.Logf("Index size: %d", index.Len())
	t.Logf("Top 3 results: %+v", results[:3])
}

func TestHNSWEmpty(t *testing.T) {
	config := Config{
		Dimension: 128,
	}

	index := NewHNSW(config)

	query := make([]float32, 128)
	_, err := index.Search(query, 10, 0)

	if err != ErrEmptyIndex {
		t.Errorf("Expected ErrEmptyIndex, got %v", err)
	}
}

func TestDistanceFunctions(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}

	// L2 距离
	l2 := L2Distance(a, b)
	expected := float32(27) // (1-4)^2 + (2-5)^2 + (3-6)^2 = 9+9+9 = 27
	if l2 != expected {
		t.Errorf("L2Distance: expected %f, got %f", expected, l2)
	}

	// 内积距离
	ip := InnerProductDistance(a, b)
	expectedIP := float32(-32) // -(1*4 + 2*5 + 3*6) = -(4+10+18) = -32
	if ip != expectedIP {
		t.Errorf("InnerProductDistance: expected %f, got %f", expectedIP, ip)
	}
}

func BenchmarkHNSWInsert(b *testing.B) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}
}

func BenchmarkHNSWSearch(b *testing.B) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 预先添加一些向量
	for i := 0; i < 10000; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(query, 10, 100)
	}
}
