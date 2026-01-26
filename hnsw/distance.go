package hnsw

type DistanceFunc func(a, b []float32) float32

func L2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("vector dimensions mismatch")
	}
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

// todo
