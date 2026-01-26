package hnsw

import (
	"math/rand/v2"
	"sync"
)

// The main structure of the HNSW index.
type HNSWIndex struct {
	// Core params
	M              int // Maximum number of connections per level.
	Mmax           int // The real value of the M.
	Mmax0          int // Maximum number of connections at level 0.(2*M).
	efConstruction int // Size of the dynamic list for the nearest neighbors during construction.
	ml             int //

	dimension int // Dimensionality of the vectors.

	nodes      []*Node // All nodes in the HNSW graph.
	entryPoint int32   // Entry point node ID.
	maxLevel   int     // Maximum level in the HNSW hierarchy.

	distFunc DistanceFunc // Distance function used for measuring similarity.

	globalLock sync.RWMutex   // Protects the entire index during insertions.
	nodeLocks  []sync.RWMutex // Locks for individual nodes.

	rng *rand.Rand // Random number generator for level assignment.
	mu  sync.Mutex // Protects the RNG.
}

// todo
