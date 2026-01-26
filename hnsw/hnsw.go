package hnsw

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// The main structure of the HNSW index.
type HNSWIndex struct {
	// Core params
	M              int     // Maximum number of connections per level.
	Mmax           int     // The real value of the M.
	Mmax0          int     // Maximum number of connections at level 0.(2*M).
	efConstruction int     // Size of the dynamic list for the nearest neighbors during construction.
	ml             float64 // Level multiplier(normalization) (1/ln(M)).

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

// Config holds the configuration parameters for the HNSW index.
type Config struct {
	M              int          // Maximum number of connections per level, default 16.
	EfConstruction int          // default 200.
	Dimension      int          // Vector dimensionality.
	DistanceFunc   DistanceFunc // default L2Distance.
	Seed           int64        // Seed for random level generation.
}

func NewHNSW(config Config) *HNSWIndex {
	if config.M <= 0 {
		config.M = 16
	}
	if config.EfConstruction <= 0 {
		config.EfConstruction = 200
	}
	if config.DistanceFunc == nil {
		config.DistanceFunc = L2Distance
	}
	if config.Seed == 0 {
		config.Seed = time.Now().UnixNano()
	}

	// normalization factor for level generation
	ml := 1.0 / math.Log(float64(config.M))

	return &HNSWIndex{
		M:              config.M,
		Mmax:           config.M,
		Mmax0:          config.M * 2,
		efConstruction: config.EfConstruction,
		ml:             ml,
		dimension:      config.Dimension,
		nodes:          make([]*Node, 0),
		entryPoint:     -1, // -1 表示还没有节点
		maxLevel:       -1,
		distFunc:       config.DistanceFunc,
		nodeLocks:      make([]sync.RWMutex, 0),
		rng:            rand.New(rand.NewSource(config.Seed)),
	}
}

// Add inserts a new vector into the HNSW index and returns its assigned node ID.
func (h *HNSWIndex) Add(vector []float32) (int, error) {
	if len(vector) != h.dimension {
		return -1, ErrDimensionMismatch
	}

	// Generate a random level for the new node
	level := h.randomLevel()

	// Create the new node
	h.globalLock.Lock()
	nodeID := len(h.nodes)

	// todo
}

// todo func Search

// Len returns the number of nodes in the HNSW index.
func (h *HNSWIndex) Len() int {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()
	return len(h.nodes)
}

// randomLevel generates a random level for a new node based on an exponential distribution.
func (h *HNSWIndex) randomLevel() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Generate a uniform random number in (0,1)
	uniform := h.rng.Float64()
	// Calculate the level using the negative logarithm
	// todo Explain why we use -ln(uniform) * ml
	level := int(math.Floor(-math.Log(uniform) * h.ml))
	return level
}

// SearchResult represents a single search result with its ID and distance.
type SearchResult struct {
	ID       int
	Distance float32
}

// 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
