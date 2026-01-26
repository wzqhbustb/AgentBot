package hnsw

// Node represents a single node in the HNSW graph.
type Node struct {
	id     int       // Unique identifier for the node.
	vector []float32 // The vector associated with the node.
	level  int       // The level of the node in the HNSW hierarchy.

	connections [][]int // Connections to other nodes at different levels.
}

// todo
