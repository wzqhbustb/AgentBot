package minigraph

import "errors"

// END is a special constant used to represent the end node in the graph.
const END = "END"

var (
	// ErrEntryPointNotSet is returned when the entry point of the graph is not set.
	ErrEntryPointNotSet = errors.New("entry point not set")

	// ErrNodeNotFound is returned when a node is not found in the graph.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNoOutgoingEdge is returned when no outgoing edge is found for a node.
	ErrNoOutgoingEdge = errors.New("no outgoing edge found for node")
)

type Edge struct {
	From string
	To   string
}
