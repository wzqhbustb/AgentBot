package minigraph

import "context"

type StateGraph[S any] struct {
	// todo
}

type TypedNode[S any] struct {
	// todo
	Name        string
	Description string
	Function    func(ctx context.Context, state S) (S, error)
}
