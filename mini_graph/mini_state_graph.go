package minigraph

import "context"

type StateGraph[S any] struct {
	nodes            map[string]TypedNode[S]
	edges            []Edge
	conditionalEdges map[string]func(ctx context.Context, state S) string
	entryPoint       string
}

type TypedNode[S any] struct {
	Name        string
	Description string
	Function    func(ctx context.Context, state S) (S, error)
}

type RetryPolicy struct {
	MaxRetries      int
	BackoffStrategy BackoffStrategy
	RetryableErrors []string
}

type BackoffStrategy int
