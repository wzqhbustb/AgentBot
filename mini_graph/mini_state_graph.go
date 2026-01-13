package minigraph

import "context"

type StateGraph[S any] struct {
	nodes            map[string]TypedNode[S]
	edges            []Edge
	conditionalEdges map[string]func(ctx context.Context, state S) string
	entryPoint       string
	retryPolicy      *RetryPolicy
	stateMerger      TypedStateMerger[S]
	Schema           StateSchema[S]
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

// StateMerger is a typed function to merge states from parallel execution.
type TypedStateMerger[S any] func(ctx context.Context, currentState S, newStates []S) (S, error)
