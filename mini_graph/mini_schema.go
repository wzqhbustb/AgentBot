package minigraph

type StateSchema[S any] interface {
	Init() S
	Update(current, new S) (S, error)
}
