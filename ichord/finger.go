package chord

import "sync"

type Finger struct {
	start UUID
	node  *Node
}

type FingerTable struct {
	table []Finger
	sync.Mutex
}

func (ft *FingerTable) ShowTable() []Finger {
	return ft.table[:]
}
