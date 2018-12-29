package chord

func NewNode(id UUID, host string, port int) *Node {
	node := &Node{Id: id, Host: host, Port: port}
	return node
}

type Node struct {
	Id          UUID
	Host        string
	Port        int
	successor   *Node
	predecessor *Node
}

// Returns true if this node is responsible for the given UUID
func (n *Node) IsResponsible(id UUID) bool {
	return (Greater(n.Id, n.predecessor.Id) && Less(id, n.Id)) || Equal(id, n.Id)
}
