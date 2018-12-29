package chord

type Application interface {
	// When a error occurs in the functionality of the DHT
	OnError(err error)

	// Receive a message intended for the self node
	OnDeliver(msg *Message)

	// Received a message that needs to be routed onwards
	OnForward(msg *Message, node *Node) bool // return False if chord should not forward

	// A new node has joined the network
	OnNodeJoin(node Node)

	// A node has left the network
	OnNodeExit(node Node)

	// Received a heartbeat signal from a peer
	OnHeartbeat(node Node)
}
