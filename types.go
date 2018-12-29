package ichord

import (
	"bytes"
	"net"
	"sync"

	"github.com/deepdive7/icodec"

	"github.com/google/uuid"
)

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

func (n *Node) String() string {
	return ""
}

func NewID() UUID {
	return uuid.New()
}

type UUID = uuid.UUID

// Returns true iff NodeID n < id
func Less(a, b UUID) bool {
	return bytes.Compare(a.NodeID(), b.NodeID()) == -1
}

// Returns true iff NodeID n > id
func Greater(a, b UUID) bool {
	return bytes.Compare(a.NodeID(), b.NodeID()) == 1
}

// Returns true iff NodeID n == id
func Equal(a, b UUID) bool {
	return bytes.Compare(a.NodeID(), b.NodeID()) == 0
}

type sock struct {
	host string
	conn net.Conn
	enc  icodec.Encoder
	dec  icodec.Decoder
}

// write to the sock via an encoder
func (s *sock) write(m *Message) error {
	return s.enc.Encode(m)
}

// read an encoded value from the sock
func (s *sock) read(m *Message) error {
	return s.dec.Decode(m)
}
