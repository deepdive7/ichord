package ichord

import (
	"os"

	"github.com/deepdive7/icfg"
	"github.com/deepdive7/ilog"
)

type Application interface {
	ID() UUID
	Startup(args *icfg.Config) error
	ShutDown() error
	// When a error occurs in the functionality of the DHT
	OnError(err error)

	// Receive a message intended for the self node
	OnDeliver(msg *Message)

	// Received a message that needs to be routed onwards
	OnForward(msg *Message, node *Node) bool // return False if chord should not forward

	// A new node has joined the network
	OnNodeJoin(node *Node)

	// A node has left the network
	OnNodeExit(node *Node)

	// Received a heartbeat signal from a peer
	OnHeartbeat(node *Node)
}

func NewLogApp() *LogApp {
	return &LogApp{
		id:  NewID(),
		log: ilog.NewSimpleDefaultLogger(os.Stdout, 0, "logapp->", true),
	}
}

type LogApp struct {
	id  UUID
	log ilog.Logger
}

func (la *LogApp) ID() UUID {
	return la.id
}

func (la *LogApp) Startup(cfg *icfg.Config) error {
	return nil
}

func (la *LogApp) OnError(err error) {
	la.log.Error(err.Error())
}

func (la *LogApp) OnDeliver(msg *Message) {
	la.log.Info("OnDeliver:", msg.String())
}

func (la *LogApp) OnForward(msg *Message, node *Node) bool {
	la.log.Info("OnForward:", node.String())
	return true
}

func (la *LogApp) OnNodeJoin(node *Node) {
	la.log.Info("OnNodeJoin:", node.String())
}

func (la *LogApp) OnNodeExit(node *Node) {
	la.log.Info("OnNodeExit:", node.String())
}

func (la *LogApp) OnHeartbeat(node *Node) {
	la.log.Info("OnHeartbeat:", node.String())
}

func (la *LogApp) ShutDown() error {
	la.log = nil
	return nil
}
