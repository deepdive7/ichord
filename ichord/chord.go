package chord

import (
	"bytes"
	"errors"
	"net"
	"os"
	"time"

	jsons "encoding/json"

	"github.com/deepdive7/icodec"
	"github.com/deepdive7/icodec/json"
	"github.com/deepdive7/ilog"
	"github.com/deepdive7/iveil"
)

var (
	chordLogger ilog.Logger
)

func init() {
	chordLogger = ilog.NewSimpleDefaultLogger(os.Stdout, ilog.DEBUG, "ichord->", true)
}

type API interface {
	Get(key []byte) ([]byte, bool)
	Set(key, value []byte) bool

	Listen(l net.Listener) error
	Register(id UUID, app Application) error
	Send(msg *Message) (*Message, error)
	Ping(node *Node) bool
	Encode(dataBuf *bytes.Buffer, v interface{}) error
	Decode(dataBuf *bytes.Buffer, v interface{}) error

	// maintain will periodically perform maintenance operations
	Maintain()
	Sync()
	Count() int
	ShutDown() error

	Join(id UUID) (*Node, error)
	Leave(id UUID)
	Route(key UUID) (*Node, error)
	// Lookup
	Lookup(key UUID) (*Node, error)
	//Find the first successor for the given ID
	FindSuccessor(key UUID) (*Node, error)
	//Find the first predecessor for the given ID
	GetPredecessor(key UUID) (*Node, error)

	List() ([]*Node, error)
	//Stabilize successor/predecessor pointers
	Stabilize() error
	//Notify a Node of our existence
	Notify(n *Node) (*Message, error)
	// Fix fingers table
	FixFingers()
	// Show fingers table
	ShowFingers() []Finger
}

// Create a new Cluster
func NewChord(self *Node, args map[string]interface{}) *Chord {
	hasher := iveil.NewBlake2b()

	return &Chord{
		self:          self,
		kill:          make(chan bool),
		stabilizeMin:  time.Second * 5,
		stabilizeMax:  time.Second * 10,
		heartbeatFreq: time.Second * 10,
		codec:         json.NewCodec(),
		hasher:        hasher,
		numFingers:    hasher.Size(),
	}
}

type Chord struct {
	self        *Node
	hash        *iveil.Blake2b
	kill        chan bool
	apps        []Application
	fingerTable *FingerTable
	numFingers  int
	conn        chan net.Conn

	// Codec & Hashing Suites
	codec  icodec.Codec
	hasher *iveil.Blake2b

	stabilizeMin  time.Duration
	stabilizeMax  time.Duration
	heartbeatFreq time.Duration
}

func (c *Chord) Get(key []byte) ([]byte, bool) {
	panic("implement me")
}

func (c *Chord) Set(key, value []byte) bool {
	panic("implement me")
}

func (c *Chord) Listen(ln net.Listener) error {
	defer ln.Close()

	go func(ln net.Listener, cc chan net.Conn) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				c.throwErr(err)
				return
			}
			Debug("Recieved connections")
			cc <- conn
		}
	}(ln, c.conn)

	for {
		select {
		case <-c.kill:
			return nil
		case <-time.After(c.heartbeatFreq):
			Debug("Sending heartbeats")
			//go c.sendHeartbeats()
			// Run the stabilize routine randomly between stabilizeMin and stabilizeMax
		case <-time.After(time.Duration(randRange(c.stabilizeMin.Nanoseconds(), c.stabilizeMax.Nanoseconds()))):
			Debug("Running stabilize routine")
			c.Stabilize()
		case conn := <-c.conn:
			Debug("Handling connection")
			go c.handleConn(conn)
		}
	}
	return nil
}

func (c *Chord) Register(id UUID, app Application) error {
	panic("implement me")
}

func (c *Chord) Ping(node *Node) bool {
	panic("implement me")
}

func (c *Chord) Encode(dataBuf *bytes.Buffer, v interface{}) error {
	return c.codec.NewEncoder(dataBuf).Encode(v)
}

func (c *Chord) Decode(dataBuf *bytes.Buffer, v interface{}) error {
	return c.codec.NewDecoder(dataBuf).Decode(v)
}

// Send a message through the network to it's intended Node
func (c *Chord) Send(msg *Message) (*Message, error) {
	Debug("Sending message with key %v", msg.key)
	// find the appropriate node in our list of known nodes
	target, err := c.Route(msg.key)
	if err != nil {
		c.throwErr(err)
		return nil, err
	}
	if Equal(target.Id, c.self.Id) {
		if msg.purpose > PRED_REQ {
			c.onDeliver(msg)
		}
		return nil, nil
	}

	// decide if our application permits the message
	if c.forward(msg, target) {
		// send the message
		resp, err := gPool.SRMsg(target.Id, msg)
		if err != nil {
			c.throwErr(err)
			return nil, err
		} else if resp.purpose == STATUS_ERROR {
			c.throwErr(err)
			return resp, errors.New(string(resp.body))
		}
		return resp, nil
	}
	return nil, nil
}

func (c *Chord) Maintain() {
	panic("implement me")
}

// TODO Sync reserved
func (c *Chord) Sync() {
	panic("implement me")
}

// TODO Count calculate total count of nodes in the network
func (c *Chord) Count() int {
	panic("implement me")
}

func (c *Chord) ShutDown() error {
	Debug("Killing cluster")
	c.kill <- true
	return gPool.Close()
}

// TODO Node join
func (c *Chord) Join(id UUID) (*Node, error) {
	/*
		Debug("Joining Chord DHT using source node %v:%v", ip, port)
		address := ip + ":" + strconv.Itoa(port)
		var buf *bytes.Buffer

		// get our successor in the network
		succReq := c.NewMessage(NODE_JOIN, c.self.Id, nil)
		resp, err := c.sendToIP(address, succReq)
		if err != nil {
			return err
		} else if resp.purpose == STATUS_ERROR {
			return errors.New(string(resp.body))
		}

		// parse the successor
		var succ *Node
		buf = bytes.NewBuffer(resp.body)
		err = c.Decode(buf, succ)
		if err != nil {
			return err
		}
		c.self.successor = succ

		// reset buffer for reuse
		buf.Reset()

		// Ele our successors predecessor
		predReq := c.NewMessage(PRED_REQ, succ.Id, nil)
		resp, err = c.sendToIP(succ.Host, predReq)
		if err != nil {
			return err
		} else if resp.purpose == STATUS_ERROR {
			return errors.New(string(resp.body))
		}

		// parse the predecessor
		var pred *Node
		buf.Write(resp.body)
		err = c.Decode(buf, pred)
		if err != nil {
			return err
		}
		c.self.predecessor = pred

		// Notify the specified successor of our existence
		resp, err = c.notify(c.self.successor)
		if err != nil {
			return err
		} else if resp.purpose == STATUS_ERROR {
			return errors.New(string(resp.body))
		}

		return nil
	*/
	return nil, nil
}

func (c *Chord) Leave(id UUID) {
	panic("implement me")
}

func (c *Chord) Route(key UUID) (*Node, error) {
	Debug("Determining route to the given NodeID: %v", key)
	// check if we are responsible for the key
	if betweenRightInc(c.self.predecessor.Id, c.self.Id, key) {
		Debug("I'm the target. Delievering message %v", key)
		return c.self, nil
	}

	// check if our successor is responsible for the key
	if betweenRightInc(c.self.Id, c.self.successor.Id, key) {
		// our successor is the desired node
		Debug("Our successor is the target. Delievering message %s", key)
		return c.self.successor, nil
	}

	// finally check if one our fingers is closer to (or is) the desired node
	Debug("Checking fingerTable for target node...")
	return c.GetPredecessor(key)
}

func (c *Chord) Lookup(key UUID) (*Node, error) {
	panic("implement me")
}

func (c *Chord) FindSuccessor(key UUID) (*Node, error) {
	Debug("Finding successor to key %v", key)
	request := NewMessage(c.self, SUCC_REQ, key, nil)
	response, err := c.Send(request)
	if err != nil {
		return nil, err
	}

	if response.purpose == STATUS_ERROR {
		return nil, errors.New(string(response.body))
	}

	var node *Node
	err = jsons.Unmarshal(response.body, &node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (c *Chord) GetPredecessor(key UUID) (*Node, error) {
	Debug("Finding closest node in our finger table to node %v", key)
	prev := c.self
	for _, finger := range c.fingerTable.table {
		if betweenRightInc(prev.Id, finger.node.Id, key) {
			return finger.node, nil
		}
		prev = finger.node
	}
	return nil, NewError("No node exists for given id")
}

func (c *Chord) List() ([]*Node, error) {
	panic("implement me")
}

func (c *Chord) Stabilize() error {
	Debug("stabilizing...")
	// craft message for pred_req
	predReq := NewMessage(c.self, PRED_REQ, c.self.successor.Id, nil)
	data, err := jsons.Marshal(predReq)
	_, err = gPool.SendTo(c.self.successor.Id, data)
	if err != nil {
		return err
	}

	var predecessor *Node
	body, err := gPool.ReadFrom(c.self.successor.Id)
	err = c.Decode(bytes.NewBuffer(body), predecessor)

	// check if our sucessor has a diff predecessor then us
	// if so notify the new successor and update our own records
	if Equal(c.self.Id, predecessor.Id) {
		Debug("Found new predecessor! Id: %v", predecessor.Id)
		resp, err := c.Notify(predecessor)
		if err != nil {
			return err
		} else if resp.purpose == STATUS_ERROR {
			return errors.New(string(resp.body))
		}

		c.self.successor = predecessor
	}

	return nil
}

func (c *Chord) Notify(n *Node) (*Message, error) {
	Debug("Notifying node: %v of our existence", n.Id)
	ann := NewMessage(c.self, NODE_NOTIFY, n.Id, nil)
	return gPool.SRMsg(n.Id, ann)
}

func (c *Chord) FixFingers() {
	panic("implement me")
}

func (c *Chord) ShowFingers() []Finger {
	panic("implement me")
}

// Internal Methods
func (c *Chord) newSock(conn net.Conn) *sock {
	//conn.SetNoDelay(true)
	//conn.SetKeepAlive(true)
	return &sock{
		host: conn.RemoteAddr().String(),
		conn: conn,
		enc:  c.codec.NewEncoder(conn),
		dec:  c.codec.NewDecoder(conn),
	}
}

// Handle new connections
func (c *Chord) handleConn(conn net.Conn) {
	Debug("Recieved a new connection")

	sock := c.newSock(conn)
	defer conn.Close()
	var msg *Message
	var resp *Message
	var err error

	if err := sock.read(msg); err != nil {
		c.throwErr(err)
		return
	}
	Debug("Recieved a message with purpose %v from node %v", msg.purpose, msg.sender)
	msg.hops++

	switch msg.purpose {
	// A node wants to join the network
	// we need to find his appropriate successor
	case NODE_JOIN:
		resp, err = c.onNodeJoin(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// A node wants to leave the network
		// so update our finger table accordingly
	case NODE_LEAVE:
		break

		// Recieved a heartbeat message from a connected
		// client, let them know were still alive
	case HEART_BEAT:
		resp = c.onHeartBeat(msg)

		// We've been notified of a new predecessor
		// node, so update our fingerTable
	case NODE_NOTIFY:
		resp, err = c.onNotify(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Recieved a successor request from a node,
	case SUCC_REQ:
		resp, err = c.onSuccessorRequest(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Recieved a predecessor request from a node
	case PRED_REQ:
		resp, err = c.onPredecessorRequest(msg)
		if err != nil {
			resp = statusErrMessage(c.self, msg.sender.Id, err)
			c.throwErr(err)
		}

		// Not an internal message, forward or deliver it
	default:
		resp = statusOKMessage(c.self, msg.sender.Id)
		c.Send(msg)
	}

	sock.write(resp)
}

//////////////////////////////////////////////
//											//
//  Internal Chord Application handlers 	//
//											//
//////////////////////////////////////////////

func (c *Chord) onDeliver(msg *Message) {
	Debug("Delivering message to registered applications")
	for _, app := range c.apps {
		app.OnDeliver(msg)
	}
}

func (c *Chord) onHeartBeat(msg *Message) *Message {
	Debug("Recieved heartbeat message from node %v", msg.sender.Id)
	for _, app := range c.apps {
		go app.OnHeartbeat(*msg.sender)
	}
	return NewMessage(c.self, STATUS_OK, msg.sender.Id, nil)
}

func (c *Chord) onNodeJoin(msg *Message) (*Message, error) {
	Debug("Recieved node join message from node %v", msg.sender.Id)
	req := NewMessage(c.self, SUCC_REQ, msg.sender.Id, nil)
	return c.Send(req)
}

// Called when a NODE_LEAVE message is recieved
func (c *Chord) onNodeLeave(msg *Message) {}

func (c *Chord) onNotify(msg *Message) (*Message, error) {
	Debug("Node %v is notifying us of its existence", msg.sender.Id)
	err := msg.DecodeBody(c.codec, &c.self.predecessor)
	if err != nil {
		return statusErrMessage(c.self, msg.sender.Id, err), err
	}
	return statusOKMessage(c.self, msg.sender.Id), nil
}

// Handle a succesor request we've recieved
func (c *Chord) onSuccessorRequest(msg *Message) (*Message, error) {
	Debug("Recieved successor request from node %v", msg.sender.Id)
	if c.self.IsResponsible(msg.target.Id) {
		// send successor
		buf := bytes.NewBuffer(make([]byte, 0))
		err := c.Encode(buf, c.self.successor)
		if err != nil {
			return statusErrMessage(c.self, msg.sender.Id, err), err
		}
		return NewMessage(c.self, SUCC_REQ, msg.sender.Id, buf.Bytes()), nil
	} else {
		// forward it on
		go c.Send(msg)
		return statusOKMessage(c.self, msg.sender.Id), nil
	}
}

func (c *Chord) onPredecessorRequest(msg *Message) (*Message, error) {
	Debug("Recieved predecessor request from node: %v", msg.sender.Id)
	if c.self.IsResponsible(msg.target.Id) {
		// send successor
		buf := bytes.NewBuffer(make([]byte, 0))
		err := c.Encode(buf, c.self.predecessor)
		if err != nil {
			return statusErrMessage(c.self, msg.sender.Id, err), err
		}
		return NewMessage(c.self, PRED_REQ, msg.sender.Id, buf.Bytes()), nil
	} else {
		// forward it on
		go c.Send(msg)
		return statusOKMessage(c.self, msg.sender.Id), nil
	}
}

// Decide whether or not to continue forwarding the message through the network
func (c *Chord) forward(msg *Message, next *Node) bool {
	Debug("Checking if we should forward the given message")
	forward := true

	for _, app := range c.apps {
		forward = forward && app.OnForward(msg, next)
	}

	return forward
}

// Handle any cluster errors
func (c *Chord) throwErr(err error) {
	Error(err.Error())
	// Send the error through all the embedded apps
	for _, app := range c.apps {
		app.OnError(err)
	}
}
