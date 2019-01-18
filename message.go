package ichord

import (
	"bytes"
	"encoding/json"
)

type Purpose int

// Message types
const (
	_           Purpose = iota
	NodeJoin            // A node is joining the network
	NodeLeave           // A node is leaving the network
	HeartBeat           // Heartbeat signal
	NodeNotify          // Notified of node existence
	NodeAnn             // A node has been announced
	SucReq              // A request for a nodes successor
	PreReq              // A request for a nodes predecessor
	StatusError         // Response indicating an error
	StatusOk            // Simple status OK response
)

func statusOKMessage(sender *Node, key UUID) *Message {
	return NewMessage(sender, StatusOk, key, nil)
}

func statusErrMessage(sender *Node, key UUID, err error) *Message {
	return NewMessage(sender, StatusError, key, []byte(err.Error()))
}

// Create a new message
func NewMessage(sender *Node, purpose Purpose, key UUID, body []byte) *Message {
	// Sender and Target are filled in by the cluster upon sending the message
	return &Message{
		key:     key,
		body:    body,
		purpose: purpose,
		sender:  sender,
		hops:    0,
	}
}

// Represents a message in the DHT network
type Message struct {
	id      UUID    // Message unique id
	key     UUID    // Message Key
	purpose Purpose // The purpose of the message
	sender  *Node   // The node who sent the message
	target  *Node   // The target node of the message
	hops    int     // Number of hops so far taken by the message
	body    []byte  // Content of message
}

// Ele the message key
func (msg *Message) Key() UUID {
	return msg.key
}

// Ele the message body
func (msg *Message) Body() []byte {
	return msg.body
}

// Ele the message purpose
func (msg *Message) Purpose() Purpose {
	return msg.purpose
}

// Ele the message hops taken
func (msg *Message) Hops() int {
	return msg.hops
}

// Ele the message target node
func (msg *Message) Target() *Node {
	return msg.target
}

// Ele the message sender node
func (msg *Message) Sender() Node {
	return *msg.sender
}

// Extract the message body into the given value (must be a pointer), using the provided coder
func (msg *Message) DecodeBody(coder Coder, v interface{}) error {
	return coder.NewDecoder(bytes.NewBuffer(msg.body)).Decode(v)
}

func (msg *Message) String() string {
	data, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(data)
}
