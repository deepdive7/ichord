package ichord

import (
	"bytes"
	"encoding/json"

	"github.com/deepdive7/icodec"
)

type Purpose int

// Message types
const (
	NODE_JOIN    Purpose = iota // A node is joining the network
	NODE_LEAVE                  // A node is leaving the network
	HEART_BEAT                  // Heartbeat signal
	NODE_NOTIFY                 // Notified of node existense
	NODE_ANN                    // A node has been announced
	SUCC_REQ                    // A request for a nodes successor
	PRED_REQ                    // A request for a nodes predecessor
	STATUS_ERROR                // Response indicating an error
	STATUS_OK                   // Simple status OK response
)

// Helper utilies for creating specific messages

func nodeJoinMessage(sender *Node, key UUID) *Message {
	return NewMessage(sender, NODE_JOIN, key, nil)
}

func heartBeatMessage(sender *Node, key UUID) *Message {
	return NewMessage(sender, HEART_BEAT, key, nil)
}

func notifyMessage(sender *Node, key UUID) *Message {
	return NewMessage(sender, NODE_NOTIFY, key, nil)
}

func statusOKMessage(sender *Node, key UUID) *Message {
	return NewMessage(sender, STATUS_OK, key, nil)
}

func statusErrMessage(sender *Node, key UUID, err error) *Message {
	return NewMessage(sender, STATUS_ERROR, key, []byte(err.Error()))
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

// Extract the message body into the given value (must be a pointer), using the provided codec
func (msg *Message) DecodeBody(codec icodec.Codec, v interface{}) error {
	return codec.NewDecoder(bytes.NewBuffer(msg.body)).Decode(v)
}

func (msg *Message) String() string {
	data, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(data)
}
