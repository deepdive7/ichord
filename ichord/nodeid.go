package chord

import (
	"bytes"

	"github.com/google/uuid"
)

// Represents a NodeID in the form of a hash
// Unless you know what you are doing, do not create a
// NodeID yourself, always use NodeIDFromBytes().

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
