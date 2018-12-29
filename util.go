package ichord

import (
	"bytes"
	"errors"
	"math/rand"
)

func NewError(err string) error {
	return errors.New(err)
}

// calculate offset, id + 2^(exp-1) mod 2^m
//func powerOffset(id []byte, exp int) int {
//}

// Calculate a random number between n, and m
func randRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

// Checks if key is between id1 and id2 exclusivly
func between(id1, id2, key UUID) bool {
	return bytes.Compare(key.NodeID(), id1.NodeID()) == 1 &&
		bytes.Compare(key.NodeID(), id2.NodeID()) == -1
}

// Checks if key E (id1, id2]
func betweenRightInc(id1, id2, key UUID) bool {
	return (bytes.Compare(key.NodeID(), id1.NodeID()) == 1 &&
		bytes.Compare(key.NodeID(), id2.NodeID()) == -1) ||
		bytes.Equal(key.NodeID(), id2.NodeID())
}

// Checks if key E [id1, id2)
func betweenLeftInc(id1, id2, key UUID) bool {
	return (bytes.Compare(key.NodeID(), id1.NodeID()) == 1 &&
		bytes.Compare(key.NodeID(), id2.NodeID()) == -1) ||
		bytes.Equal(key.NodeID(), id1.NodeID())
}

// Cluster logging //

func Debugf(format string, v ...interface{}) {
	chordLogger.Debugf(format, v...)
}

func Warnf(format string, v ...interface{}) {
	chordLogger.Warnf(format, v...)
}

func Errorf(format string, v ...interface{}) {
	chordLogger.Errorf(format, v...)
}

func Debug(v ...interface{}) {
	chordLogger.Debug(v...)
}

func Warn(v ...interface{}) {
	chordLogger.Warn(v...)
}

func Error(v ...interface{}) {
	chordLogger.Error(v...)
}
