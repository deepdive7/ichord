package ichord

import (
	"encoding/hex"
	"fmt"
	"sync"
)

func toStr(key []byte) string {
	return fmt.Sprintf("%x", key)
}

func toByte(key string) []byte {
	d, err := hex.DecodeString(key)
	if err != nil {
		return []byte{}
	}
	return d
}

type KvDb interface {
	Name() string
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error
	Close() error
	Drop()
}

func NewMemKvDb(dbName string) (mkd *MemKvDb) {
	mkd = &MemKvDb{}
	mkd.name = dbName
	mkd.data = make(map[string][]byte)
	mkd.mu = &sync.RWMutex{}
	mkd.closed = false
	return mkd
}

type MemKvDb struct {
	name   string
	data   map[string][]byte
	mu     *sync.RWMutex
	closed bool
}

func (mkd *MemKvDb) Name() string {
	return mkd.name
}

func (mkd *MemKvDb) Set(key, value []byte) error {
	if mkd.closed {
		return NewError("db closed")
	}
	mkd.mu.Lock()
	defer mkd.mu.Unlock()
	mkd.data[toStr(key)] = value
	return nil
}

func (mkd *MemKvDb) Get(key []byte) ([]byte, error) {
	if mkd.closed {
		return nil, NewError("db closed")
	}
	mkd.mu.RLock()
	defer mkd.mu.RUnlock()
	if d, ok := mkd.data[toStr(key)]; ok {
		return d, nil
	}
	return nil, NewError("no such key:" + toStr(key))
}

func (mkd *MemKvDb) Del(key []byte) error {
	if mkd.closed {
		return NewError("db closed")
	}
	mkd.mu.Lock()
	defer mkd.mu.Unlock()
	if _, ok := mkd.data[toStr(key)]; ok {
		delete(mkd.data, toStr(key))
	}
	return nil
}

func (mkd *MemKvDb) Close() error {
	if mkd == nil {
		return NewError("db is nil")
	}
	mkd.mu.Lock()
	defer mkd.mu.Unlock()
	mkd.closed = true
	return nil
}

func (mkd *MemKvDb) Drop() {
	mkd.mu.Lock()
	defer mkd.mu.Unlock()
	mkd.data = make(map[string][]byte)
}
