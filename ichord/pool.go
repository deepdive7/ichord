package chord

import (
	"bytes"
	"encoding/json"
	"net"
	"sync"
)

var (
	gPool = &ConnPool{cs: map[UUID]net.Conn{}, mu: &sync.RWMutex{}}
)

type ConnPool struct {
	cs map[UUID]net.Conn
	mu *sync.RWMutex
}

func (cp *ConnPool) AddConn(id UUID, conn net.Conn) bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if _, ok := cp.cs[id]; ok {
		return false
	}
	cp.cs[id] = conn
	return true
}

func (cp *ConnPool) DelConn(id UUID) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if _, ok := cp.cs[id]; !ok {
		return
	}
	delete(cp.cs, id)
}

func (cp *ConnPool) SendTo(id UUID, data []byte) (n int, err error) {
	return cp.cs[id].Write(data)
}

func (cp *ConnPool) ReadFrom(id UUID) (data []byte, err error) {
	var conn net.Conn
	if _, ok := cp.cs[id]; !ok {
		return nil, NewError("No connection for " + id.String())
	}
	buf := bytes.NewBuffer(nil)
	tmp := make([]byte, 128, 128)
	n := 0
	for {
		n, err = conn.Read(tmp)
		if n == 0 {
			if err.Error() != "EOF" {
				return nil, err
			}
			break
		}
		buf.Write(tmp[:n])
	}
	return buf.Bytes(), nil
}

func (cp *ConnPool) SRMsg(id UUID, req *Message) (resp *Message, err error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = cp.SendTo(id, data)
	if err != nil {
		return nil, err
	}
	rData, err := gPool.ReadFrom(id)
	if err != nil {
		return nil, err
	}
	resp = &Message{}
	err = json.Unmarshal(rData, resp)
	return resp, err
}

func (cp *ConnPool) Close() (err error) {
	for _, v := range cp.cs {
		err = v.Close()
	}
	return
}
