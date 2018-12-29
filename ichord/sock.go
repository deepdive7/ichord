package chord

import (
	"net"

	"github.com/deepdive7/icodec"
)

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
