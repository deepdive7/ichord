package ichord

import (
	"encoding/gob"
	"encoding/json"
	"io"
)

func NewCoder(t string) Coder {
	switch t {
	case "json":
		return NewJsonCoder()
	case "gob":
		return NewGobCoder()
	default:
		return NewJsonCoder()
	}
}

// Standard encoder interface, usually created from an io.Writer
// Important: Needs to be stateless, in that each call to Encode() must
// act the same as any other call
type Encoder interface {
	// Encode given interface, or error
	Encode(v interface{}) error
}

// Standard decoder interface, usually created from an io.Reader
// Important: Needs to be stateless, in that each call to Decode() must
// act the same as any other call
type Decoder interface {
	// Decode into the given interface, or error
	Decode(v interface{}) error
}

// A coder for coding ghord messages
type Coder interface {
	Name() string
	NewEncoder(io.Writer) Encoder
	NewDecoder(io.Reader) Decoder
}

type JsonCoder struct{}

func NewJsonCoder() Coder {
	return &JsonCoder{}
}

func (c *JsonCoder) Name() string {
	return "json"
}

func (c *JsonCoder) NewEncoder(w io.Writer) Encoder {
	return json.NewEncoder(w)
}

func (c *JsonCoder) NewDecoder(r io.Reader)  Decoder {
	return json.NewDecoder(r)
}

type GobCoder struct{}

func NewGobCoder() Coder {
	return &GobCoder{}
}

func (g *GobCoder) NewEncoder(w io.Writer) Encoder {
	return gob.NewEncoder(w)
}

func (g *GobCoder) NewDecoder(r io.Reader) Decoder {
	return gob.NewDecoder(r)
}

func (g *GobCoder) Name() string{
	return "gob"
}