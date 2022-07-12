package codec

import "io"

type Header struct {
	SrvMod string
	Seq    uint64
	Err    string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	Gob  Type = "application/gob"
	Json Type = "application/json"
)

var CodecMap map[Type]NewCodecFunc

func init() {
	CodecMap = make(map[Type]NewCodecFunc)
}
