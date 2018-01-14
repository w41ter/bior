package walpd

import "encoding/gob"

type Record struct {
	Type int32
	Crc  uint32
	Data []byte
}

func (m *Record) Reset() { *m = Record{} }

func init() {
	gob.Register(Record{})
}
