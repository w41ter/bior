package pd

import (
	"log"
	"bytes"
	"encoding/gob"
)

type Message interface {
	Reset()
}

func Marshal(msg Message) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func MustMarshal(msg Message) []byte {
	d, err := Marshal(msg)
	if err != nil {
		log.Panicf("marshal should never fail (%v)", err)
	}
	return d
}

func Unmarshal(msg Message, data []byte) error {
	buf := bytes.NewBuffer(data)
	decode := gob.NewDecoder(buf)
	if err := decode.Decode(msg); err != nil {
		return err
	}
	return nil
}

func MustUnmarshal(msg Message, data []byte) {
	if err := Unmarshal(msg, data); err != nil {
		log.Panicf("unmarshal should never fail (%v)", err)
	}
}

func MaybeUnmarshal(msg Message, data []byte) bool {
	if err := Unmarshal(msg, data); err != nil {
		return false
	}
	return true
}

func GetBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

func Boolp(b bool) *bool { return &b }
