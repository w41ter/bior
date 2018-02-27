package raft

import (
	"encoding/gob"
)

type packetType int

const (
	typeHeartbeat packetType = iota
	typeNormal
)

type Packet struct {
	PkgType packetType
	Msg     interface{}
}

func (pkg *Packet) Reset() { *pkg = Packet{} }

func init() {
	gob.Register(Packet{})
}
