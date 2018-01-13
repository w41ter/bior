package raft

import (
	"llkv/core/proto"
	"llkv/core/utils/pd"
)

type Transport interface {
	Send(msg pd.Message) error
}
