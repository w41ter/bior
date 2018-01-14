package raft

import "github.com/thinkermao/bior/utils/pd"

type Transport interface {
	Send(msg pd.Message) error
}
