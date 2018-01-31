package raft

import (
	"github.com/thinkermao/bior/utils/pd"
)

// Transporter is interface used by raft to send
// Message to others.
type Transporter interface {
	Send(to uint64, msg pd.Messager) error
}
