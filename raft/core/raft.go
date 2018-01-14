package core

import (
	"github.com/thinkermao/bior/raft/proto"
)

type Raft interface {
	Periodic(millsSinceLastPeriod int)
	Propose(bytes []byte) (uint64, uint64, bool)
	ProposeConfChange(cc *raftpd.ConfChange) (uint64, uint64, bool)
	ApplyConfChange(cc *raftpd.ConfChange) raftpd.ConfState
	CompactTo(metadata *raftpd.SnapshotMetadata)
	Read(context []byte)
	Step(msg *raftpd.Message)
	Ready() Ready
}

func MakeRaft(config *Config, app NodeApplication) Raft {
	return MakeRawNode(config, app)
}
