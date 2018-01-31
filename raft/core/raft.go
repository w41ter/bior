package core

import (
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
)

// Raft raft interface provides the driver to run
// the entire raft algorithm, and the query of raft status.
type Raft interface {
	// Read status of raft.
	ReadSoftState() SoftState
	ReadHardState() raftpd.HardState
	ReadConfState() raftpd.ConfState

	// Propose.
	Read(context []byte) bool
	Step(msg *raftpd.Message)
	Periodic(millsSinceLastPeriod int)

	// Propose first test whether the current role is leader,
	// if true adds the log to the queue and returns index
	// and term; otherwise it returns false.
	Propose(bytes []byte) (uint64, uint64, bool)
	ProposeConfChange(cc *raftpd.ConfChange) (uint64, uint64, bool)

	// Apply change.
	ApplySnapshot(metadata *raftpd.SnapshotMetadata)
	ApplyConfChange(cc *raftpd.ConfChange) raftpd.ConfState

	Ready() Ready
	ReadStatus() (uint64, bool)
}

// MakeRaft return a Raft interface.
func MakeRaft(config *conf.Config, app NodeApplication) Raft {
	return MakeRawNode(config, app)
}
