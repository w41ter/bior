package core

import (
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/read"
	"github.com/thinkermao/bior/raft/proto"
)

type raftOpt func(c *core)

func vote(idx uint64) raftOpt {
	return func(c *core) {
		c.vote = idx
	}
}

func term(idx uint64) raftOpt {
	return func(c *core) {
		c.term = idx
	}
}

func randTick(tick int) raftOpt {
	return func(c *core) {
		c.randomizedElectionTick = tick
	}
}

func timeElapsed(time int) raftOpt {
	return func(c *core) {
		c.timeElapsed = time
	}
}

func leaderID(idx uint64) raftOpt {
	return func(c *core) {
		c.leaderID = idx
	}
}

func state(state StateRole) raftOpt {
	return func(c *core) {
		c.state = state
	}
}

func makeTestRaft(
	id uint64,
	peers []uint64,
	election, heartbeat int,
	entries []raftpd.Entry,
	callback application,
	opts ...raftOpt,
) *core {
	c := conf.Config{
		ID:            id,
		Vote:          conf.InvalidID,
		Term:          conf.InvalidTerm,
		ElectionTick:  election,
		HeartbeatTick: heartbeat,
		Nodes:         peers,
		MaxSizePreMsg: 1024,
		Entries:       entries,
	}

	raft := makeCore(&c, callback)

	for _, opt := range opts {
		opt(raft)
	}
	return raft
}

type appImpl struct {
	sendCB          func(msg *raftpd.Message)
	saveReadStateCB func(readStatus *read.ReadState)
	readSnapshotCB  func() *raftpd.Snapshot
	applyEntryCB    func(entry *raftpd.Entry)
	applySnapshotCB func(snapshot *raftpd.Snapshot)
}

func (a *appImpl) send(msg *raftpd.Message) {
	if a.sendCB != nil {
		a.sendCB(msg)
	}
}

func (a *appImpl) saveReadState(readStatus *read.ReadState) {
	if a.saveReadStateCB != nil {
		a.saveReadStateCB(readStatus)
	}
}

func (a *appImpl) applyEntry(entry *raftpd.Entry) {
	if a.applyEntryCB != nil {
		a.applyEntryCB(entry)
	}
}

func (a *appImpl) applySnapshot(snapshot *raftpd.Snapshot) {
	if a.applySnapshotCB != nil {
		a.applySnapshotCB(snapshot)
	}
}

func (a *appImpl) readSnapshot() *raftpd.Snapshot {
	if a.readSnapshotCB != nil {
		return a.readSnapshotCB()
	}
	return nil
}
