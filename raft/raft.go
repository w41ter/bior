package raft

import (
	"github.com/thinkermao/bior/raft/core"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/raft/wal"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/pd"
	"sync"
	"time"
)

type Application interface {
	applyEntry(entry *raftpd.Entry)
	readStateNotice(idx uint64, bytes []byte)
	applySnapshot(snapshot *raftpd.Snapshot)
	readSnapshot() *raftpd.Snapshot
}

type Raft struct {
	mutex sync.Mutex

	id uint64

	raft core.Raft
	wal  *wal.Wal

	timerStopper chan struct{}
	callback     Application
	transport    Transport
}

func MakeRaft(id uint64, nodes []uint64,
	electionTimeout, heartbeatTimeout int, tickSize int,
	walDir string, application Application, transport Transport) (*Raft, error) {
	raft := &Raft{id: id}
	raft.callback = application
	raft.transport = transport

	config := core.Config{
		Id:            id,
		Vote:          core.InvalidId,
		Term:          core.InvalidTerm,
		ElectionTick:  electionTimeout,
		HeartbeatTick: heartbeatTimeout,
		Nodes:         nodes,
		Entries:       nil,
	}

	raft.raft = core.MakeRaft(&config, application)

	w, err := wal.CreateWal(walDir, core.InvalidIndex+1)
	if err != nil {
		return nil, err
	}
	raft.wal = w

	raft.service(tickSize)

	return raft, nil
}

func RebuildRaft(id uint64, logSequenceNumber uint64,
	nodes []uint64, electionTimeout, heartbeatTimeout int, tickSize int,
	walDir string, application Application, transport Transport) (*Raft, error) {
	w, err := wal.Open(walDir, logSequenceNumber)
	if err != nil {
		return nil, err
	}

	state, entries, err := w.ReadAll()
	if err != nil {
		return nil, err
	}

	raft := &Raft{id: id}
	raft.callback = application
	raft.transport = transport
	config := core.Config{
		Id:            id,
		Vote:          state.Vote,
		Term:          state.Term,
		ElectionTick:  electionTimeout,
		HeartbeatTick: heartbeatTimeout,
		Nodes:         nodes,
		Entries:       entries,
	}
	raft.raft = core.MakeRaft(&config, application)
	raft.wal = w

	raft.service(tickSize)

	return raft, nil
}

// Kill is the only one global method no need mutex.
func (raft *Raft) Kill() {
	raft.timerStopper <- struct{}
}

// Read operate not sync disk
func (raft *Raft) Read(bytes []byte) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	raft.raft.Read(bytes)
}

// Write write operate will sync disk.
func (raft *Raft) Write(bytes []byte) (uint64, bool) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	// Check is leader
	index, _, isLeader := raft.raft.Propose(bytes)
	if !isLeader {
		return 0, false
	}
	return index, true
}

func (raft *Raft) Compact(snapshot *raftpd.Snapshot) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	raft.raft.CompactTo(&snapshot.Metadata)
}

func (raft *Raft) handleRaftReady() {
	ready := raft.raft.Ready()

	// FIXME: 在save之前可以先处理 readStateNotice
	raft.wal.Save(ready.HS, ready.Entries)

	for i := 0; i < len(ready.CommitEntries); i++ {
		// FIXME: 是否有必要将更改配置信息应用
		raft.callback.applyEntry(&ready.CommitEntries[i])
		if ready.CommitEntries[i].Type == raftpd.EntryConfChange {
			cc := raftpd.ConfChange{}
			pd.MustUnmarshal(&cc, ready.CommitEntries[i].Data)
			raft.raft.ApplyConfChange(&cc)
		}
	}

	for i := 0; i < len(ready.ReadStates); i++ {
		raft.callback.readStateNotice(ready.ReadStates[i].Index,
			ready.ReadStates[i].RequestCtx)
	}

	// send messages accumulation at raft.msg
	for i := 0; i < len(ready.Messages); i++ {
		raftMsg := &ready.Messages[i]
		raft.transport.Send(raftMsg)
	}
}

// service create tick per 100 milliseconds,
// when tick, call periodic and handleRaftReady()
func (raft *Raft) service(tickSize int) {
	last := time.Now()
	raft.timerStopper = utils.StartTimer(tickSize, func(time time.Time) {
		nanoseconds := time.Sub(last).Nanoseconds()
		last = time

		raft.mutex.Lock()
		defer raft.mutex.Unlock()

		var millsSinceLastPeriod = int(nanoseconds / 1000000)
		raft.raft.Periodic(millsSinceLastPeriod)
		raft.handleRaftReady()
	})
}
