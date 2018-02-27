package raft

import (
	"sync"
	"time"

	"github.com/thinkermao/bior/raft/core"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/pd"
)

// Application is interface for state machine.
type Application interface {
	ApplyEntry(entry *raftpd.Entry)
	ReadStateNotice(idx uint64, bytes []byte)
	ApplySnapshot(snapshot *raftpd.Snapshot)
	ReadSnapshot() *raftpd.Snapshot
}

// Raft is a implements of raft consensus algorithm,
// with log storage and periodic timer. Raft is thread-safty.
type Raft struct {
	mutex sync.Mutex

	id uint64

	raft core.Raft
	wal  *logStorage

	timer     *utils.Timer
	callback  Application
	transport Transporter
}

// MakeRaft return a instance of Raft.
func MakeRaft(
	id uint64,
	nodes []uint64,
	electionTimeout, heartbeatTimeout, tickSize int,
	walDir string,
	application Application,
	transport Transporter) (*Raft, error) {
	raft := &Raft{id: id}
	raft.callback = application
	raft.transport = transport

	config := conf.Config{
		ID:            id,
		Vote:          conf.InvalidID,
		Term:          conf.InvalidTerm,
		ElectionTick:  electionTimeout,
		HeartbeatTick: heartbeatTimeout,
		Nodes:         nodes,
		Entries:       nil,
	}

	raft.raft = core.MakeRaft(&config, raft)

	w, err := CreateLogStorage(walDir, conf.InvalidIndex+1)
	if err != nil {
		return nil, err
	}
	raft.wal = w

	raft.service(tickSize)

	return raft, nil
}

// RebuildRaft rebuild a instance of Raft.
func RebuildRaft(
	id uint64,
	logSequenceNumber uint64,
	nodes []uint64,
	electionTimeout, heartbeatTimeout, tickSize int,
	walDir string,
	application Application,
	transport Transporter) (*Raft, error) {

	ls, entries, state, err := RestoreLogStorage(walDir, logSequenceNumber)
	if err != nil {
		return nil, err
	}

	raft := &Raft{id: id}
	raft.callback = application
	raft.transport = transport
	config := conf.Config{
		ID:            id,
		Vote:          state.Vote,
		Term:          state.Term,
		ElectionTick:  electionTimeout,
		HeartbeatTick: heartbeatTimeout,
		Nodes:         nodes,
		Entries:       entries,
	}
	raft.raft = core.MakeRaft(&config, raft)
	raft.wal = ls

	raft.service(tickSize)

	return raft, nil
}

// GetState return the state of raft.
func (raft *Raft) GetState() (uint64, bool) {
	return raft.raft.ReadStatus()
}

// Kill is the only one global method no need mutex.
func (raft *Raft) Kill() {
	raft.timer.Stop()
	raft.wal.close()
}

// Read operate not sync disk
func (raft *Raft) Read(bytes []byte) bool {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	return raft.raft.Read(bytes)
}

// Write write operate will sync disk.
func (raft *Raft) Write(bytes []byte) (uint64, uint64, bool) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	return raft.raft.Propose(bytes)
}

// Compact notice
func (raft *Raft) Compact(snapshot *raftpd.Snapshot) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	raft.raft.ApplySnapshot(&snapshot.Metadata)
}

func (raft *Raft) ready() (rd core.Ready) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	rd = raft.raft.Ready()
	return
}

func (raft *Raft) handleRaftReady() {
	ready := raft.ready()
	// FIXME: 在save之前可以先处理 readStateNotice
	raft.wal.save(ready.HS, ready.Entries)
	raft.wal.sync()

	for i := 0; i < len(ready.CommitEntries); i++ {
		// FIXME: 是否有必要将更改配置信息应用
		raft.callback.ApplyEntry(&ready.CommitEntries[i])
	}

	raft.mutex.Lock()
	for i := 0; i < len(ready.CommitEntries); i++ {
		if ready.CommitEntries[i].Type == raftpd.EntryConfChange {
			cc := raftpd.ConfChange{}
			pd.MustUnmarshal(&cc, ready.CommitEntries[i].Data)
			raft.raft.ApplyConfChange(&cc)
		}
	}
	raft.mutex.Unlock()

	for i := 0; i < len(ready.ReadStates); i++ {
		raft.callback.ReadStateNotice(ready.ReadStates[i].Index,
			ready.ReadStates[i].RequestCtx)
	}

	// send messages accumulation at raft.msg
	for i := 0; i < len(ready.Messages); i++ {
		raftMsg := &ready.Messages[i]
		if err := raft.transport.Send(raftMsg.To, raftMsg); err != nil {
			raft.Unreachable(raftMsg.To)
		}
	}
}

// service create tick per 100 milliseconds,
// when tick, call periodic and handleRaftReady()
func (raft *Raft) service(tickSize int) {
	last := time.Now()
	raft.timer = utils.StartTimer(tickSize, func(time time.Time) {
		// FIXME: Adjust time, because lock cost.
		nanoseconds := time.Sub(last).Nanoseconds()
		last = time

		var millsSinceLastPeriod = int(nanoseconds / 1000000)
		raft.periodic(millsSinceLastPeriod)
		raft.handleRaftReady()
	})
}

func (raft *Raft) periodic(millsSinceLastPeriod int) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	raft.raft.Periodic(millsSinceLastPeriod)
}

func (raft *Raft) ApplySnapshot(snapshot *raftpd.Snapshot) {
	raft.callback.ApplySnapshot(snapshot)
}

func (raft *Raft) ReadSnapshot() *raftpd.Snapshot {
	return raft.callback.ReadSnapshot()
}

func (raft *Raft) Step(msg *raftpd.Message) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	raft.raft.Step(msg)
}

func (raft *Raft) Unreachable(peer uint64) {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()

	raft.raft.Unreachable(peer)
}
