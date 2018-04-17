package core

import (
	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/read"
	"github.com/thinkermao/bior/raft/proto"
)

type NodeApplication interface {
	ApplySnapshot(snapshot *raftpd.Snapshot)
	// if snapshot is building at now, it will return nil.
	ReadSnapshot() *raftpd.Snapshot
}

type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	SS *SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	HS *raftpd.HardState

	// read_states states can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the read_state will be returned when raft receives MsgReadIndex.
	// The returned is only valid for the request that requested to read.
	ReadStates []read.ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []raftpd.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot raftpd.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommitEntries []raftpd.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap pd, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []raftpd.Message
}

type RawNode struct {
	*core
	prevHS raftpd.HardState
	prevSS SoftState

	readStates    []read.ReadState
	commitEntries []raftpd.Entry
	messages      []raftpd.Message

	application NodeApplication
}

func MakeRawNode(config *conf.Config, app NodeApplication) *RawNode {
	node := &RawNode{}

	node.core = makeCore(config, node)
	node.application = app
	node.prevSS = node.core.ReadSoftState()
	node.prevHS = node.core.ReadHardState()
	return node
}

func (node *RawNode) Unreachable(peer uint64) {
	msg := raftpd.Message{
		From:    peer,
		To:      conf.InvalidID,
		Term:    node.term,
		MsgType: raftpd.MsgUnreachable,
	}
	node.Step(&msg)
}

func (node *RawNode) Ready() Ready {
	ready := Ready{}

	ss := node.core.ReadSoftState()
	ready.SS = &ss

	hs := node.core.ReadHardState()
	if hs != node.prevHS {
		ready.HS = &hs
		node.prevHS = hs
	}

	ready.Entries = node.core.log.StableEntries()
	ready.CommitEntries = node.commitEntries
	ready.Messages = node.messages
	ready.ReadStates = node.drainReadState()

	log.Debugf("%d handle ready: [stable: %d, commit: %d, msg: %d]",
		node.id, len(ready.Entries), len(ready.CommitEntries), len(ready.Messages))

	// clear all
	node.commitEntries = make([]raftpd.Entry, 0)
	node.messages = make([]raftpd.Message, 0)

	return ready
}

func (node *RawNode) ReadStatus() (uint64, bool) {
	ss := node.core.ReadSoftState()
	hs := node.core.ReadHardState()

	return hs.Term, ss.State.IsLeader()
}

func (node *RawNode) send(msg *raftpd.Message) {
	node.messages = append(node.messages, *msg)
}

func (node *RawNode) saveReadState(readState *read.ReadState) {
	node.readStates = append(node.readStates, *readState)
}

func (node *RawNode) applyEntry(entry *raftpd.Entry) {
	node.commitEntries = append(node.commitEntries, *entry)
}

func (node *RawNode) applySnapshot(snapshot *raftpd.Snapshot) {
	node.application.ApplySnapshot(snapshot)
}

func (node *RawNode) readSnapshot() *raftpd.Snapshot {
	return node.application.ReadSnapshot()
}

func (node *RawNode) drainReadState() []read.ReadState {
	var readStates []read.ReadState
	lastApplied := node.prevHS.Commit
	if len(node.commitEntries) > 0 {
		waitCommitIdx := node.commitEntries[len(node.commitEntries)-1].Index
		if lastApplied < waitCommitIdx {
			lastApplied = waitCommitIdx
		}
	}
	i := 0
	for ; i < len(node.readStates); i++ {
		if node.readStates[i].Index > lastApplied {
			break
		}
	}
	//save and drain read states.
	readStates = make([]read.ReadState, i)
	copy(readStates, node.readStates)
	node.readStates = node.readStates[i:]
	return readStates
}
