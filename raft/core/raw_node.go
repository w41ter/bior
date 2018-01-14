package core

import (
	"github.com/thinkermao/bior/raft/proto"
)

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
	ReadStates []ReadState

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
	raft   *core
	prevHS raftpd.HardState
	prevSS SoftState

	readStates    []ReadState
	commitEntries []raftpd.Entry
	messages      []raftpd.Message

	application NodeApplication
}

func MakeRawNode(config *Config, app NodeApplication) *RawNode {
	node := &RawNode{}

	node.raft = makeCore(config, node)
	node.application = app
	node.prevSS = node.raft.ReadSoftState()
	node.prevHS = node.raft.ReadHardState()
	return node
}

func (node *RawNode) Periodic(millsSinceLastPeriod int) {
	node.raft.Periodic(millsSinceLastPeriod)
}

func (node *RawNode) Propose(bytes []byte) (uint64, uint64, bool) {
	return node.raft.Propose(bytes)
}

func (node *RawNode) ProposeConfChange(cc *raftpd.ConfChange) (uint64, uint64, bool) {
	return node.raft.ProposeConfChange(cc)
}

func (node *RawNode) ApplyConfChange(cc *raftpd.ConfChange) raftpd.ConfState {
	return node.raft.ApplyConfChange(cc)
}

func (node *RawNode) CompactTo(metadata *raftpd.SnapshotMetadata) {
	node.raft.CompactTo(metadata)
}

func (node *RawNode) Read(context []byte) {
	node.raft.Read(context)
}

func (node *RawNode) Step(msg *raftpd.Message) {
	// TODO: validate
	node.raft.Step(msg)
}

func (node *RawNode) Ready() Ready {
	ready := Ready{}

	ss := node.raft.ReadSoftState()
	if ss != node.prevSS {
		ready.SS = &ss
		node.prevSS = ss
	}

	hs := node.raft.ReadHardState()
	if hs != node.prevHS {
		ready.HS = &hs
		node.prevHS = hs
	}

	ready.Entries = node.raft.log.StableEntries()
	ready.CommitEntries = node.commitEntries
	ready.Messages = node.messages
	ready.ReadStates = node.readStates

	// clear all
	node.commitEntries = make([]raftpd.Entry, 0)
	node.messages = make([]raftpd.Message, 0)
	node.readStates = make([]ReadState, 0)

	return ready
}

func (node *RawNode) send(msg *raftpd.Message) {
	node.messages = append(node.messages, *msg)
}

func (node *RawNode) saveReadState(readStae *ReadState) {
	node.readStates = append(node.readStates, *readStae)
}

func (node *RawNode) applyEntry(entry *raftpd.Entry) {
	node.commitEntries = append(node.commitEntries, *entry)
}

func (node *RawNode) applySnapshot(snapshot *raftpd.Snapshot) {
	node.application.applySnapshot(snapshot)
}

func (node *RawNode) readSnapshot() *raftpd.Snapshot {
	return node.application.readSnapshot()
}

type NodeApplication interface {
	applySnapshot(snapshot *raftpd.Snapshot)
	readSnapshot() *raftpd.Snapshot
}
