package raftpd

import "encoding/gob"

type MessageType int

// Message from local:
// - Unreachable	infer whether remote is online. such as generate at
// 	heartbeat checkout failed by local transport.
//
// Message from leader:
// - Append request
// - Snapshot request
// - Heartbeat request
// - ReadIndex response
//
// Message from follower:
// - Append response
// - Snapshot response
// - ReadIndex request
//
// Message from all server:
// - PreVote response
// - Vote response
//
// Message from candidate:
// - PreVote request
// - Vote request
//
// After change term, there are eventually receiver.
// Message leader will receive:
// - Unreachable
// - Append response
// - Snapshot response
// - Heartbeat response
// - ReadIndex request
// - PreVote request
// - Vote request
//
// Message follower will receive:
// - Append request
// - Snapshot request
// - Heartbeat response
// - ReadIndex response
// - PreVote request
// - Vote request
//
// Message candidate will receive:
// - PreVote response
// - Vote response
// - Append request
// - Snapshot request
// - Heartbeat response
// - ReadIndex response
//
// may be confused by append, snapshot request, because it will lose
// at campaign, so candidate has same term with leader.
//
const (
	MsgAppendRequest MessageType = iota
	MsgAppendResponse
	MsgPreVoteRequest
	MsgPreVoteResponse
	MsgVoteRequest
	MsgVoteResponse
	MsgSnapshotRequest
	MsgSnapshotResponse
	MsgHeartbeatRequest
	MsgHeartbeatResponse
	MsgReadIndexRequest
	MsgReadIndexResponse
	MsgUnreachable

	MsgConfChange
)

type Message struct {
	MsgType           MessageType
	From, To          uint64
	Index, Term       uint64
	LogIndex, LogTerm uint64
	Reject            bool
	RejectHint        uint64
	Entries           []Entry
	Snapshot          *Snapshot
	Context           []byte
}

func (c *Message) Reset() { *c = Message{} }

var MessageTypeString = []string{
	"Append request",
	"Append response",
	"PreVote Request",
	"PreVote Response",
	"Vote Request",
	"Vote Response",
	"Snapshot request",
	"Snapshot response",
	"Heartbeat request",
	"Heartbeat response",
	"ReadIndex request",
	"ReadIndex response",
	"Unreachable",
	"Configuration change",
}

func (tp MessageType) String() string {
	return MessageTypeString[tp]
}

type ConfState struct {
	Nodes []uint64
}

func (c *ConfState) Reset() { *c = ConfState{} }

type ConfChangeType int

const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
	ConfChangeLearnerNode
)

type ConfChange struct {
	ID         uint64
	ChangeType ConfChangeType
	NodeID     uint64
}

func (c *ConfChange) Reset() { *c = ConfChange{} }

var ConfChangeString = []string{
	"Config: Add node",
	"Config: Remove node",
}

func (t ConfChangeType) String() string {
	return ConfChangeString[t]
}

func init() {
	gob.Register(Message{})
	gob.Register(ConfChange{})
}