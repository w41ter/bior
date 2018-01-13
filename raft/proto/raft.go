package raftpd

import "encoding/gob"

type HardState struct {
	Vote   uint64
	Term   uint64
	Commit uint64
}

func (e *HardState) Reset() { *e = HardState{} }

type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

type Entry struct {
	Index uint64
	Term  uint64
	Type EntryType
	Data  []byte
}

func (e *Entry) Reset() { *e = Entry{} }

type SnapshotMetadata struct {
	Index uint64
	Term  uint64
}

func (e *SnapshotMetadata) Reset() { *e = SnapshotMetadata{} }

type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

func (s *Snapshot) Reset() { *s = Snapshot{} }

type RequestSnapshot struct {
	Term     uint64
	LeaderId uint64
	Snapshot *Snapshot
}

func (m *RequestSnapshot) Reset() { *m = RequestSnapshot{} }

type RequestAppend struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []Entry
}

func (m *RequestAppend) Reset() { *m = RequestAppend{} }

type RequestVote struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (m *RequestVote) Reset() { *m = RequestVote{} }

type Response struct {
	Term      uint64
	Index     uint64
	HintIndex uint64
	Success   bool
}

func (m *Response) Reset() { *m = Response{} }

type RequestHeartbeat struct {
	Term     uint64
	LeaderId uint64
}

func (m *RequestHeartbeat) Reset() { *m = RequestHeartbeat{} }

func (m *RequestReadIndex) Reset() { *m = RequestReadIndex{} }

type RequestReadIndex struct {
	Term     uint64
	From     uint64
	To       uint64
	LogIndex uint64
	LogTerm  uint64
}

func init() {
	gob.Register(Entry{})
	gob.Register(SnapshotMetadata{})
	gob.Register(Snapshot{})
	gob.Register(RequestAppend{})
	gob.Register(RequestSnapshot{})
	gob.Register(RequestVote{})
	gob.Register(Response{})
}
