package raftpd

import (
	"encoding/gob"
	"fmt"
)

type HardState struct {
	Vote   uint64
	Term   uint64
	Commit uint64
}

func (e *HardState) Reset() { *e = HardState{} }

func (e HardState) String() string {
	return fmt.Sprintf("raftpd.HardState{vote: %d, term: %d, commit: %d}",
		e.Vote, e.Term, e.Commit)
}

type EntryType int

const (
	EntryNormal EntryType = iota
	EntryBroadcast
	EntryConfChange
)

var entryTypeStr = []string{
	"Normal",
	"Broadcast",
	"ConfChange",
}

func (t EntryType) String() string {
	return entryTypeStr[t]
}

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

func (e *Entry) Reset() { *e = Entry{} }

func (e Entry) String() string {
	return fmt.Sprintf("raftpd.Entry{idx: %d, term: %d, type: %v, data: %v}",
		e.Index, e.Term, e.Type, e.Data)
}

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

func init() {
	gob.Register(Entry{})
	gob.Register(SnapshotMetadata{})
	gob.Register(Snapshot{})
	gob.Register(HardState{})
}
