package raft

import (
	"encoding/gob"

	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils/pd"
	"github.com/thinkermao/wal-go"
)

type recordType int

const (
	recordEntry recordType = iota
	recordState
)

type record struct {
	Type recordType
	Data []byte
}

func (r *record) Reset() { *r = record{} }

func init() {
	gob.Register(record{})
}

type logStorage struct {
	wal *wal.Wal
}

func CreateLogStorage(walDir string, index uint64) (*logStorage, error) {
	w, err := wal.Create(walDir, index)
	if err != nil {
		return nil, err
	}

	return &logStorage{wal: w}, nil
}

func RestoreLogStorage(walDir string, index uint64) (
	ls *logStorage, entries []raftpd.Entry, HS raftpd.HardState, err error) {

	entries = []raftpd.Entry{}

	recordReader := func(index uint64, data []byte) error {
		var record record
		pd.MustUnmarshal(&record, data)

		var entry raftpd.Entry
		var state raftpd.HardState
		switch record.Type {
		case recordEntry:
			if err := pd.Unmarshal(&entry, record.Data); err != nil {
				return err
			}
			/* truncate and append */
			idx := 0
			for i := len(entries) - 1; i >= 0; i-- {
				if entry.Index > entries[i].Index {
					idx = i + 1
					break
				}
			}
			entries = append(entries[:idx], entry)
			return nil
		case recordState:
			if err := pd.Unmarshal(&state, record.Data); err != nil {
				return err
			}
			/* use latest hard state */
			HS = state
			return nil
		}

		panic("wrong type of record")
	}

	var w *wal.Wal
	w, err = wal.Open(walDir, index, recordReader)
	if err != nil {
		return
	}

	ls = &logStorage{wal: w}
	return
}

func (ls *logStorage) saveState(at uint64, state *raftpd.HardState) (<-chan error, error) {
	bytes, err := pd.Marshal(state)
	if err != nil {
		return nil, err
	}

	rec := record{
		Type: recordState,
		Data: bytes,
	}
	return ls.wal.Write(at, pd.MustMarshal(&rec)), nil
}

func (ls *logStorage) saveEntry(entry *raftpd.Entry) (<-chan error, error) {
	bytes, err := pd.Marshal(entry)
	if err != nil {
		return nil, err
	}

	rec := record{
		Type: recordEntry,
		Data: bytes,
	}
	return ls.wal.Write(entry.Index, pd.MustMarshal(&rec)), nil
}

func (ls *logStorage) save(lastIndex uint64,
	state *raftpd.HardState, entries []raftpd.Entry) error {
	var errorChs []<-chan error

	for i := 0; i < len(entries); i++ {
		entry := &entries[i]
		ch, err := ls.saveEntry(entry)
		if err != nil {
			return err
		}
		errorChs = append(errorChs, ch)
	}

	if state != nil {
		ch, err := ls.saveState(lastIndex, state)
		if err != nil {
			return err
		}
		errorChs = append(errorChs, ch)
	}

	for _, ch := range errorChs {
		if err := <-ch; err != nil {
			return err
		}
	}
	return nil
}

func (ls *logStorage) sync() error {
	return <-ls.wal.Sync()
}

func (ls *logStorage) close() error {
	return ls.wal.Close()
}
