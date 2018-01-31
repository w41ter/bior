package raft

import (
	"errors"

	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils/pd"
	"github.com/thinkermao/wal-go"
)

var errEmptyEntries = errors.New("entries is empty")

type logStorage struct {
	wal *wal.Wal
}

func CreateLogStorate(walDir string, index uint64) (*logStorage, error) {
	w, err := wal.Create(walDir, index)
	if err != nil {
		return nil, err
	}

	return &logStorage{wal: w}, nil
}

func RestoreLogStorage(walDir string, index uint64) (
	ls *logStorage, entries []raftpd.Entry, HS raftpd.HardState, err error) {
	/* catch exception */
	defer func() { err = recover().(error) }()

	entries = []raftpd.Entry{}

	recordReader := func(index uint64, data []byte) {
		var entry raftpd.Entry
		var state raftpd.HardState
		if err := pd.Unmarshal(&entry, data); err == nil {
			entries = append(entries, entry)
		}
		if err := pd.Unmarshal(&state, data); err != nil {
			/* stop restore */
			panic(err)
		}
		HS = state
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
	return ls.wal.Write(at, bytes), nil
}

func (ls *logStorage) saveEntry(entry *raftpd.Entry) (<-chan error, error) {
	bytes, err := pd.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return ls.wal.Write(entry.Index, bytes), nil
}

func (ls *logStorage) save(state *raftpd.HardState, entries []raftpd.Entry) error {
	if len(entries) == 0 {
		return errEmptyEntries
	}

	errorChs := []<-chan error{}
	for i := 0; i < len(entries); i++ {
		entry := &entries[i]
		ch, err := ls.saveEntry(entry)
		if err != nil {
			return err
		}
		errorChs = append(errorChs, ch)
	}

	if state != nil {
		lastIdx := entries[len(entries)-1].Index
		ch, err := ls.saveState(lastIdx, state)
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
