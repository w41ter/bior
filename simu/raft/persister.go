package raft

import (
	"sync"

	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils/pd"
)

// Persister support for Raft to save persistent Raft snapshots.
type Persister struct {
	mu       sync.Mutex
	snapshot []byte
}

// MakePersister return instance of Persister.
func MakePersister() *Persister {
	return &Persister{}
}

// SaveSnapshot save Raft snapshot
func (ps *Persister) SaveSnapshot(snapshot *raftpd.Snapshot) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = pd.MustMarshal(snapshot)
}

// ReadSnapshot read Raft snapshot saved before, otherwise return nil.
func (ps *Persister) ReadSnapshot() *raftpd.Snapshot {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.snapshot == nil {
		return nil
	}

	snapshot := &raftpd.Snapshot{}
	pd.MustUnmarshal(snapshot, ps.snapshot)
	return snapshot
}

// SnapshotSize return size of snapshot saved last.
func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
