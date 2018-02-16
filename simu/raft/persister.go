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

// //
// func (ps *Persister) Copy() *Persister {
// 	ps.mu.Lock()
// 	defer ps.mu.Unlock()
// 	np := MakePersister()
// 	np.snapshot = ps.snapshot
// 	return np
// }

// SaveSnapshot save Raft snapshot
func (ps *Persister) SaveSnapshot(snapshot *raftpd.Snapshot) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = pd.MustMarshal(snapshot)
}

// ReadSnapshot read Raft snapshot saved before.
func (ps *Persister) ReadSnapshot() *raftpd.Snapshot {
	ps.mu.Lock()
	defer ps.mu.Unlock()
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
