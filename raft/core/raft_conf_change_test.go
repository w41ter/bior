package core

import (
	"testing"

	"github.com/thinkermao/bior/raft/proto"
)

// TestRaft_RecoverPendingConfig tests that new leader recovers its
// pendingConf flag based on uncommitted entries.
func TestRaft_RecoverPendingConfig(t *testing.T) {
	tests := []struct {
		tp       raftpd.EntryType
		wpending bool
	}{
		{raftpd.EntryNormal, false},
		{raftpd.EntryConfChange, true},
	}
	for i, test := range tests {
		r := makeTestRaft(1, []uint64{1, 2}, 10, 1, nil, nil)
		r.log.Append([]raftpd.Entry{{Index: 1, Term: 2, Type: test.tp}})
		r.becomeCandidate()
		r.becomeLeader()

		if r.pendingConf != test.wpending {
			t.Fatalf("#%d pendingConf want: %d, get: %d",
				i, test.wpending, r.pendingConf)
		}
	}
}

// TestRaft_RecoverDoublePendingConfig tests that new leader
// will panic if there exist two uncommitted config entries.
func TestRaft_RecoverDoublePendingConfig(t *testing.T) {
	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("expect panic, but nothing happens")
			}
		}()
		r := makeTestRaft(1, []uint64{1, 2}, 10, 1, nil, nil)
		r.log.Append([]raftpd.Entry{
			{Index: 1, Term: 1, Type: raftpd.EntryConfChange},
			{Index: 2, Term: 1, Type: raftpd.EntryConfChange},
		})
		r.becomeCandidate()
		r.becomeLeader()
	}()
}

// TestRaft_AddNode tests that addNode could
// update pendingConf and nodes correctly.
func TestRaft_AddNode(t *testing.T) {
	r := makeTestRaft(1, []uint64{1}, 10, 1, nil, nil)
	r.pendingConf = true
	r.addNode(2)

	if r.pendingConf {
		t.Fatalf("pending conf want: false, get: true")
	}

	if len(r.nodes) != 1 || r.nodes[0].ID != 2 {
		t.Fatalf("node add failed")
	}
}

// TestRemoveNode tests that removeNode could update
// pendingConf, nodes and removed list correctly.
func TestRaft_RemoveNode(t *testing.T) {
	r := makeTestRaft(1, []uint64{1, 2}, 10, 1, nil, nil)
	r.pendingConf = true
	r.removeNode(2)

	if r.pendingConf {
		t.Fatalf("pending conf want: false, get: true")
	}

	if len(r.nodes) != 0 {
		t.Fatalf("node remove failed")
	}
}
