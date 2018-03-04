package core

import (
	"bytes"
	"testing"

	"github.com/thinkermao/bior/raft/proto"
)

func TestRaft_BasicReadOnly(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	a := makeTestRaft(1, nodes, 10, 1, nil, nil)
	b := makeTestRaft(2, nodes, 10, 1, nil, nil)
	c := makeTestRaft(3, nodes, 10, 1, nil, nil)
	n := makeNetwork(a, b, c)

	n.startElection(1)

	// wait noop commit
	if !n.waitCommit(1) {
		t.Fatal("failed to acheive agreement")
	}

	if a.state != RoleLeader {
		t.Fatal("a not leader")
	}

	tests := []struct {
		sm        *RawNode
		proposals int
		wri       uint64
		wctx      []byte
	}{
		// notice noop entry.
		{a, 10, 11, []byte("ctx1")},
		{a, 10, 21, []byte("ctx2")},
		{b, 0, 21, []byte("ctx2")},
	}

	for i, test := range tests {
		var idx uint64
		for j := 0; j < test.proposals; j++ {
			idx, _ = n.propose(1, []byte(""))
		}
		if !n.waitCommit(idx) {
			t.Fatal("failed to acheive agreement")
		}

		n.readIndex(test.sm.id, test.wctx)

		if len(test.sm.readStates) != 1 {
			t.Fatalf("%d read states size want: %d, get: %d",
				i, 1, len(test.sm.readStates))
		}

		rs := test.sm.readStates[0]
		if rs.Index != test.wri {
			t.Fatalf("%d read state index want: %d, get: %d",
				i, test.wri, rs.Index)
		}

		if !bytes.Equal(rs.RequestCtx, test.wctx) {
			t.Fatalf("%d read state ctx not equals", i)
		}
		test.sm.readStates = test.sm.readStates[:0]
	}
}

// TestRaft_ReadOnlyForNewLeader ensures that a leader only accepts
// MsgReadIndex message when it commits at least one log entry at it term.
func TestRaft_ReadOnlyForNewLeader(t *testing.T) {
	configs := []struct {
		id, committed, applied, compactedIdx uint64
	}{
		{1, 1, 1, 0},
		{2, 2, 2, 2},
		{3, 2, 2, 2},
	}
	n := generate(3)
	for _, c := range configs {
		peer := n.peer(c.id)
		peer.log.Append([]raftpd.Entry{
			{Index: 1, Term: 1, Type: raftpd.EntryNormal},
			{Index: 2, Term: 1, Type: raftpd.EntryNormal},
		})
		peer.log.StableEntries()
		peer.log.CommitTo(c.committed)
		if c.compactedIdx != 0 {
			peer.log.CompactTo(c.compactedIdx, 1)
		}
	}

	n.peer(2).term = 1
	n.peer(3).term = 1
	p1 := n.peer(1)
	p1.term = 1

	// Drop MsgAppendRequest to forbid peer a to commit any
	// log entry at its term after it becomes leader.
	n.ignore(raftpd.MsgAppendRequest)
	n.startElection(1)

	if p1.state != RoleLeader {
		t.Fatal("a not leader")
	}

	// Ensure p1 drops read only request.
	n.readIndex(1, []byte("ctx"))
	if len(p1.readStates) != 0 {
		t.Fatalf("read states size want: %d, get: %d",
			0, len(p1.readStates))
	}
	n.recover()

	// Force p1 to commit a log entry at its term
	if !n.waitCommit(3) {
		t.Fatal("failed to acheive agreement")
	}
	idx, _ := n.propose(1, []byte(""))
	if !n.waitCommit(idx) {
		t.Fatal("failed to acheive agreement")
	}

	n.readIndex(1, []byte("ctx"))
	if len(p1.readStates) != 1 {
		t.Fatalf("read states size want: %d, get: %d",
			1, len(p1.readStates))
	}

	rs := p1.readStates[0]
	if rs.Index != 4 {
		t.Fatalf("read state index want: %d, get: %d", 4, rs.Index)
	}

	if !bytes.Equal(rs.RequestCtx, []byte("ctx")) {
		t.Fatalf("read state ctx not equals")
	}
}
