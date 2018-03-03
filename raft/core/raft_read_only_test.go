package core

import (
	"bytes"
	"testing"
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
