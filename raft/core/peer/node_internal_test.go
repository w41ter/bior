package peer

import (
	"testing"
)

func TestNode_isPaused(t *testing.T) {
	tests := []struct {
		node   *Node
		paused bool
	}{
		// probe no paused
		{
			node: &Node{
				state:  nodeStateProbe,
				paused: false,
			},
			paused: false,
		},

		// probe paused
		{
			node: &Node{
				state:  nodeStateProbe,
				paused: true,
			},
			paused: true,
		},
		{
			node: &Node{
				state: nodeStateReplicate,
				ins: inFlights{
					start:  0,
					count:  5,
					buffer: make([]uint64, 20),
				},
			},
			paused: false,
		},
		{
			node: &Node{
				state: nodeStateReplicate,
				ins: inFlights{
					start:  0,
					count:  10,
					buffer: make([]uint64, 10),
				},
			},
			paused: true,
		},
		{
			node: &Node{
				state: nodeStateSnapshot,
			},
			paused: true,
		},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		res := test.node.IsPaused()
		if res != test.paused {
			t.Fatalf("#%d: paused wrong, want: %v, get: %v",
				i, test.paused, res)
		}
	}
}

// probe:
// 		send log entries (pause: true)
// 		unreachable (pause: false)
// 		receive append response (pause: false)
//			success: => replicate (m: n, n: n+1)
// 			failed: => probe (m: 0, n: min{rejectIdx, hintIdx})
//			ignore on rejectIdx != n-1
// 		send snapshot => snapshot (p: log.snapshot.meta.idx)
func TestNode_HandleUnreachable_probe(t *testing.T) {
	tests := []struct {
		paused, want bool
	}{
		{false, false},
		{true, false},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := Node{
			state:  nodeStateProbe,
			paused: test.paused,
		}

		node.HandleUnreachable()
		if node.paused != test.want {
			t.Fatalf("#%d: paused wrong, want: %v, get: %v",
				i, test.want, node.paused)
		}
	}
}

// snapshot:
// 		receive snapshot response
//			success: => replicate (m: p, n: p + 1) (should be probe, because
// 						when receive response, leader may generate new snapshot)
//			failed: => probe (m: 0, n: p), because core will become follower if recieve
//					reject from follower, it mean that term is old.
//		unreachable => probe (m: 0, n: p)
func TestNode_HandleUnreachable_snapshot(t *testing.T) {
	node := Node{
		state: nodeStateSnapshot,
	}

	node.HandleUnreachable()
	if node.state != nodeStateProbe {
		t.Fatalf("wrong unreachable, need be probe state")
	}
}

func TestNode_HandleUnreachable_replicate(t *testing.T) {
	tests := []struct {
		match, nextIdx uint64
		wnextIdx       uint64
	}{
		{10, 100, 11},
		{10, 11, 11},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := Node{
			state:   nodeStateReplicate,
			Matched: test.match,
			NextIdx: test.nextIdx,
		}

		node.HandleUnreachable()
		if test.wnextIdx != node.NextIdx {
			t.Fatalf("#%d: wrong nextIdx, want: %d, get: %d",
				i, test.wnextIdx, node.NextIdx)
		}
	}
}

func TestNode_handleSnapshot(t *testing.T) {
	node := Node{
		state: nodeStateSnapshot,
	}
	node.HandleSnapshot(false, 5)
	if node.Matched != 5 || node.NextIdx != 6 {
		t.Fatalf("wrong handle snapshot")
	}
}

func TestNode_handleAppendEntries_probe(t *testing.T) {
	tests := []struct {
		nextIdx            uint64
		reject             bool
		rejectIdx, hintIdx uint64
		wstate             nodeState
		wmatched           uint64
		wnextidx           uint64
		wresult            bool
		wpaused            bool
	}{
		/* success */
		{5, false, 4, 5, nodeStateReplicate, 5, 6, true, false},
		/* reject decr 1 */
		{5, true, 4, 4, nodeStateProbe, 0, 4, false, false},
		/* reject set hint */
		{5, true, 4, 2, nodeStateProbe, 0, 3, false, false},
		/* reject discard */
		{5, true, 3, 3, nodeStateProbe, 0, 5, false, true},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := Node{
			state:   nodeStateProbe,
			paused:  true,
			Matched: 0,
			NextIdx: test.nextIdx,
		}
		res := node.HandleAppendEntries(test.reject, test.rejectIdx, test.hintIdx)
		if res != test.wresult {
			t.Fatalf("#%d: result want: %v, get: %v", i, test.wresult, res)
		}
		if test.wstate == nodeStateProbe && node.paused != test.wpaused {
			t.Fatalf("#%d paused want: %v, get: %v",
				i, test.wpaused, node.paused)
		}
		if node.state != test.wstate {
			t.Fatalf("#%d state want: %v, get: %v", i, test.wstate, node.state)
		}
		if node.Matched != test.wmatched {
			t.Fatalf("#%d matched want: %v, get: %v",
				i, test.wmatched, node.Matched)
		}
		if node.NextIdx != test.wnextidx {
			t.Fatalf("#%d next idx want: %v, get: %v",
				i, test.wnextidx, test.wmatched)
		}
	}
}

func TestNode_handleAppendEntries_replicate(t *testing.T) {
	tests := []struct {
		match              uint64
		reject             bool
		rejectIdx, hintIdx uint64
		wstate             nodeState
		wmatch, wnext      uint64
	}{
		{5, false, 10, 10, nodeStateReplicate, 10, 0},
		{10, false, 5, 5, nodeStateReplicate, 10, 0},
		{5, true, 10, 6, nodeStateProbe, 7, 5},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := Node{
			state:   nodeStateReplicate,
			Matched: test.match,
		}
		node.HandleAppendEntries(test.reject, test.rejectIdx, test.hintIdx)
		if node.state != test.wstate {
			t.Fatalf("#%d: want state: %v, get: %v", i, test.wstate, node.state)
		}
	}
}
