package core

import (
	"testing"
)

func TestInFlights_cap(t *testing.T) {
	tests := []struct {
		cap, wcap uint
	}{
		{10, 10},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := makeInFlights(test.cap)
		if inf.cap() != test.wcap {
			t.Errorf("#%d: cap wrong get: %d, want: %d",
				i, inf.cap(), test.wcap)
		}
	}
}

func TestInFlights_full(t *testing.T) {
	tests := []struct {
		count uint
		w     bool
	}{
		{1, false},
		{10, true},
	}
	inf := makeInFlights(10)
	for i := 0; i < len(tests); i++ {
		inf.count = tests[i].count
		if inf.full() != tests[i].w {
			t.Errorf("#%d: full wrong, want: %v, get: %v",
				i, tests[i].w, inf.full())
		}
	}
}

func TestInFlights_freeTo(t *testing.T) {
	tests := []struct {
		start, count   uint
		buffer         []uint64
		to             uint64
		wstart, wcount uint
	}{
		// stale
		{0, 3, []uint64{1, 2, 3, 4}, 0, 0, 3},
		// free
		{0, 3, []uint64{1, 2, 3, 4}, 1, 1, 2},
		// free all
		{0, 3, []uint64{1, 2, 3, 4}, 3, 0, 0},
		// great
		{0, 3, []uint64{1, 2, 3, 4}, 4, 0, 0},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := InFlights{
			start:  test.start,
			count:  test.count,
			buffer: test.buffer,
		}
		inf.freeTo(test.to)
		if inf.start != test.wstart {
			t.Errorf("#%d: wrong freeTo, want start: %d, get: %d",
				i, test.wstart, inf.start)
		}
		if inf.count != test.wcount {
			t.Errorf("#%d: wrong freeTo, want count: %d, get: %d",
				i, test.wcount, inf.count)
		}
	}
}

func TestInFlights_reset(t *testing.T) {
	inf := makeInFlights(10)
	inf.count = 10
	inf.start = 5

	inf.reset()
	if inf.count != 0 || inf.start != 0 {
		t.Error("wrong reset")
	}
}

func TestInFlights_mod(t *testing.T) {
	var cap uint = 10
	tests := []struct {
		v, m uint
	}{
		{1, 1},
		{10, 0},
		{20, 0},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := makeInFlights(cap)
		get := inf.mod(test.v)
		if get != test.m {
			t.Errorf("#%d: mod wrong, want: %d, get: %d",
				i, test.m, get)
		}
	}
}

// func TestInFlights_add(t *testing.T) {

// }

func TestNode_isPaused(t *testing.T) {
	tests := []struct {
		node   *node
		paused bool
	}{
		// probe no paused
		{
			node: &node{
				state:  nodeStateProbe,
				paused: false,
			},
			paused: false,
		},

		// probe paused
		{
			node: &node{
				state:  nodeStateProbe,
				paused: true,
			},
			paused: true,
		},
		{
			node: &node{
				state: nodeStateReplicate,
				ins: InFlights{
					start:  0,
					count:  5,
					buffer: make([]uint64, 20),
				},
			},
			paused: false,
		},
		{
			node: &node{
				state: nodeStateReplicate,
				ins: InFlights{
					start:  0,
					count:  10,
					buffer: make([]uint64, 10),
				},
			},
			paused: true,
		},
		{
			node: &node{
				state: nodeStateSnapshot,
			},
			paused: true,
		},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		res := test.node.isPaused()
		if res != test.paused {
			t.Errorf("#%d: paused wrong, want: %v, get: %v",
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
func TestNode_handleUnreachable_probe(t *testing.T) {
	tests := []struct {
		paused, want bool
	}{
		{false, false},
		{true, false},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := node{
			state:  nodeStateProbe,
			paused: test.paused,
		}

		node.handleUnreachable()
		if node.paused != test.want {
			t.Errorf("#%d: paused wrong, want: %v, get: %v",
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
func TestNode_handleUnreachable_snapshot(t *testing.T) {
	node := node{
		state: nodeStateSnapshot,
	}

	node.handleUnreachable()
	if node.state != nodeStateProbe {
		t.Errorf("wrong unreachable, need be probe state")
	}
}

func TestNode_handleUnreachable_replicate(t *testing.T) {
	tests := []struct {
		match, nextIdx uint64
		wnextIdx       uint64
	}{
		{10, 100, 11},
		{10, 11, 11},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := node{
			state:   nodeStateReplicate,
			matched: test.match,
			nextIdx: test.nextIdx,
		}

		node.handleUnreachable()
		if test.wnextIdx != node.nextIdx {
			t.Errorf("#%d: wrong nextIdx, want: %d, get: %d",
				i, test.wnextIdx, node.nextIdx)
		}
	}
}

func TestNode_handleSnapshot(t *testing.T) {
	node := node{
		state: nodeStateSnapshot,
	}
	node.handleSnapshot(false, 5)
	if node.matched != 5 || node.nextIdx != 6 {
		t.Errorf("wrong handle snapshot")
	}
}

func TestNode_handleAppendEntries_probe(t *testing.T) {
	tests := []struct {
		nextIdx            uint64
		wstate             nodeState
		wmatched           uint64
		wnextidx           uint64
		reject             bool
		rejectIdx, hintIdx uint64
		wresult            bool
		wpaused            bool
	}{
		/* success */
		{5, nodeStateReplicate, 5, 6, false, 5, 0, true, false},
		/* reject decr 1 */
		{5, nodeStateProbe, 0, 4, true, 4, 4, false, false},
		/* reject set hint */
		{5, nodeStateProbe, 0, 3, true, 4, 2, false, false},
		/* reject discard */
		{5, nodeStateProbe, 0, 5, true, 3, 3, false, true},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		node := node{
			state:   nodeStateProbe,
			paused:  true,
			matched: 0,
			nextIdx: test.nextIdx,
		}
		res := node.handleAppendEntries(test.reject, test.rejectIdx, test.hintIdx)
		if res != test.wresult {
			t.Errorf("#%d: result want: %v, get: %v", i, test.wresult, res)
		}
		if test.wstate == nodeStateProbe && node.paused != test.wpaused {
			t.Errorf("#%d paused want: %v, get: %v",
				i, test.wpaused, node.paused)
		}
		if node.state != test.wstate {
			t.Errorf("#%d state want: %v, get: %v", i, test.wstate, node.state)
		}
		if node.matched != test.wmatched {
			t.Errorf("#%d matched want: %v, get: %v",
				i, test.wmatched, node.matched)
		}
		if node.nextIdx != test.wnextidx {
			t.Errorf("#%d next idx want: %v, get: %v",
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
		node := node{
			state:   nodeStateReplicate,
			matched: test.match,
		}
		node.handleAppendEntries(test.reject, test.rejectIdx, test.hintIdx)
		if node.state != test.wstate {
			t.Errorf("#%d: want state: %v, get: %v", i, test.wstate, node.state)
		}
	}
}
