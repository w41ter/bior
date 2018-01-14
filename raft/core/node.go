package core

import (
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/log"
)

//  					  +--------------------------------------------------------+
//						  |                  send snapshot                         |
//						  |                                                        |
//				+---------+----------+                                  +----------v---------+
//			+--->       probe        |                                  |      snapshot      |
//			|   |  max inflight = 1  <----------------------------------+  max inflight = 0  |
//			|   +---------+----------+                                  +--------------------+
//			|             |            1. snapshot success
//			|             |               (next=snapshot.index + 1)
//			|             |            2. snapshot failure
//			|             |               (no change)
//			|             |            3. receives msgAppResp(rej=false&&index>lastsnap.index)
//			|             |               (match=m.index,next=match+1)
//receives msgAppResp(rej=true)
//(next=match+1)|         |
//			|             |
//			|             |
//			|             |   receives msgAppResp(rej=false&&index>match)
//			|             |   (match=m.index,next=match+1)
//			|             |
//			|             |
//			|             |
//			|   +---------v----------+
//			|   |     replicate      |
//			+---+  max inflight = n  |
//				+--------------------+
//
// Default state => probe (m: 0, n: log.lastIdx)
//
// probe:
// 		send log entries (pause: true)
// 		receive append response (pause: false)
//			success: => replicate (m: n, n: n+1)
// 			failed: => probe (m: 0, n: min{rejectIdx, hintIdx})
//			ignore on rejectIdx != n-1
// 		send snapshot => snapshot (p: log.snapshot.meta.idx)
//
// snapshot:
// 		receive snapshot response
//			success: => replicate (m: p, n: p + 1) (should be probe, because when receive response, leader may generate new snapshot)
//			failed: => probe (m: 0, n: p)
//		unreachable => probe (m: 0, n: p)
//
// replicate:
// 		send log entries (size: {infs.left, log.lastIdx-n}, n: lastIndex send)
// 		unreachable => probe (n: m + 1)
// 		receive replicate response:
//			success (m: idx)
// 			failed => probe (n: min{rejectIdx, hintIdx}) // FIXME: 什么情况会出现呢？
//
type nodeState int

const (
	nodeStateProbe nodeState = iota
	nodeStateReplicate
	nodeStateSnapshot
)

var StateString = []string{
	"Probe",
	"Replicate",
	"Snapshot",
}

func (state nodeState) String() string {
	return StateString[state]
}

func defaultNodeState() nodeState {
	return nodeStateProbe
}

type InFlights struct {
	start  uint
	count  uint
	buffer []uint64
}

func makeInFlights(cap uint) InFlights {
	return InFlights{
		start:  0,
		count:  0,
		buffer: make([]uint64, cap),
	}
}

func (i *InFlights) full() bool {
	return i.count == i.cap()
}

func (i *InFlights) cap() uint {
	return uint(len(i.buffer))
}

func (i *InFlights) mod(idx uint) uint {
	if idx >= i.cap() {
		idx -= i.cap()
	}
	return idx
}

// add adds an inFlight into inFlights
func (i *InFlights) add(inFlight uint64) {
	utils.Assert(i.full(), "cannot add into a full inFlights")

	next := i.mod(i.start + i.count)

	utils.Assert(next <= uint(len(i.buffer)), "out of range")

	i.buffer[next] = inFlight
	i.count++
}

// free_to frees the inFlights smaller or equal to the given `to` flight.
func (i *InFlights) freeTo(to uint64) {
	if i.count == 0 || to < i.buffer[i.start] {
		// out of the left side of the window
		return
	}

	for j := uint(0); j < i.count; j++ {
		idx := i.mod(i.start + j)
		if to >= i.buffer[idx] {
			continue
		}

		// found the first large inflight
		// free i inflights and set new start index
		i.count -= j
		i.start = idx
		return
	}
}

func (i *InFlights) freeFirstOne() {
	i.freeTo(i.buffer[i.start])
}

func (i *InFlights) reset() {
	i.count = 0
	i.start = 0
}

type voteState int

const (
	voteNone voteState = iota
	voteReject
	voteGranted
)

type Node interface {
	updateVoteState(bool)
	resetVoteState()
}

type node struct {
	id      uint64
	vote    voteState
	matched uint64
	nextIdx uint64

	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	state nodeState
	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	paused bool
	// pending_snapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	pendingSnapshot uint64

	// recent_active is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//recentActive bool

	// Inflights is a sliding window for the inflight messages.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo.
	ins InFlights
}

// TODO: set inFlats size
func makeNode(id, nextIdx uint64) node {
	var inFlightWindow uint = 10
	node := node{
		id:              id,
		vote:            voteNone,
		matched:         InvalidIndex,
		nextIdx:         nextIdx,
		state:           defaultNodeState(),
		paused:          false,
		pendingSnapshot: InvalidIndex,
		ins:             makeInFlights(inFlightWindow),
	}
	return node
}

func (n *node) handleUnreachable() {
	// During optimistic replication, if the remote becomes unreachable,
	// there is huge probability that a MsgApp is lost.
	if n.state == nodeStateReplicate {
		n.nextIdx = n.matched + 1
		n.becomeProbe()
		//n.state = nodeStateProbe
		//n.paused = false
	} else if n.state == nodeStateProbe {
		n.resume()
	} else { // n.state == nodeStateSnapshot
		// 2. snapshot failure
		// 	(no change)
		n.becomeProbe()
	}
}

func (n *node) becomeProbe() {
	n.paused = false
	n.state = nodeStateProbe
}

func (n *node) handleSnapshot(reject bool, index uint64) {
	switch n.state {
	case nodeStateSnapshot:
		// 1. snapshot success
		// (next=snapshot.index + 1)
		if !reject {
			n.matched = index
			n.nextIdx = index + 1
			n.becomeProbe()
		} else {
			log.Panicf("unreachable")
		}
	}
}

func (n *node) handleAppendEntries(reject bool, index uint64, rejectHint uint64) bool {
	switch n.state {
	case nodeStateSnapshot:
		// 3. receives msgAppResp(rej=false&&index>lastsnap.index)
		//	(match=m.index,next=match+1)
		// TODO: Why ?
		if !reject && index > n.pendingSnapshot {
			n.matched = index
			n.nextIdx = n.matched + 1
			n.pendingSnapshot = InvalidIndex
			n.state = nodeStateProbe
		}
		return false
	case nodeStateReplicate:
		//receives msgAppResp(rej=true)
		//	(next=match+1)
		if reject {
			n.nextIdx = n.matched + 1
			n.becomeProbe()
			return false
		} else if n.matched < index {
			n.ins.freeTo(index - n.matched)
			n.matched = index

			if n.nextIdx <= n.matched {
				n.nextIdx = n.matched + 1
			}
			return true
		}
	case nodeStateProbe:
		// receives msgAppResp(rej=false&&index>match)
		// 	(match=m.index,next=match+1)
		if !reject && index > n.matched {
			n.matched = index
			n.nextIdx = n.matched + 1
			n.becomeReplicate()
			return true
		} else {
			// the rejection must be stale if "rejected" does not match next - 1
			if n.nextIdx == 0 || n.nextIdx-1 != index {
				return false
			}
			n.nextIdx = utils.MinUint64(index, rejectHint+1)
			if n.nextIdx < 1 {
				n.nextIdx = 1
			}
			n.resume()
		}
		return false
	}
	return false
}

func (n *node) becomeReplicate() {
	n.ins.reset()
	n.state = nodeStateReplicate
}

func (n *node) sendSnapshot(idx uint64) {
	n.pendingSnapshot = idx
	n.state = nodeStateSnapshot
}

func (n *node) updateVoteState(reject bool) {
	if reject {
		n.vote = voteReject
	} else {
		n.vote = voteGranted
	}
}

func (n *node) resetVoteState() {
	n.vote = voteNone
}

func (n *node) optimisticUpdate(idx uint64) {
	n.nextIdx = idx + 1
	n.ins.add(idx)
}

func (n *node) isPaused() bool {
	switch n.state {
	case nodeStateProbe:
		return n.paused
	case nodeStateSnapshot:
		return n.ins.full()
	case nodeStateReplicate:
		return true
	default:
		panic("unreachable")
	}
}

func (n *node) resume() {
	n.paused = false
}

func (n *node) pause() {
	n.paused = true
}
