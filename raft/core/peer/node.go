package peer

import (
	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
)

// Node maintains the same information as other nodes in the raft group.
type Node struct {
	belongID uint64

	// node id
	ID uint64

	// detected status
	Vote VoteState

	// known to the maximum location
	Matched uint64

	// next entry index to send
	NextIdx uint64

	// When in nodeStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual status of the follower.
	//
	// When in nodeStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in nodeStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	state nodeState

	// paused is used in nodeStateProbe.
	// When paused is true, raft should pause sending replication message to this peer.
	paused bool

	// pendingSnapshot is used in nodeStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this status will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	pendingSnapshot uint64

	// inflights is a sliding window for the inflight messages.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo.
	ins inFlights
}

// MakeNode create instance for remote peer.
// TODO: set inFlats size
func MakeNode(belong, id, nextIdx uint64) *Node {
	var inFlightWindow uint = 10
	node := &Node{
		belongID:        belong,
		ID:              id,
		Vote:            VoteNone,
		Matched:         conf.InvalidIndex,
		NextIdx:         nextIdx,
		state:           defaultNodeState(),
		paused:          false,
		pendingSnapshot: conf.InvalidIndex,
		ins:             makeInFlights(inFlightWindow),
	}
	return node
}

// HandleUnreachable trigger unreachable event.
func (n *Node) HandleUnreachable() {
	switch n.state {
	case nodeStateReplicate:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		n.NextIdx = n.Matched + 1
		n.becomeProbe()
	case nodeStateProbe:
		n.resume()
	case nodeStateSnapshot:
		n.becomeProbe()
		n.NextIdx = n.pendingSnapshot
	}
}

// HandleSnapshot trigger receive snapshot response event.
func (n *Node) HandleSnapshot(reject bool, index uint64) {
	switch n.state {
	case nodeStateSnapshot:
		// TODO: handle staled msg
		if !reject {
			n.Matched = index
			n.NextIdx = index + 1
			n.becomeProbe()
		} else {
			log.Panicf("unreachable")
		}
	}
}

// HandleAppendEntries trigger append response event.
func (n *Node) HandleAppendEntries(reject bool, index uint64, hintIdx uint64) bool {
	switch n.state {
	case nodeStateReplicate:
		if reject {
			n.NextIdx = n.Matched + 1
			n.becomeProbe()
			return false
		} else if n.Matched < index {
			n.ins.freeTo(hintIdx)
			n.Matched = hintIdx

			if n.NextIdx <= n.Matched {
				n.NextIdx = n.Matched + 1
			}
			return true
		}
	case nodeStateProbe:
		if !reject {
			// stale
			if index < n.Matched {
				log.Debugf("%d node: %d [next: %d] ignore staled append response: %d",
					n.belongID, n.ID, n.NextIdx, index)
				return false
			}

			n.Matched = hintIdx
			n.NextIdx = n.Matched + 1
			n.becomeReplicate()
			return true
		}

		// the rejection must be stale if "rejected" does not match next - 1
		if n.NextIdx == 0 || n.NextIdx-1 != index {
			log.Debugf("%d node: %d [next: %d] ignore staled rejection: %d",
				n.belongID, n.ID, n.NextIdx, index)
			return false
		}
		n.NextIdx = utils.MinUint64(index, hintIdx+1)
		if n.NextIdx <= conf.InvalidIndex {
			n.NextIdx = conf.InvalidIndex + 1
		}
		log.Debugf("%d node: %d update next index: %d",
			n.belongID, n.ID, n.NextIdx)

		n.resume()
		return false
	}
	return false
}

// SendSnapshot translate state to nodeStateSnapshot,
// and set pendingSnapshot to idx.
func (n *Node) SendSnapshot(idx uint64) {
	log.Debugf("%d node: %d from %v => %v [pd snapshot: %d]",
		n.belongID, n.ID, n.state, nodeStateSnapshot, idx)

	n.pendingSnapshot = idx
	n.state = nodeStateSnapshot
}

// UpdateVoteState set vote by reject, if true vote
// set to voteReject, otherwise set to voteGranted.
func (n *Node) UpdateVoteState(reject bool) {
	if reject {
		n.Vote = VoteReject
	} else {
		n.Vote = VoteGranted
	}
}

// ResetVoteState set vote to voteNone.
func (n *Node) ResetVoteState() {
	n.Vote = VoteNone
}

// optimisticUpdate records count of entries will
// be send, and increase NextIdx to idx + 1.
func (n *Node) optimisticUpdate(idx uint64) {
	n.NextIdx = idx + 1
	n.ins.add(idx)
}

// SendEntries change fields by entries.
func (n *Node) SendEntries(entries []raftpd.Entry) {
	switch n.state {
	case nodeStateProbe:
		// FIXME: only send little message
		n.pause()
	case nodeStateReplicate:
		if len(entries) != 0 {
			// optimistically increase the next when in nodeReplicate
			lastIndex := entries[len(entries)-1].Index
			n.optimisticUpdate(lastIndex)
		}
	default:
		log.Fatalf("%x is sending append in unhandled state %s", n.ID, n.state)
	}
}

// IsPaused test whether reached pause status.
func (n *Node) IsPaused() bool {
	switch n.state {
	case nodeStateProbe:
		// FIXME: it will blocking node if remote lose response.
		return n.paused
	case nodeStateReplicate:
		return n.ins.full()
	case nodeStateSnapshot:
		return true
	default:
		panic("unreachable")
	}
}

// ToProbe transfer status to probe, and reset fileds.
func (n *Node) ToProbe(nextIdx uint64) {
	n.Matched = conf.InvalidIndex
	n.NextIdx = nextIdx
	n.becomeProbe()
}

func (n *Node) resume() {
	n.paused = false
}

func (n *Node) pause() {
	n.paused = true
}

func (n *Node) becomeProbe() {
	origin := n.state
	n.paused = false
	n.state = nodeStateProbe

	log.Debugf("%d node: %d from %v => %v", n.belongID, n.ID, origin, n.state)
}

func (n *Node) becomeReplicate() {
	origin := n.state
	n.ins.reset()
	n.state = nodeStateReplicate

	log.Debugf("%d node: %d from %v => %v", n.belongID, n.ID, origin, n.state)
}
