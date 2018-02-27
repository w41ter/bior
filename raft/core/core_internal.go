package core

import (
	"math/rand"

	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
)

func quorum(len int) int {
	return len/2 + 1
}

// send send message to remote peers.
func (c *core) send(msg *raftpd.Message) {
	if msg.MsgType == raftpd.MsgPreVoteRequest {
		/* request pre vote: next term */
		msg.Term = c.term + 1
	} else {
		msg.Term = c.term
	}

	msg.From = c.id
	c.callback.send(msg)
}

func (c *core) resetRandomizedElectionTimeout() {
	previousTimeout := c.randomizedElectionTick
	c.randomizedElectionTick =
		c.electionTick + rand.Intn(c.electionTick)

	log.Debugf("%d reset randomized election timeout [%d => %d]",
		c.id, previousTimeout, c.randomizedElectionTick)
}

func (c *core) resetLease() {
	c.timeElapsed = 0
	c.resetRandomizedElectionTimeout()
}

func (c *core) reset(term uint64) {
	if c.term != term {
		c.term = term
		c.vote = conf.InvalidID
	}
	c.leaderID = conf.InvalidID
	c.timeElapsed = 0
	c.resetLease()
}

func (c *core) becomeFollower(term, leaderID uint64) {
	c.reset(term)
	c.leaderID = leaderID
	c.state = RoleFollower
	c.vote = leaderID

	if leaderID != conf.InvalidID {
		log.Infof("%v become %d's follower at %d", c.id, leaderID, c.term)
	} else {
		log.Infof("%v become follower at %d, without leader", c.id, c.term)
	}
}

func (c *core) becomeLeader() {
	utils.Assert(c.state == RoleCandidate || c.state == RoleLeader,
		"%d invalid translation [%v => Leader]", c.id, c.state)

	c.reset(c.term)
	c.leaderID = c.id
	c.state = RoleLeader

	utils.Assert(c.vote == c.id, "leader will vote itself")

	log.Infof("%v become leader at %d", c.id, c.term)
}

func (c *core) becomeCandidate() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => Candidate]", c.id)

	c.reset(c.term + 1)
	c.vote = c.id
	c.state = RoleCandidate

	c.resetNodesVoteState()

	log.Infof("%v become candidate at %d", c.id, c.term)
}

func (c *core) becomePreCandidate() {
	c.reset(c.term)

	// as semantic said, will be InvalidID.
	c.leaderID = conf.InvalidID
	c.state = RolePrevCandidate

	c.resetNodesVoteState()

	// Becoming a pre-candidate changes our state,
	// but doesn't change anything else. In particular it does not increase
	// currentTerm or change votedFor.
	log.Infof("%x became pre-candidate at term %d", c.id, c.term)
}

func (c *core) preCampaign() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => PreCandidate]", c.id)

	c.becomePreCandidate()

	msg := raftpd.Message{
		LogIndex: c.log.LastIndex(),
		LogTerm: c.log.LastTerm(),
		MsgType: raftpd.MsgPreVoteRequest,
	}
	c.sendToNodes(&msg)
}

func (c *core) campaign() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => Candidate]", c.id)

	c.becomeCandidate()

	msg := raftpd.Message{
		LogIndex: c.log.LastIndex(),
		LogTerm: c.log.LastTerm(),
		MsgType: raftpd.MsgVoteRequest,
	}
	c.sendToNodes(&msg)
}

func (c *core) sendToNodes(msg *raftpd.Message) {
	for i := 0; i < len(c.nodes); i++ {
		node := c.nodes[i]
		msg.To = node.ID

		log.Debugf("%x [term: %d, index: %d] send %v request to %x at term %d",
			c.id, c.log.LastTerm(), c.log.LastIndex(), msg.MsgType, msg.To, c.term)
		c.send(msg)
	}
}

func (c *core) quorum() int {
	return quorum(len(c.nodes) + 1)
}

// commit all could commit
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (c *core) poll(idx uint64) {
	if idx <= c.log.CommitIndex() || c.log.Term(idx) != c.term {
		/* maybe committed, or old Term's log entry */
		return
	}
	count := 1
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].Matched >= idx {
			count++
		}
	}

	if count >= c.quorum() {
		c.log.CommitTo(idx)
	}
}

func (c *core) getNodeByID(nodeID uint64) *peer.Node {
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].ID == nodeID {
			return c.nodes[i]
		}
	}
	log.Panicf("not found node with Id: %d", nodeID)
	panic("") /* solve return error */
}

// when someone become leader, commit empty entry first
// to apply old entries. on the same time, reset all nodes
// which active recently to replica state, otherwise probe state.
func (c *core) broadcastVictory() {
	/* noop: empty log ensure commit old Term logs */
	entry := raftpd.Entry{
		Type:  raftpd.EntryBroadcast,
		Index: c.nextIndex(),
		Term:  c.term,
	}
	c.log.Append([]raftpd.Entry{entry})

	c.resetNodesProgress()

	log.Debugf("%d [Term: %d] begin broadcast self's victory ", c.id, c.term)

	c.broadcastAppend()
}

func (c *core) reject(msg *raftpd.Message) {
	var tp raftpd.MessageType
	switch msg.MsgType {
	case raftpd.MsgAppendRequest:
		tp = raftpd.MsgAppendResponse
	case raftpd.MsgHeartbeatRequest:
		tp = raftpd.MsgHeartbeatResponse
	case raftpd.MsgPreVoteRequest:
		tp = raftpd.MsgPreVoteResponse
	case raftpd.MsgReadIndexRequest:
		tp = raftpd.MsgReadIndexResponse
	case raftpd.MsgSnapshotRequest:
		tp = raftpd.MsgSnapshotResponse
	case raftpd.MsgVoteRequest:
		tp = raftpd.MsgVoteResponse
	default:
		return
	}

	m := raftpd.Message{
		To:      msg.From,
		Reject:  true,
		MsgType: tp,
	}

	c.send(&m)
}

func (c *core) applyEntries() {
	entries := c.log.ApplyEntries()
	numberOfEntries := len(entries)
	for i := 0; i < numberOfEntries; i++ {
		entry := &entries[i]
		if entry.Type == raftpd.EntryBroadcast {
			/* ignore broadcast entry */
			continue
		}
		c.callback.applyEntry(entry)
	}
}

func (c *core) resetNodesVoteState() {
	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].ResetVoteState()
	}
}

func (c *core) resetNodesProgress() {
	// When a leader first comes to power,
	// it initializes all nextIndex values to the index just after the
	// last one in its log (11 in Figure 7).
	nextIndex := c.nextIndex()
	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].ToProbe(nextIndex)
	}
}

func (c *core) nextIndex() uint64 {
	return c.log.LastIndex() + 1
}