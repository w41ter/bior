package core

import (
	"math/rand"

	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/raft/core/read"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
)

func quorum(len int) int {
	return len/2 + 1
}

// send send message to remote peers.
func (c *core) send(msg *raftpd.Message) {
	if msg.MsgType == raftpd.MsgPreVoteRequest {
		/* request pre vote: future term */
		msg.Term = c.term + 1
	} else if msg.MsgType == raftpd.MsgPreVoteResponse {
		/* don't change term of prevote response, so
		sender need set term by self. */
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
	c.resetLease()
	c.pendingConf = false
}

func (c *core) becomeFollower(term, leaderID uint64) {
	c.reset(term)
	c.leaderID = leaderID
	c.state = RoleFollower
	c.vote = leaderID

	if leaderID != conf.InvalidID {
		log.Debugf("%v become %d's follower at %d", c.id, leaderID, c.term)
	} else {
		log.Debugf("%v become follower at %d, without leader", c.id, c.term)
	}
}

func (c *core) becomeLeader() {
	utils.Assert(c.state == RoleCandidate || c.state == RoleLeader,
		"%d invalid translation [%v => Leader]", c.id, c.state)

	c.reset(c.term)
	c.leaderID = c.id
	c.state = RoleLeader

	num := c.numOfPendingConf()
	if num > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	c.pendingConf = num == 1 // whether there has conf entry.

	utils.Assert(c.vote == c.id, "leader will vote itself")

	log.Debugf("%v become leader at %d [firstIdx: %d, lastIdx: %d]",
		c.id, c.term, c.log.FirstIndex(), c.log.LastIndex())
}

func (c *core) becomeCandidate() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => Candidate]", c.id)

	c.reset(c.term + 1)
	c.vote = c.id
	c.state = RoleCandidate

	c.resetNodesVoteState()

	log.Debugf("%v become candidate at %d", c.id, c.term)
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
	log.Debugf("%x became pre-candidate at term %d", c.id, c.term)
}

func (c *core) preCampaign() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => PreCandidate]", c.id)

	c.becomePreCandidate()

	msg := raftpd.Message{
		LogIndex: c.log.LastIndex(),
		LogTerm:  c.log.LastTerm(),
		MsgType:  raftpd.MsgPreVoteRequest,
	}
	c.sendToNodes(&msg)
}

func (c *core) campaign() {
	utils.Assert(c.state != RoleLeader,
		"%d invalid translation [Leader => Candidate]", c.id)

	c.becomeCandidate()

	msg := raftpd.Message{
		LogIndex: c.log.LastIndex(),
		LogTerm:  c.log.LastTerm(),
		MsgType:  raftpd.MsgVoteRequest,
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
	return nil
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
		//if entry.Type == raftpd.EntryBroadcast {
		//	/* ignore broadcast entry */
		//	continue
		//}
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

// backToFollower used by preCandidate/Candidate to
// go back to follower campaign failed.
func (c *core) backToFollower(term uint64, leaderID uint64) {
	c.vote = leaderID
	c.becomeFollower(term, leaderID)
}

func (c *core) numOfPendingConf() int {
	var num int
	entries := c.log.Slice(c.log.CommitIndex()+1, c.log.LastIndex()+1)
	for i := 0; i < len(entries); i++ {
		if entries[i].Type == raftpd.EntryConfChange {
			num++
		}
	}
	return num
}

func (c *core) addNode(nodeID uint64) {
	c.pendingConf = false

	// Ignore any redundant addNode calls (which can happen because the
	// initial bootstrapping entries are applied twice).
	var node = c.getNodeByID(nodeID)
	if node != nil || c.id == nodeID {
		/* do not add self to nodes */
		return
	}
	lastIndex := c.log.LastIndex()
	c.nodes = append(c.nodes, peer.MakeNode(c.id, nodeID, lastIndex))
}

func (c *core) removeNode(nodeID uint64) {
	c.pendingConf = false

	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].ID != nodeID {
			continue
		}

		for j := i; j+1 < len(c.nodes); j++ {
			c.nodes[j] = c.nodes[j+1]
		}
		c.nodes = c.nodes[:len(c.nodes)-1]
		return
	}
}

func (c *core) advanceReadOnly(ctx []byte) {
	rss := c.readOnly.Advance(ctx)
	for _, rs := range rss {
		if rs.To == c.id {
			log.Debugf("%d [term: %d] save read state: %d, %v",
				c.id, c.term, rs.Index, rs.Context)

			readState := read.ReadState{
				Index:      rs.Index,
				RequestCtx: rs.Context,
			}

			c.callback.saveReadState(&readState)
		} else {
			log.Debugf("%d [term: %d] redirect heartbeat response %d to %d %v",
				c.id, c.term, rs.Index, rs.To, rs.Context)

			redirect := raftpd.Message{
				To:      rs.To,
				MsgType: raftpd.MsgReadIndexResponse,
				Index:   rs.Index,
				Context: rs.Context,
			}
			c.send(&redirect)
		}
	}
}
