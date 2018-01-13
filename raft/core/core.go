package core

import (
	"math/rand"
	"raft/proto"
	"utils"
	"utils/pd"
	"utils/log"
)

type campaignState int

const (
	campaignPreCandidate campaignState = iota
	campaignCandidate
)

type Application interface {
	send(msg *raftpd.Message)
	saveReadState(readStatus *ReadState)
	applyEntry(entry *raftpd.Entry)
	applySnapshot(snapshot *raftpd.Snapshot)
	readSnapshot() *raftpd.Snapshot
}

type SoftState struct {
	LeaderId uint64
	State    StateRole
}

type core struct {
	term uint64
	vote uint64
	log  *LogHolder

	id       uint64
	leaderId uint64
	state    StateRole
	callback Application

	nodes []node

	timeElapsed            int
	randomizedElectionTick int
	electionTick           int
	heartbeatTick          int

	readOnly *readOnly
	maxSizePerMsg uint
}

func makeCore(config *Config, cb Application) *core {
	c := new(core)
	c.callback = cb
	c.id = config.Id
	c.vote = config.Vote
	c.term = config.Term
	c.electionTick = config.ElectionTick
	c.heartbeatTick = config.HeartbeatTick
	c.readOnly = makeReadOnly()
	c.maxSizePerMsg = config.MaxSizePreMsg

	if config.Entries == nil {
		c.log = MakeLogHolder(config.Id, InvalidIndex, InvalidTerm)
	} else {
		c.log = RebuildLogHolder(config.Id, config.Entries)
	}

	/* make nodes */
	c.nodes = make([]node, 0)
	lastIndex := c.log.lastIndex()
	for i := 0; i < len(config.Nodes); i++ {
		if config.Nodes[i] != c.id {
			node := makeNode(config.Nodes[i], lastIndex+1)
			c.nodes = append(c.nodes, node)
		}
	}

	c.state = FOLLOWER
	c.leaderId = InvalidId
	c.timeElapsed = 0
	c.resetRandomizedElectionTimeout()

	utils.Assert(c.log.lastIndex() >= c.log.commitIndex,
		"%d [Term: %d] last idx: %d less than commit: %d",
		c.id, c.term, c.log.lastIndex(), c.log.commitIndex)
	return c
}

func (c *core) ReadSoftState() SoftState {
	return SoftState{
		LeaderId: c.leaderId,
		State:    c.state,
	}
}

func (c *core) ReadHardState() raftpd.HardState {
	return raftpd.HardState{
		Vote:   c.vote,
		Term:   c.term,
		Commit: c.log.commitIndex,
	}
}

func (c *core) Propose(bytes []byte) (index uint64, term uint64, isLeader bool) {
	if !c.state.IsLeader() {
		return InvalidIndex, InvalidTerm, false
	}

	entry := raftpd.Entry{
		Index: c.log.lastIndex() + 1,
		Term:  c.term,
		Type:  raftpd.EntryNormal,
		Data:  bytes,
	}

	c.log.Append([]raftpd.Entry{entry})
	return entry.Index, entry.Term, true
}

func (c *core) ProposeConfChange(cc *raftpd.ConfChange) (index uint64, term uint64, isLeader bool) {
	if !c.state.IsLeader() {
		return InvalidIndex, InvalidTerm, false
	}

	entry := raftpd.Entry{
		Index: c.log.lastIndex() + 1,
		Term:  c.term,
		Type:  raftpd.EntryConfChange,
		Data:  pd.MustMarshal(cc),
	}
	c.log.Append([]raftpd.Entry{entry})
	return entry.Index, entry.Term, true
}

func (c *core) readConfState() raftpd.ConfState {
	state := raftpd.ConfState{}
	state.Nodes = make([]uint64, len(c.nodes))
	for i := 0; i < len(c.nodes); i++ {
		state.Nodes[i] = c.nodes[i].id
	}
	return state
}

func (c *core) ApplyConfChange(cc *raftpd.ConfChange) raftpd.ConfState {
	switch cc.ChangeType {
	case raftpd.ConfChangeAddNode:
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		var node = c.getNodeById(cc.NodeId)
		if node != nil {
			return c.readConfState()
		}
		lastIndex := c.log.lastIndex()
		c.nodes = append(c.nodes, makeNode(cc.NodeId, lastIndex))
	case raftpd.ConfChangeRemoveNode:
		for i := 0; i < len(c.nodes); i++ {
			if c.nodes[i].id != cc.NodeId {
				continue
			}
			for j := i; j + 1 < len(c.nodes); j++ {
				c.nodes[j] = c.nodes[j+1]
			}
			c.nodes = c.nodes[:len(c.nodes)-1]
			return c.readConfState()
		}
	}
	return c.readConfState()
}

func (c *core) CompactTo(metadata *raftpd.SnapshotMetadata) {
	c.log.CompactTo(metadata.Index, metadata.Term)
}

// Read propose a read only request, context is the unique id
// for request.
func (c *core) Read(context []byte) {
	switch c.state {
	case LEADER:
		c.readOnly.addRequest(c.log.commitIndex, c.id, context)
		c.broadcastHeartbeatWithCtx(context)
	case FOLLOWER:
		// redirect to leader
		if c.leaderId == InvalidId {
			return
		}
		msg := raftpd.Message{}
		msg.To = c.leaderId
		msg.MsgType = raftpd.MsgReadIndexRequest
		msg.Context = context
		c.send(&msg)
	}
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
		panic("unreachable")
	}

	m := raftpd.Message{
		To:      msg.From,
		Reject:  true,
		MsgType: tp,
	}

	c.send(&m)
}

func (c *core) Step(msg *raftpd.Message) {
	if msg.Term < c.term {
		log.Infof("%d [term: %d] ignore a %s message with lower term from: %d [term: %d]",
			c.id, c.term, msg.MsgType, msg.From, msg.Term)
		// don't send reject, because candidate will effect leader.
		// but no one tell leader to update itself until new leader
		// broadcast it's victory.
		// update: because pre vote, leader will not be effected by
		// candidate, but it can't solve when some node become candidate,
		// and leader come up .
		c.reject(msg)
	} else if msg.Term > c.term {
		if msg.MsgType == raftpd.MsgPreVoteRequest {
			// currentTerm never changes when receiving a PreVote.
		} else if msg.MsgType == raftpd.MsgPreVoteResponse && msg.Reject {
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		} else {
			// important: set leader id for future request, such as ReadIndex.
			leaderId := msg.From
			if msg.MsgType == raftpd.MsgVoteRequest {
				leaderId = InvalidId
			}
			log.Infof("%d [Term: %d] receive a %s message with higher Term from %d [Term: %d]",
				c.id, c.term, msg.MsgType, msg.From, msg.Term)
			c.becomeFollower(msg.Term, leaderId)
		}
	}

	switch msg.MsgType {
	case raftpd.MsgPreVoteRequest:
		c.handlePreVote(msg)
	case raftpd.MsgVoteRequest:
		c.handleVote(msg)
	default:
		c.dispatch(msg)
	}
}

func (c *core) Periodic(millsSinceLastPeriod int) {
	c.timeElapsed += millsSinceLastPeriod
	log.Debugf("%d periodic %d, time elapsed %d", c.id, millsSinceLastPeriod, c.timeElapsed)

	if c.state.IsLeader() {
		if c.heartbeatTick <= c.timeElapsed {
			c.broadcastAppend()
			c.becomeLeader()
		}
	} else if c.randomizedElectionTick <= c.timeElapsed {
		if len(c.nodes) > 1 {
			c.campaign(campaignPreCandidate)
		}
	}
}

func (c *core) send(msg *raftpd.Message) {
	msg.Term = c.term
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

func (c *core) reset(term uint64) {
	if c.term != term {
		c.term = term
		c.vote = InvalidId
	}
	c.leaderId = InvalidId
	c.timeElapsed = 0
	c.resetRandomizedElectionTimeout()
}

func (c *core) becomeFollower(term, leaderId uint64) {
	c.reset(term)
	c.leaderId = leaderId
	c.state = FOLLOWER
	c.vote = leaderId

	log.Infof("%v become follower at %d", c.id, c.term)
}

func (c *core) becomeLeader() {
	utils.Assert(c.state == CANDIDATE, "invalid translation [%v => Leader]", c.state)

	c.reset(c.term)
	c.leaderId = c.id
	c.state = LEADER
	c.vote = c.id

	log.Infof("%v become leader at %d", c.id, c.term)
}

func (c *core) becomeCandidate() {
	utils.Assert(c.state != LEADER, "invalid translation [Leader => Candidate]")

	c.reset(c.term + 1)
	c.vote = c.id
	c.state = CANDIDATE

	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].resetVoteState()
	}

	log.Infof("%v become candidate at %d", c.id, c.term)
}

func (c *core) becomePreCandidate() {
	c.reset(c.term)
	c.state = PRE_CANDIDATE

	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].resetVoteState()
	}

	// Becoming a pre-candidate changes our state,
	// but doesn't change anything else. In particular it does not increase
	// currentTerm or change votedFor.
	log.Infof("%x became pre-candidate at term %d", c.id, c.term)
}

func (c *core) campaign(ct campaignState) {
	utils.Assert(c.state != LEADER,
		"invalid translation [Leader => PreCandidate/Candidate]")

	msg := raftpd.Message{}
	msg.LogIndex = c.log.lastIndex()
	msg.LogTerm = c.log.lastTerm()
	if ct == campaignPreCandidate {
		msg.Term = c.term + 1
		msg.MsgType = raftpd.MsgPreVoteRequest
		c.becomePreCandidate()
	} else {
		msg.Term = c.term
		msg.MsgType = raftpd.MsgVoteRequest
		c.becomeCandidate()
	}

	for i := 0; i < len(c.nodes); i++ {
		node := &c.nodes[i]
		msg.To = node.id

		log.Infof("%x [term: %d, index: %d] send %v request to %x at term %d",
			c.id, c.log.lastTerm(), c.log.lastIndex(), msg.MsgType, msg.To, c.term)
		c.send(&msg)
	}
}

func quorum(len int) int {
	return len/2 + 1
}

func (c *core) quorum() int {
	return quorum(len(c.nodes) + 1)
}

// commit all could commit
func (c *core) poll(idx uint64) {
	if idx <= c.log.commitIndex || c.log.Term(idx) != c.term {
		/* maybe committed, or old Term's log entry */
		return
	}
	count := 1
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].matched >= idx {
			count++
		}
	}

	if count >= quorum(len(c.nodes)+1) {
		c.log.commitIndex = idx
	}
}

func (c *core) getNodeById(nodeId uint64) *node {
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].id == nodeId {
			return &c.nodes[i]
		}
	}
	log.Panicf("not found node with Id: %d", nodeId)
	panic("") /* solve return error */
}

// when someone become leader, commit empty entry first
// to apply old entries. on the same time, reset all nodes
// which active recently to replica state, otherwise probe state.
func (c *core) broadcastVictory() {
	/* empty log ensure commit old Term logs */
	entry := raftpd.Entry{
		Index: c.log.lastIndex() + 1,
		Term:  c.term,
		Data:  nil,
	}
	entries := []raftpd.Entry{entry}
	c.log.Append(entries)

	lastIndex := c.log.lastIndex()
	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].matched = InvalidId
		c.nodes[i].nextIdx = lastIndex + 1
		c.nodes[i].becomeProbe()
	}

	log.Debugf("%d [Term: %d] begin broadcast self's victory ", c.id, c.term)

	c.broadcastAppend()
}
