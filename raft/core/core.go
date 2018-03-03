package core

import (
	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/holder"
	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/raft/core/read"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/pd"
)

type application interface {
	// send message to other node.
	send(msg *raftpd.Message)

	// save read state
	saveReadState(readStatus *read.ReadState)

	// applyEntry apply entry to state machine.
	applyEntry(entry *raftpd.Entry)

	// applySnapshot apply snapshot to state machine.
	// When snapshot has been persisted by state machine,
	// should call ApplySnapshot to rebuild log infos.
	applySnapshot(snapshot *raftpd.Snapshot)

	// readSnapshot return latest snapshot has been persisted
	// from state machine.
	readSnapshot() *raftpd.Snapshot
}

type core struct {
	// Fields need to be persistent.
	term uint64            // current term
	vote uint64            // vote for
	log  *holder.LogHolder // log holder

	// Fields just keep in memory.
	id uint64 // raft id

	// last leader id. If the long time did not
	// receive the leader's message, set InvalidID.
	leaderID uint64
	state    StateRole    // current state role
	nodes    []*peer.Node // information of other nodes in same raft group.

	// Fields for time.
	timeElapsed            int // total elapsed
	randomizedElectionTick int // randomized election tick
	electionTick           int // basis election tick
	heartbeatTick          int // heartbeat timeout tick

	// member-ship change fields.
	pendingConf bool // new configuration is ignored if
	// there exists unapplied configuration.

	// Other fields.
	maxSizePerMsg uint
	readOnly      *read.ReadOnly
	callback      application
}

func makeCore(config *conf.Config, callback application) *core {
	c := new(core)

	// Initialize persistence fields.
	c.vote = config.Vote
	c.term = config.Term
	if config.Entries == nil {
		c.log = holder.MakeLogHolder(config.ID, conf.InvalidIndex, conf.InvalidTerm)
	} else {
		c.log = holder.RebuildLogHolder(config.ID, config.Entries)
	}

	// Initialize memory fields.
	c.id = config.ID
	c.leaderID = conf.InvalidID
	c.state = RoleFollower

	/* make nodes */
	c.nodes = make([]*peer.Node, 0)
	lastIndex := c.log.LastIndex()
	for i := 0; i < len(config.Nodes); i++ {
		if config.Nodes[i] != c.id {
			node := peer.MakeNode(c.id, config.Nodes[i], lastIndex+1)
			c.nodes = append(c.nodes, node)
		}
	}

	// Initialize time rl fields.
	c.timeElapsed = 0
	c.electionTick = config.ElectionTick
	c.heartbeatTick = config.HeartbeatTick
	c.resetRandomizedElectionTimeout()

	// member-ship change fields.
	c.pendingConf = false

	c.callback = callback
	c.readOnly = read.MakeReadOnly()
	c.maxSizePerMsg = config.MaxSizePreMsg

	utils.Assert(c.log.LastIndex() >= c.log.CommitIndex(),
		"%d [Term: %d] last idx: %d less than commit: %d",
		c.id, c.term, c.log.LastIndex(), c.log.CommitIndex())

	log.Debugf("%d build raft at term: %d [firstIdx: %d, lastIdx: %d, commitIdx: %d]",
		c.id, c.term, c.log.FirstIndex(), c.log.LastIndex(), c.log.CommitIndex())

	return c
}

func (c *core) ReadSoftState() SoftState {
	return SoftState{
		LeaderID:  c.leaderID,
		State:     c.state,
		LastIndex: c.log.LastIndex(),
	}
}

func (c *core) ReadHardState() raftpd.HardState {
	return raftpd.HardState{
		Vote:   c.vote,
		Term:   c.term,
		Commit: c.log.CommitIndex(),
	}
}

func (c *core) ReadConfState() raftpd.ConfState {
	state := raftpd.ConfState{}
	state.Nodes = make([]uint64, len(c.nodes))
	for i := 0; i < len(c.nodes); i++ {
		state.Nodes[i] = c.nodes[i].ID
	}
	return state
}

func (c *core) Propose(bytes []byte) (index uint64, term uint64, isLeader bool) {
	if !c.state.IsLeader() {
		return conf.InvalidIndex, conf.InvalidTerm, false
	}

	entry := raftpd.Entry{
		Index: c.log.LastIndex() + 1,
		Term:  c.term,
		Type:  raftpd.EntryNormal,
		Data:  bytes,
	}

	// Leader Append-Only: a leader never overwrites or deletes
	// entries in its log; it only appends new entries. §5.3
	c.log.Append([]raftpd.Entry{entry})

	return entry.Index, entry.Term, true
}

func (c *core) ProposeConfChange(cc *raftpd.ConfChange) (
	index uint64, term uint64, isLeader bool) {
	if !c.state.IsLeader() {
		return conf.InvalidIndex, conf.InvalidTerm, false
	}

	if c.pendingConf {
		log.Infof("propose conf ignored since pending unapplied configuration")
	}
	c.pendingConf = true

	entry := raftpd.Entry{
		Index: c.log.LastIndex() + 1,
		Term:  c.term,
		Type:  raftpd.EntryConfChange,
		Data:  pd.MustMarshal(cc),
	}

	// Leader Append-Only: a leader never overwrites or deletes
	// entries in its log; it only appends new entries. §5.3
	c.log.Append([]raftpd.Entry{entry})

	return entry.Index, entry.Term, true
}

// Read propose a read only request, context is the unique id
// for request.
func (c *core) Read(context []byte) bool {
	// leader must has committed entry at current term.
	if c.log.Term(c.log.CommitIndex()) != c.term {
		return false
	}

	switch c.state {
	case RoleLeader:
		c.readOnly.AddRequest(c.log.CommitIndex(), c.id, context)
		c.broadcastHeartbeatWithCtx(context)
	case RoleFollower:
		// redirect to leader
		if c.leaderID == conf.InvalidID {
			return false
		}
		msg := raftpd.Message{
			MsgType: raftpd.MsgReadIndexRequest,
			To:      c.leaderID,
			Context: context,
		}
		c.send(&msg)
	}
	return true
}

func (c *core) Step(msg *raftpd.Message) {
	log.Debugf("%d received msg: %v", c.id, msg)

	if msg.Term < c.term {
		log.Debugf("%d [term: %d] ignore a %s message with lower term from: %d [term: %d]",
			c.id, c.term, msg.MsgType, msg.From, msg.Term)
		// don't send reject without implement pre-candidate,
		// because candidate will effect leader. but no one tell
		// leader to update itself until new leader broadcast it's victory.
		//
		// because pre vote, leader will not be effected by candidate,
		// but it still can't solve when some node become candidate,
		// and leader come up.
		c.reject(msg)
		return
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
			// leader id will set after received really msg from leader.
			log.Infof("%d [Term: %d] receive a %s message with higher Term from %d [Term: %d]",
				c.id, c.term, msg.MsgType, msg.From, msg.Term)
			c.becomeFollower(msg.Term, conf.InvalidID)
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

	/* apply entries to state machine after handle remote msg */
	c.applyEntries()
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
			c.preCampaign()
		}
	}
}

func (c *core) ApplyConfChange(cc *raftpd.ConfChange) raftpd.ConfState {
	switch cc.ChangeType {
	case raftpd.ConfChangeAddNode:
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		var node = c.getNodeByID(cc.NodeID)
		if node != nil {
			return c.ReadConfState()
		}
		lastIndex := c.log.LastIndex()
		c.nodes = append(c.nodes, peer.MakeNode(c.id, cc.NodeID, lastIndex))
	case raftpd.ConfChangeRemoveNode:
		for i := 0; i < len(c.nodes); i++ {
			if c.nodes[i].ID != cc.NodeID {
				continue
			}
			for j := i; j+1 < len(c.nodes); j++ {
				c.nodes[j] = c.nodes[j+1]
			}
			c.nodes = c.nodes[:len(c.nodes)-1]
			return c.ReadConfState()
		}
	}
	return c.ReadConfState()
}

func (c *core) ApplySnapshot(metadata *raftpd.SnapshotMetadata) {
	c.log.CompactTo(metadata.Index, metadata.Term)
}
