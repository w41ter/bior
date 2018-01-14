package core

import (
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/log"
)

func (c *core) stepLeader(msg *raftpd.Message) {
	switch msg.MsgType {
	case raftpd.MsgHeartbeatResponse:
		c.handleHeartbeatResponse(msg)
	case raftpd.MsgSnapshotResponse:
		c.handleSnapshotResponse(msg)
	case raftpd.MsgAppendResponse:
		c.handleAppendEntriesResponse(msg)
	case raftpd.MsgUnreachable:
		c.handleUnreachable(msg)
	case raftpd.MsgReadIndexRequest:
		c.handleReadIndexRequest(msg)
	}
}

func (c *core) stepFollower(msg *raftpd.Message) {
	switch msg.MsgType {
	case raftpd.MsgReadIndexResponse:
		readState := ReadState{
			Index:      msg.Index,
			RequestCtx: msg.Context,
		}

		c.callback.saveReadState(&readState)
	case raftpd.MsgAppendRequest:
		c.handleAppendEntries(msg)
	case raftpd.MsgHeartbeatRequest:
		c.handleHeartbeat(msg)
	case raftpd.MsgSnapshotRequest:
		c.handleSnapshot(msg)
	}
}

func (c *core) stepCandidate(msg *raftpd.Message) {
	switch msg.MsgType {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	case raftpd.MsgPreVoteResponse:
		if c.state == PRE_CANDIDATE {
			c.handleVoteResponse(msg)
		}
	case raftpd.MsgVoteResponse:
		if c.state == CANDIDATE {
			c.handleVoteResponse(msg)
		}

		// If a candidate receives an AppendEntries RPC from another rpc claiming
		// to be leader whose term is at least as large as the candidate's current term,
		// it recognizes the leader as legitimate and returns to follower state.
	case raftpd.MsgAppendRequest:
		c.becomeFollower(msg.Term, msg.From)
		c.handleAppendEntries(msg)
	case raftpd.MsgHeartbeatRequest:
		c.becomeFollower(msg.Term, msg.From)
		c.handleHeartbeat(msg)
	case raftpd.MsgSnapshotRequest:
		c.becomeFollower(msg.Term, msg.From)
		c.handleSnapshot(msg)
	}
}

func (c *core) dispatch(msg *raftpd.Message) {
	switch c.state {
	case LEADER:
		c.stepLeader(msg)
	case FOLLOWER:
		c.stepFollower(msg)
	case PRE_CANDIDATE:
		fallthrough
	case CANDIDATE:
		c.stepCandidate(msg)
	}
}

func (c *core) handleReadIndexRequest(msg *raftpd.Message) {
	utils.Assert(c.quorum() > 1 && c.state.IsLeader(), "receive wrong message")
	// c must be leader, so term great than InvalidTerm.
	if c.log.commitIndex != c.term {
		// Reject read only request when this leader has not
		// committed any log entry at its term. (raft thesis 6.4)
		return
	}

	c.readOnly.addRequest(c.log.commitIndex, msg.From, msg.Context)
	c.broadcastHeartbeatWithCtx(msg.Context)
}

// RPC:
// - AppendEntries(commitIndex, prevLogIndex, prevLogTerm, entries)
// - AppendEntriesReply(index, hint, reject)
func (c *core) handleAppendEntries(msg *raftpd.Message) {
	reply := raftpd.Message{}
	reply.MsgType = raftpd.MsgAppendResponse
	reply.From = c.id
	if c.log.commitIndex > msg.LogIndex {
		log.Infof("%d [Term: %d, commit: %d] reject expired append Entries "+
			"from %d [logterm: %d, idx: %d]", c.id, c.term, c.log.commitIndex,
			msg.From, msg.LogTerm, msg.LogIndex)
		// expired append Entries has been committed,
		// so it reply same with success append.
		reply.Index = c.log.commitIndex
		reply.Reject = false
		c.send(&reply)
	} else if idx, ok := c.log.TryAppend(msg.LogIndex, msg.LogTerm, msg.Entries); ok {
		log.Infof("%d [Term: %d, commit: %d] accept append Entries "+
			"from %d [logterm: %d, idx: %d]", c.id, c.term, c.log.commitIndex,
			msg.From, msg.LogTerm, msg.LogIndex)

		c.log.CommitTo(utils.MinUint64(msg.Index, idx))
		reply.Index = idx
		reply.Reject = false
		c.send(&reply)
	} else {
		log.Infof("%d [logterm: %d, commit: %d, last idx: %d] rejected msgApp "+
			"[logterm: %d, idx: %d] from %d", c.id, c.log.Term(msg.LogIndex),
			c.log.commitIndex, c.log.lastIndex(), msg.LogTerm, msg.LogIndex, msg.From)
		reply.Index = msg.LogIndex
		reply.RejectHint = idx /* idx is hintIndex*/
		reply.Reject = true
		c.send(&reply)
	}
}

func (c *core) handleAppendEntriesResponse(msg *raftpd.Message) {
	node := c.getNodeById(msg.From)

	successAppend := node.handleAppendEntries(msg.Reject, msg.Index, msg.RejectHint)
	if successAppend {
		c.poll(node.matched)
	}
}

func (c *core) tryRestore(snapshot *raftpd.Snapshot) bool {
	utils.Assert(snapshot != nil, "nullptr exception")

	if snapshot.Metadata.Index <= c.log.commitIndex {
		/* expired snapshot install */
		return false
	}

	if c.log.Term(snapshot.Metadata.Index) == snapshot.Metadata.Term {
		c.log.CommitTo(snapshot.Metadata.Index)
		return false
	}

	// c.log.tryRestore(snapshot)
	return true
}

func (c *core) handleSnapshot(msg *raftpd.Message) {
	reply := raftpd.Message{}
	reply.To = msg.From
	reply.MsgType = raftpd.MsgSnapshotResponse
	reply.Reject = false
	if c.tryRestore(msg.Snapshot) {
		reply.Index = c.log.lastIndex()
		log.Infof("%x [commit: %d] restore snapshot [index: %d, term: %d]",
			c.id, c.log.commitIndex, msg.Snapshot.Metadata.Index, msg.Snapshot.Metadata.Term)
		c.callback.applySnapshot(msg.Snapshot)
	} else {
		log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			c.id, c.log.commitIndex, msg.Snapshot.Metadata.Index, msg.Snapshot.Metadata.Term)
		reply.Index = c.log.commitIndex
	}
	c.send(&reply)
}

func (c *core) handleSnapshotResponse(msg *raftpd.Message) {
	node := c.getNodeById(msg.From)
	node.handleSnapshot(msg.Reject, msg.Index)
}

func (c *core) handleUnreachable(msg *raftpd.Message) {
	node := c.getNodeById(msg.From)

	node.handleUnreachable()
	log.Infof("%x failed to send message to %x"+
		" because it is unreachable", c.id, msg.From)
}

func (c *core) handleHeartbeat(msg *raftpd.Message) {
	c.leaderId = msg.From
	c.timeElapsed = 0
	c.log.CommitTo(msg.Index)

	reply := raftpd.Message{}
	reply.To = msg.From
	reply.Reject = false
	reply.MsgType = raftpd.MsgHeartbeatResponse
	c.send(&reply)
}

func (c *core) handleHeartbeatResponse(msg *raftpd.Message) {
	ackCount := c.readOnly.receiveAck(msg.From, msg.Context)
	if ackCount < c.quorum() {
		return
	}

	rss := c.readOnly.advance(msg.Context)
	for _, rs := range rss {
		if rs.to == c.id {
			readState := ReadState{
				Index:      rs.index,
				RequestCtx: rs.context,
			}

			c.callback.saveReadState(&readState)
		} else {
			redirect := raftpd.Message{
				To:      rs.to,
				MsgType: raftpd.MsgReadIndexResponse,
				Index:   rs.index,
				Context: rs.context,
			}
			c.send(&redirect)
		}
	}
}

func (c *core) voteStateCount(state voteState) int {
	/* self has one */
	var count = 1
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].vote == state {
			count++
		}
	}
	return count
}

func (c *core) handlePreVote(msg *raftpd.Message) {
	reply := raftpd.Message{}
	reply.To = msg.From
	reply.MsgType = raftpd.MsgPreVoteResponse

	// Reply false if last AppendEntries call was received less than election timeout ago.
	// Reply false if term < currentTerm.
	// Reply false if candidate's log isn't at least as up­to­date as receiver's log.
	if (c.leaderId != InvalidId && c.timeElapsed < c.electionTick) ||
		(msg.Term < c.term) ||
		!c.log.IsUpToDate(msg.LogIndex, msg.LogTerm) {
		reply.Reject = false
	} else {
		reply.Reject = true
	}

	c.send(&reply)
}

func (c *core) handleVote(msg *raftpd.Message) {
	reply := raftpd.Message{}
	reply.To = msg.From
	reply.MsgType = raftpd.MsgVoteResponse

	// no vote or vote for candidate, and log is at least as up-to-date as receiver's.
	if c.vote == InvalidId || c.vote == msg.From ||
		c.log.IsUpToDate(msg.LogIndex, msg.LogTerm) {
		reply.Reject = false
	} else {
		reply.Reject = true
	}

	c.send(&reply)
}

func (c *core) handleVoteResponse(msg *raftpd.Message) {
	if msg.Reject {
		log.Infof("%x received %v rejection from %x at term %d",
			c.id, msg.MsgType, msg.From, c.term)
	} else {
		log.Infof("%x received %v from %x at term %s",
			c.id, msg.MsgType, msg.From, msg.Term)
	}

	node := c.getNodeById(msg.From)
	node.updateVoteState(msg.Reject)

	count := c.voteStateCount(voteGranted)
	if count >= c.quorum() {
		if msg.MsgType == raftpd.MsgVoteResponse {
			c.becomeLeader()
			c.broadcastVictory()
		} else {
			c.campaign(campaignCandidate)
		}
		return
	}

	// TODO: why?
	// return to follower state if it receives vote denial from a majority
	count = c.voteStateCount(voteReject)
	if count >= c.quorum() {
		c.becomeFollower(msg.Term, InvalidId)
	}
}

func (c *core) broadcastHeartbeatWithCtx(context []byte) {
	for i := 0; i < len(c.nodes); i++ {
		node := &c.nodes[i]
		c.sendHeartbeat(node, context)
	}
}

func (c *core) sendHeartbeat(node *node, context []byte) {
	// Attach the commit as min(to.matched, raftlog.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index, in order to preserving Log Matching Property.

	msg := raftpd.Message{}
	msg.To = node.id
	msg.MsgType = raftpd.MsgHeartbeatRequest
	msg.Index = utils.MinUint64(node.matched, c.log.commitIndex)
	msg.Context = context

	c.send(&msg)
}

// broadcastAppend send append or snapshot to followers.
func (c *core) broadcastAppend() {
	firstIndex := c.log.firstIndex()
	for i := 0; i < len(c.nodes); i++ {
		node := &c.nodes[i]
		/* ignore paused node */
		if node.isPaused() {
			continue
		}

		if c.nodes[i].nextIdx >= firstIndex {
			c.sendAppend(node)
		} else {
			// send snapshot if we failed to get term or entries
			c.sendSnapshot(node)
		}
	}
}

func (c *core) sendAppend(node *node) {
	msg := raftpd.Message{}
	msg.To = node.id
	msg.Index = c.log.commitIndex
	msg.MsgType = raftpd.MsgAppendRequest
	msg.LogIndex = node.nextIdx - 1
	msg.LogTerm = c.log.Term(msg.LogIndex)

	if c.log.lastIndex() >= node.nextIdx {
		entries := c.log.Slice(node.nextIdx, c.log.lastIndex()+1)
		// slice message with max size
		var size uint = 0
		for i := 0; i < len(entries); i++ {
			size += uint(16 + len(entries[i].Data))
			if size > c.maxSizePerMsg {
				entries = entries[:i]
			}
		}
		msg.Entries = make([]raftpd.Entry, len(entries))
		copy(msg.Entries, entries)
		utils.Assert(len(entries) == 0 || msg.Entries[0].Index != InvalidIndex, "")
	} else {
		msg.Entries = make([]raftpd.Entry, 0)
	}

	log.Debugf("%d [Term: %d] send append pd [idx: %d, Term: %d] "+
		"to node: %d [matched: %d next index: %d]",
		c.id, c.term, msg.LogIndex, msg.LogTerm, node.id, node.matched, node.nextIdx)

	// Debug: validate consistency
	if len(msg.Entries) > 0 {
		for i := 0; i < len(msg.Entries)-1; i++ {
			utils.Assert(msg.Entries[i].Index+1 == msg.Entries[i+1].Index,
				"%d index:%d at:%d not sequences", c.id, msg.Entries[i].Index, i)
		}
	}

	if len(msg.Entries) != 0 {
		switch node.state {
		case nodeStateProbe:
			// FIXME: only send little message
			node.pause()
		case nodeStateReplicate:
			// optimistically increase the next when in ProgressStateReplicate
			lastIndex := msg.Entries[len(msg.Entries)-1].Index
			node.optimisticUpdate(lastIndex)
		default:
			log.Fatalf("%x is sending append in unhandled state %s", c.id, node.state)
		}
	}
	c.send(&msg)
}

func (c *core) sendSnapshot(node *node) {
	msg := raftpd.Message{}
	msg.To = node.id

	snapshot := c.callback.readSnapshot()
	// if snapshot is building at now, it will return nil,
	// so just ignore it and send message to it on next tick.
	if snapshot == nil {
		log.Infof("%x failed to send snapshot to %x because snapshot "+
			"is temporarily unavailable", c.id, node.id)
		return
	}

	log.Infof("%x [firstIndex: %d, commit: %d] send "+
		"snapshot[index: %d, term: %d] to %x", c.id, c.log.firstIndex(),
		snapshot.Metadata.Index, snapshot.Metadata.Term, node.id)

	node.sendSnapshot(snapshot.Metadata.Index)

	log.Infof("%x paused sending replication messages to %x", c.id, node.id)
	msg.Snapshot = snapshot
	msg.MsgType = raftpd.MsgSnapshotRequest

	c.send(&msg)
}
