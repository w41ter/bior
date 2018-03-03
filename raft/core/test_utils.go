package core

import (
	"container/list"
	"time"

	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
)

func sleep(milliseconds int) {
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

type raftOpt func(c *RawNode)

func vote(idx uint64) raftOpt {
	return func(c *RawNode) {
		c.vote = idx
	}
}

func term(idx uint64) raftOpt {
	return func(c *RawNode) {
		c.term = idx
	}
}

func randTick(tick int) raftOpt {
	return func(c *RawNode) {
		c.randomizedElectionTick = tick
	}
}

func timeElapsed(time int) raftOpt {
	return func(c *RawNode) {
		c.timeElapsed = time
	}
}

func leaderID(idx uint64) raftOpt {
	return func(c *RawNode) {
		c.leaderID = idx
	}
}

func state(state StateRole) raftOpt {
	return func(c *RawNode) {
		c.state = state
	}
}

func makeTestRaft(
	id uint64,
	peers []uint64,
	election, heartbeat int,
	entries []raftpd.Entry,
	callback NodeApplication,
	opts ...raftOpt,
) *RawNode {
	c := conf.Config{
		ID:            id,
		Vote:          conf.InvalidID,
		Term:          conf.InvalidTerm,
		ElectionTick:  election,
		HeartbeatTick: heartbeat,
		Nodes:         peers,
		MaxSizePreMsg: 1024,
		Entries:       entries,
	}

	raft := MakeRawNode(&c, callback)

	for _, opt := range opts {
		opt(raft)
	}
	return raft
}

type network struct {
	peers      map[uint64]*RawNode
	msgs       *list.List
	cutMap     map[uint64]uint64
	ignoreType map[raftpd.MessageType]struct{}
}

func makeNetwork(prs ...*RawNode) *network {
	net := network{
		peers:      make(map[uint64]*RawNode),
		msgs:       list.New(),
		cutMap:     make(map[uint64]uint64),
		ignoreType: make(map[raftpd.MessageType]struct{}),
	}
	for i := 0; i < len(prs); i++ {
		net.peers[prs[i].id] = prs[i]
	}
	return &net
}

func (n *network) add(node *RawNode) {
	n.peers[node.id] = node
}

func (n *network) send(msg *raftpd.Message) {
	n.msgs.PushBack(*msg)
	n.dispatchMessages()
}

func (n *network) transferMessages(node uint64) {
	peer := n.peers[node]
	for i := 0; i < len(peer.messages); i++ {
		n.msgs.PushBack(peer.messages[i])
	}
	peer.messages = peer.messages[:0]
}

func (n *network) dispatchMessages() {
	for n.msgs.Len() > 0 {
		// should stable all entries before any op.
		n.stableAllEntries()

		first := n.msgs.Front()
		msg := first.Value.(raftpd.Message)
		n.msgs.Remove(first)

		// Drop the message if the remote peer is dead or
		// the connection to remote is cut down.
		if _, ok := n.peers[msg.To]; !ok || n.cutMap[msg.From] == msg.To {
			continue
		}
		// ignore the message
		if _, ok := n.ignoreType[msg.MsgType]; ok {
			continue
		}
		n.peers[msg.To].Step(&msg)
		n.transferMessages(msg.To)
	}
}

func (n *network) startElection(node uint64) {
	n.peers[node].preCampaign()
	n.transferMessages(node)
	n.dispatchMessages()
}

func (n *network) propose(node uint64, data []byte) (uint64, uint64) {
	idx, term, isLeader := n.peers[node].Propose(data)
	if !isLeader {
		panic("propose but not leader")
	}
	return idx, term
}

func (n *network) readIndex(node uint64, data []byte) bool {
	if ok := n.peers[node].Read(data); !ok {
		return false
	}
	n.transferMessages(node)
	n.dispatchMessages()
	return true
}

func (n *network) peer(node uint64) *RawNode {
	return n.peers[node]
}

func (n *network) set(node *RawNode) {
	n.peers[node.id] = node
}

func (n *network) down(node uint64) {
	delete(n.peers, node)
}

// Cut down the connection between n1 and n2.
func (n *network) cut(c1, c2 uint64) {
	n.cutMap[c1] = c2
	n.cutMap[c2] = c1
}

// Restore the connection between n1 and n2.
func (n *network) restore(c1, c2 uint64) {
	n.cutMap[c1] = c2
	n.cutMap[c2] = c1
}

// ignore a specified type of message
func (n *network) ignore(tp raftpd.MessageType) {
	n.ignoreType[tp] = struct{}{}
}

// recover the whole network to normal
func (n *network) recover() {
	n.ignoreType = make(map[raftpd.MessageType]struct{})
	n.cutMap = make(map[uint64]uint64)
}

// wait idx been apply, otherwise broadcast entries.
func (n *network) waitApply(idx uint64) (raftpd.Entry, bool) {
	beg := time.Now()
	for time.Since(beg).Seconds() < 5 {
		sleep(50)
		leader := n.leader()
		peer, ok := n.peers[leader]
		if leader == conf.InvalidID || !ok {
			continue
		}
		peer.broadcastAppend()
		n.transferMessages(peer.id)
		n.dispatchMessages()

		if entry, ok := n.applied(peer.id, idx); ok {
			return entry, ok
		}
	}
	return raftpd.Entry{}, false
}

// wait idx been commit, otherwise broadcast entries.
func (n *network) waitCommit(idx uint64) bool {
	beg := time.Now()
	for time.Since(beg).Seconds() < 5 {
		sleep(50)
		leader := n.leader()
		peer, ok := n.peers[leader]
		if leader == conf.InvalidID || !ok {
			continue
		}
		peer.broadcastAppend()
		n.transferMessages(peer.id)
		n.dispatchMessages()

		if n.allCommitted(idx) {
			return true
		}
	}
	return false
}

// return the leader of group, if no leader here, return InvalidId
func (n *network) leader() uint64 {
	for _, rf := range n.peers {
		if rf.state == RoleLeader {
			return rf.id
		}
	}
	return conf.InvalidID
}

func (n *network) applied(node uint64, index uint64) (raftpd.Entry, bool) {
	peer := n.peers[node]
	for i := 0; i < len(peer.commitEntries); i++ {
		if peer.commitEntries[i].Index == index {
			return peer.commitEntries[i], true
		}
	}
	return raftpd.Entry{}, false
}

func (n *network) allApplied(idx uint64) (raftpd.Entry, bool) {
	// TODO:
	return raftpd.Entry{}, false
}

func (n *network) allCommitted(idx uint64) bool {
	for _, peer := range n.peers {
		if peer.log.CommitIndex() < idx {
			return false
		}
	}
	return true
}

func (n *network) stableAllEntries() {
	for _, peer := range n.peers {
		peer.log.StableEntries()
	}
}
