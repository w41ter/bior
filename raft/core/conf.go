package core

import (
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils/log"
	"math"
)

const (
	InvalidIndex = 0
	InvalidId    = math.MaxUint64
	InvalidTerm  = 0
)

type Config struct {
	// id is the identity of the local raft. id cannot be 0.
	Id uint64

	Vote uint64
	Term uint64

	// electionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before electionTick has elapsed, it will become
	// candidate and start an election. electionTick must be greater than
	// HeartbeatTick. We suggest electionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	MaxSizePreMsg uint

	Nodes   []uint64
	Entries []raftpd.Entry
}

func (c *Config) validate() bool {
	if c.Id == 0 {
		log.Panicf("ID cannot be zero")
	}

	if c.HeartbeatTick <= 0 {
		log.Panicf("heartbeat tick must be great than zero")
	}

	if c.ElectionTick <= 0 {
		log.Panicf("election tick must be great than zero")
	}

	return true
}
