package conf

import (
	"math"

	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/proto"
)

// Invalid value for raft.
const (
	InvalidIndex uint64 = 0
	InvalidID    uint64 = math.MaxUint64
	InvalidTerm  uint64 = 0
)

// Config given information to build raft algorithm.
type Config struct {
	// Id is the identity of the local raft. id cannot be 0.
	ID uint64

	Vote uint64
	Term uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
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

// Verify check whether fields of Config is valid.
func (c *Config) Verify() bool {
	if c.ID == 0 {
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
