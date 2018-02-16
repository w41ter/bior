package test

import (
	"fmt"
	"testing"
	"time"
)

import "github.com/thinkermao/bior/simu/env"

// The tester generously allows solutions to complete elections in One second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestRaft_InitialElection(t *testing.T) {
	servers := 3
	env := envior.MakeEnviornment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: initial election ...\n")

	// is a leader elected?
	env.CheckOneLeader()

	// does the leader+term stay the same if there is no network failure?
	term1 := env.CheckTerms()
	time.Sleep(2 * RaftElectionTimeout)
	term2 := env.CheckTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	fmt.Printf("  ... Passed\n")
}

func TestRaft_ReElection(t *testing.T) {
	servers := 3
	env := envior.MakeEnviornment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test : election after network failure ...\n")

	leader1 := env.CheckOneLeader()

	// if the leader disconnects, a new One should be elected.
	env.Disconnect(leader1)
	env.CheckOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the old leader.
	env.Connect(leader1)
	leader2 := env.CheckOneLeader()

	// if there's no quorum, no leader should
	// be elected.
	env.Disconnect(leader2)
	env.Disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	env.CheckNoLeader()

	// if a quorum arises, it should elect a leader.
	env.Connect((leader2 + 1) % servers)
	env.CheckOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	env.Connect(leader2)
	env.CheckOneLeader()

	fmt.Printf("  ... Passed\n")
}
