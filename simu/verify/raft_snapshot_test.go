package verify

import (
	"fmt"
	"testing"

	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/simu/env"
)

func TestRaft_RestartSnapshot(t *testing.T) {
	peer.Simulation = true

	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: restart with snapshot ...\n")

	for i := 0; i < 10; i++ {
		env.One(100+i, servers)
	}
	env.GenSnapshot(0)
	for i := 0; i < 10; i++ {
		env.One(110+i, servers)
	}

	// let node restart with snapshot.
	env.Crash1(0)
	env.Start1(0)
	env.Connect(0)

	env.One(120, servers)

	fmt.Printf("  ... Passed\n")
}

func TestRaft_CatchUpBySnapshot(t *testing.T) {
	peer.Simulation = true

	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: follower catch up ...\n")

	for i := 0; i < 10; i++ {
		env.One(100+i, servers)
	}

	leader := env.CheckOneLeader()
	follower := (leader + 1) % servers

	env.Disconnect(follower)

	for i := 0; i < 10; i++ {
		env.One(110+i, servers-1)
	}
	env.GenSnapshot(0)
	for i := 0; i < 10; i++ {
		env.One(120+i, servers-1)
	}

	// catch up
	env.Connect(follower)

	env.One(121, servers)

	fmt.Printf("  ... Passed\n")
}
