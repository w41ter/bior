package verify

import (
	"fmt"
	"testing"

	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/simu/env"
	"github.com/thinkermao/bior/simu/raft"
)

func TestRaft_BasicPersistence(t *testing.T) {
	peer.Simulation = true
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: basic persistence ...\n")

	env.One(11, servers)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		env.Start1(i)
	}
	for i := 0; i < servers; i++ {
		env.Disconnect(i)
		env.Connect(i)
	}

	env.One(12, servers)

	leader1 := env.CheckOneLeader()
	env.Disconnect(leader1)
	env.Start1(leader1)
	env.Connect(leader1)

	env.One(13, servers)

	leader2 := env.CheckOneLeader()
	env.Disconnect(leader2)

	index := env.One(14, servers-1)
	env.Start1(leader2)
	env.Connect(leader2)

	env.Wait(index, servers, -1) // wait for leader2 to join before killing i3

	i3 := (env.CheckOneLeader() + 1) % servers
	env.Disconnect(i3)
	env.One(15, servers-1)
	env.Start1(i3)
	env.Connect(i3)

	env.One(16, servers)

	fmt.Printf("  ... Passed\n")
}

func TestRaft_MoreNodesCrashPersist(t *testing.T) {
	peer.Simulation = true
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: more persistence ...\n")

	index := 1
	for iters := 0; iters < 5; iters++ {
		env.One(10+index, servers)
		index++

		leader1 := env.CheckOneLeader()

		env.Disconnect((leader1 + 1) % servers)
		env.Disconnect((leader1 + 2) % servers)

		env.One(10+index, servers-2)
		index++

		env.Disconnect((leader1 + 0) % servers)
		env.Disconnect((leader1 + 3) % servers)
		env.Disconnect((leader1 + 4) % servers)

		env.Start1((leader1 + 1) % servers)
		env.Start1((leader1 + 2) % servers)
		env.Connect((leader1 + 1) % servers)
		env.Connect((leader1 + 2) % servers)

		sleep(2 * raft.ElectionTimeout)

		env.Start1((leader1 + 3) % servers)
		env.Connect((leader1 + 3) % servers)

		env.One(10+index, servers-2)
		index++

		env.Connect((leader1 + 4) % servers)
		env.Connect((leader1 + 0) % servers)
	}

	env.One(1000, servers)

	fmt.Printf("  ... Passed\n")
}

func TestRaft_PartitionedPersist(t *testing.T) {
	peer.Simulation = true
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: partitioned leader and one follower crash, leader restarts ...\n")

	env.One(101, 3)

	leader := env.CheckOneLeader()
	env.Disconnect((leader + 2) % servers)

	env.One(102, 2)

	env.Crash1((leader + 0) % servers)
	env.Crash1((leader + 1) % servers)
	env.Connect((leader + 2) % servers)
	env.Start1((leader + 0) % servers)
	env.Connect((leader + 0) % servers)

	env.One(103, 2)

	env.Start1((leader + 1) % servers)
	env.Connect((leader + 1) % servers)

	env.One(104, servers)

	fmt.Printf("  ... Passed\n")
}
