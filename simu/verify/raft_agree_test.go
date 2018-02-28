package verify

import (
	"fmt"
	"testing"

	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/simu/env"
	"github.com/thinkermao/bior/simu/raft"
)

func TestRaft_BasicAgree(t *testing.T) {
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: basic agreement ...\n")

	iters := 6
	// one noop commit.
	istart := conf.InvalidIndex + 2
	for index := istart; index < iters+istart; index++ {
		nd, _ := env.CommittedNumber(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := env.One(index*100, servers)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestRaft_FailAgree(t *testing.T) {
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: agreement despite follower disconnection ...\n")

	env.One(101, servers)

	// follower network disconnection
	leader := env.CheckOneLeader()
	env.Disconnect((leader + 1) % servers)

	// agree despite one disconnected server?
	env.One(102, servers-1)
	env.One(103, servers-1)
	sleep(raft.ElectionTimeout)
	env.One(104, servers-1)
	env.One(105, servers-1)

	// re-connect
	env.Connect((leader + 1) % servers)

	// agree with full set of servers?
	env.One(106, servers)
	sleep(raft.ElectionTimeout)
	env.One(107, servers)

	fmt.Printf("  ... Passed\n")
}
