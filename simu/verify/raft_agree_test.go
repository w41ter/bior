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

func TestRaft_FailNoAgree(t *testing.T) {
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: no agreement if too many followers disconnect ...\n")

	env.One(10, servers)

	// 3 of 5 followers disconnect
	leader := env.CheckOneLeader()
	env.Disconnect((leader + 1) % servers)
	env.Disconnect((leader + 2) % servers)
	env.Disconnect((leader + 3) % servers)

	var index1 int
	if index, _, ok := env.Propose(leader, 20); !ok {
		t.Fatalf("leader rejected propose 20.")
	} else {
		sleep(2 * raft.ElectionTimeout)
		if n, _ := env.CommittedNumber(int(index)); n > 0 {
			t.Fatalf("%v committed but no quorum", n)
		}
		index1 = int(index)
	}

	// repair
	env.Connect((leader + 1) % servers)
	env.Connect((leader + 2) % servers)
	env.Connect((leader + 3) % servers)

	leader2 := env.CheckOneLeader()
	if index, _, ok := env.Propose(leader2, 30); !ok {
		t.Fatalf("leader2 rejected propose 30")
	} else if index1 >= int(index) {
		t.Fatalf("unexpected index %v", index)
	}

	env.One(1000, servers)

	fmt.Printf("  ... Passed\n")
}
