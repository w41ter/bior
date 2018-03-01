package verify

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/simu/env"
	"github.com/thinkermao/bior/simu/raft"
)

func TestRaft_BasicAgree(t *testing.T) {
	peer.Simulation = true
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
	peer.Simulation = true
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
	peer.Simulation = true
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

func TestRaft_RejoinAgree(t *testing.T) {
	peer.Simulation = true
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: rejoin of partitioned leader ...\n")

	env.One(101, servers)

	// leader network failure
	leader1 := env.CheckOneLeader()
	env.Disconnect(leader1)

	// make old leader try to agree on some entries
	env.Propose(leader1, 102)
	env.Propose(leader1, 103)
	env.Propose(leader1, 104)

	// new leader commits
	env.One(103, servers-1)

	// new leader network failure
	leader2 := env.CheckOneLeader()
	env.Disconnect(leader2)

	// old leader connected again
	env.Connect(leader1)

	// wait for new elected leader
	env.CheckOneLeader()

	env.One(104, servers-1)

	// all together now
	env.Connect(leader2)

	env.One(105, servers)

	fmt.Printf("  ... Passed\n")
}

func TestRaft_BackupAgree(t *testing.T) {
	peer.Simulation = true
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: leader backs up quickly over incorrect follower logs ...\n")

	env.One(rand.Int(), servers)

	// put leader and one follower in a partition
	leader1 := env.CheckOneLeader()
	env.Disconnect((leader1 + 2) % servers)
	env.Disconnect((leader1 + 3) % servers)
	env.Disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		env.Propose(leader1, rand.Int())
	}

	time.Sleep(raft.ElectionTimeout / 2)

	env.Disconnect((leader1 + 0) % servers)
	env.Disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	env.Connect((leader1 + 2) % servers)
	env.Connect((leader1 + 3) % servers)
	env.Connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		env.One(rand.Int(), 3)
	}

	// now another partitioned leader and one follower
	leader2 := env.CheckOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	env.Disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		env.Propose(leader2, rand.Int())
	}

	time.Sleep(raft.ElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		env.Disconnect(i)
	}
	env.Connect((leader1 + 0) % servers)
	env.Connect((leader1 + 1) % servers)
	env.Connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		env.One(rand.Int(), 3)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		env.Connect(i)
	}
	env.One(rand.Int(), servers)

	fmt.Printf("  ... Passed\n")
}

// Delay 300ms
func TestRaft_LongDelayAgree(t *testing.T) {
	peer.Simulation = true
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	env.SetLongDelay(true)
	defer env.Cleanup()

	fmt.Printf("Test: long delay network agreement ...\n")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				env.One((100*iters)+j, 1)
			}(iters, j)
		}
		env.One(iters, 1)
	}

	wg.Wait()

	env.One(100, servers)

	fmt.Printf("  ... Passed\n")
}
