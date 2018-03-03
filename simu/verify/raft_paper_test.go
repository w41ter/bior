package verify

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/thinkermao/bior/simu/env"
	"github.com/thinkermao/bior/simu/raft"
)

func TestRaft_PaperFigure8(t *testing.T) {
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: extend raft paper figure 8 ...\n")

	env.One(1, 1)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		// find a leader, and propose.
		for i := 0; i < servers; i++ {
			if !env.IsCrash(i) {
				if _, _, ok := env.Propose(i, (10+i)*1000+iters); ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (raft.ElectionTimeout / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		// if leader exists, crash it.
		if leader != -1 {
			env.Crash1(leader)
			nup -= 1
		}

		// connect one if alive node less three.
		if nup < 3 {
			s := rand.Int() % servers
			if env.IsCrash(s) {
				env.Start1(s)
				env.Connect(s)
				nup += 1
			}
		}
	}

	// wake up all nodes.
	for i := 0; i < servers; i++ {
		if env.IsCrash(i) {
			env.Start1(i)
			env.Connect(i)
		}
	}

	env.One(2, servers)

	fmt.Printf("  ... Passed\n")
}
