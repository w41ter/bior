package verify

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

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
			sleep(int(rand.Int63() % (raft.ElectionTimeout / 2)))
		} else {
			sleep(int(rand.Int63() % 13))
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

func TestRaft_Churn(t *testing.T) {
	type commit struct {
		index int
		value int
	}
	servers := 5
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: churn ...\n")

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []commit) {
		var ret []commit
		ret = nil
		defer func() { ch <- ret }()
		values := []commit{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				if !env.IsCrash(i) {
					index1, _, ok1 := env.Propose(i, x)
					if ok1 {
						ok = ok1
						index = int(index1)
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := env.CommittedNumber(index)
					if nd > 0 && cmd == x {
						values = append(values, commit{index: index, value: x})
						break
					}
					sleep(to)
				}
			} else {
				sleep(79 + me*17)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []commit{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []commit))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			env.Disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if env.IsCrash(i) {
				env.Start1(i)
			}
			env.Connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if !env.IsCrash(i) {
				env.Crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		sleep((raft.ElectionTimeout * 7) / 10)
	}

	sleep(raft.ElectionTimeout)
	// wake up all nodes
	for i := 0; i < servers; i++ {
		if env.IsCrash(i) {
			env.Start1(i)
		}
		env.Connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []commit{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	sleep(raft.ElectionTimeout)

	env.One(1, servers)

	for _, v1 := range values {
		if n, v := env.CommittedNumber(v1.index); v != v1.value || n != servers {
			t.Fatalf("didn't find a value")
		}
	}

	fmt.Printf("  ... Passed\n")
}
