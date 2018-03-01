package verify

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/thinkermao/bior/raft/core/peer"
	"github.com/thinkermao/bior/simu/env"
	"github.com/thinkermao/bior/simu/raft"
)

// TODO: why ?

func TestRaft_ConcurrentPropose(t *testing.T) {
	peer.Simulation = true
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: concurrent proposes ...\n")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := env.CheckOneLeader()
		_, term, ok := env.Propose(leader, 1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := env.Propose(leader, 100+i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- int(idx)
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := env.GetState(j); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := env.Wait(index, servers, int(term))
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	fmt.Printf("  ... Passed\n")
}

func TestRaft_NetworkCount(t *testing.T) {
	peer.Simulation = true
	servers := 3
	env := envior.MakeEnvironment(t, servers, false)
	defer env.Cleanup()

	fmt.Printf("Test: network counts aren't too high ...\n")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += env.GetCount(j)
		}
		return
	}

	leader := env.CheckOneLeader()

	total1 := rpcs()

	if total1 > 60 || total1 < 1 {
		t.Fatalf("too many or few RPCs :%v to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = env.CheckOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := env.Propose(leader, 1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := env.Propose(leader, x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+uint64(i) != index1 {
				t.Fatalf("Propose failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := env.Wait(int(starti)+i, servers, int(term))
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n",
					cmd, starti+uint64(i), cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := env.GetState(j); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += env.GetCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*6 {
			t.Fatalf("too many RPCs: %v for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(raft.ElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += env.GetCount(j)
	}

	if total3-total2 > 6*50 {
		t.Fatalf("too many RPCs: %v for 1 second of idleness\n", total3-total2)
	}

	fmt.Printf("  ... Passed\n")
}
