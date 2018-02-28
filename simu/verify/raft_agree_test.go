package verify

import (
	"fmt"
	"testing"

	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/simu/env"
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
