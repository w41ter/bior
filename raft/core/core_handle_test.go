package core

import (
	"testing"

	"github.com/thinkermao/bior/raft/proto"
)

// TODO: test state to follower need reset leaderId.
// accept:
// 	- Than electionTick did not receive the leader message
//	- CurrentTerm less than remote Term
// 	- Remote log is update
func TestCore_handlePreVote(t *testing.T) {
	cases := []struct {
		timeElapsed int
		msgTerm     uint64
		logIndex    uint64
		logTerm     uint64
		term        uint64
		entries     []raftpd.Entry
		want        bool
	}{
		// accept
		// {11, 1, 2, }
	}

	for i, test := range cases {
		cb := &appImpl{}
		c := makeTestRaft(1, []uint64{2, 3}, 10, 1,
			test.entries, cb, timeElapsed(test.timeElapsed), term(test.term))

		cb.sendCB = func(msg *raftpd.Message) {
			if msg.Reject != test.want {
				t.Fatalf("#%d: handle pre vote request want: %+v, get: %+v",
					i, test.want, msg.Reject)
			}
		}
		msg := raftpd.Message{
			MsgType:  raftpd.MsgPreVoteRequest,
			LogIndex: test.logIndex,
			LogTerm:  test.logTerm,
			Term:     test.msgTerm,
		}
		c.handlePreVote(&msg)
	}
}

func TestCore_handleVote(t *testing.T) {}

func TestCore_handleVoteResponse(t *testing.T) {}

func TestCore_sendHeartbeat(t *testing.T) {}

func TestCore_broadcastAppend(t *testing.T) {}

func TestCore_sendAppend(t *testing.T) {}

func TestCore_sendSnapshot(t *testing.T) {}

func TestCore_handleHeartbeat(t *testing.T) {}

func TestCore_handleHeartbeatResponse(t *testing.T) {}

func TestCore_handleAppendEntries(t *testing.T) {}

func TestCore_handleSnapshot(t *testing.T) {}
