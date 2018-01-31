package peer

// VoteState record node voting status.
type VoteState int

// Vote status
const (
	VoteNone VoteState = iota
	VoteReject
	VoteGranted
)

// State transfer graph.
//
// Default state => probe (m: 0, n: log.lastIdx)
//
// probe:
// 		send log entries (pause: true)
// 		unreachable (pause: false)
// 		receive append response (pause: false)
//			success: => replicate (m: n, n: n+1)
// 			failed: => probe (m: 0, n: max{1, min{rejectIdx, hintIdx+1}})
//			ignore on rejectIdx != n-1
// 		send snapshot => snapshot (p: log.snapshot.meta.idx)
//
// snapshot:
// 		receive snapshot response
//			success: => replicate (m: p, n: p + 1) (should be probe, because
// 						when receive response, leader may generate new snapshot)
//			failed: => probe (m: 0, n: p), because core will become follower if recieve
//					reject from follower, it mean that term is old.
//		unreachable => probe (m: 0, n: p)
//
// replicate:
// 		send log entries (size: {infs.left, log.lastIdx-n}, n: lastIndex send)
// 		unreachable => probe (n: m + 1)
// 		receive replicate response:
//			success (m: max{m, idx})
// 			failed => probe (n: min{rejectIdx, hintIdx})
//				failed may not exists, because Unreachable & becomeFollower eat this
//
type nodeState int

const (
	nodeStateProbe nodeState = iota
	nodeStateReplicate
	nodeStateSnapshot
)

var nodeStateString = []string{
	"Probe",
	"Replicate",
	"Snapshot",
}

func (state nodeState) String() string {
	return nodeStateString[state]
}

func defaultNodeState() nodeState {
	return nodeStateProbe
}
