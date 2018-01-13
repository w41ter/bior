package core

type StateRole int

const (
	FOLLOWER  StateRole = 0
	CANDIDATE StateRole = 1
	LEADER    StateRole = 2
	PRE_CANDIDATE StateRole = 3
)

func (role StateRole) String() string {
	switch role {
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	case LEADER:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (role StateRole) IsLeader() bool {
	return role == LEADER
}

func (role StateRole) IsCandidate() bool {
	return role == CANDIDATE
}

func (role StateRole) IsFollower() bool {
	return role == FOLLOWER
}

