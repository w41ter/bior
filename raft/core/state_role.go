package core

type StateRole int

const (
	FOLLOWER      StateRole = 0
	CANDIDATE     StateRole = 1
	LEADER        StateRole = 2
	PRE_CANDIDATE StateRole = 3
)

var stateRoleString = []string{
	"Follower",
	"Candidate",
	"Leader",
	"PreCandidate",
}

func (role StateRole) String() string {
	return stateRoleString[role]
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
