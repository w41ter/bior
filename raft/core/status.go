package core

// SoftState gives some raft runtime information.
type SoftState struct {
	// LeaderID return current node's leader ID.
	LeaderID uint64
	// State return current node's state role.
	State StateRole
	// LastIndex return current node's index of last entry.
	LastIndex uint64
}

// StateRole said the state role of raft.
type StateRole int

// Role enum constants.
const (
	RoleFollower StateRole = iota
	RoleLeader
	RoleCandidate
	RolePrevCandidate
)

var stateRoleString = []string{
	"Follower",
	"Leader",
	"Candidate",
	"PrevCandidate",
}

func (role StateRole) String() string {
	return stateRoleString[role]
}

// IsLeader test whether role is leader.
func (role StateRole) IsLeader() bool {
	return role == RoleLeader
}

// IsCandidate test whether role is candidate.
func (role StateRole) IsCandidate() bool {
	return role == RoleCandidate
}

// IsFollower test whether role is follower.
func (role StateRole) IsFollower() bool {
	return role == RoleFollower
}

// IsPrevCandidate test whether role is prev candidate.
func (role StateRole) IsPrevCandidate() bool {
	return role == RolePrevCandidate
}
