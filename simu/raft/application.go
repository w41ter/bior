package raft

// Application provides a raft implements.
type Application interface {
	ID() int
	Kill()
	Start(nodes []uint64) error
	Shutdown()
	IsCrash() bool
	Propose(data int) (uint64, uint64, bool)
	GenSnapshot() (uint64, uint64)

	GetState() (uint64, bool)
	ApplyError() error

	LogLength() int
	LogAt(index int) (int, bool)
}
