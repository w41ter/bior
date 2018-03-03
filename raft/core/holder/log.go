package holder

import (
	"fmt"

	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"

	log "github.com/sirupsen/logrus"
)

// LogHolder provides structure to holder log entries,
// and given some useful information for raft.
// Here is the memory layout of LogHolder:
//
// [offset, lastApplied, commitIndex, stabled, lastIndex)
// +--------------+--------------+-------------+-------------+
// | wait compact |  wait apply  | wait commit | wait stable |
// +--------------+--------------+-------------+-------------+
// ^ offset       ^ Applied      ^ committed   ^ stabled     ^ last
//
// Notice:
// 	- stabled must great than commit index, but sometime
// we need to stable & send append parallel, so stabled will less
// than commitIndex sometimes. in order to keep consistency, lastApplied
// must less or equal to stabled.
// 	- there always has a dummy entry with empty data, it make
// the programming more easy, and we needn't offset field.
type LogHolder struct {
	// raft inner Id
	id uint64

	// last index of entry has been applied
	lastApplied uint64

	// last index of committed entry
	commitIndex uint64

	// last index stable to storage
	lastStabled uint64

	// buffered entries
	entries []raftpd.Entry
}

// MakeLogHolder create & initialize empty LogHolder, and returns.
func MakeLogHolder(id uint64, firstIndex uint64, firstTerm uint64) *LogHolder {
	log.Debugf("make log holder id: %d [idx: %d, term: %d]", id, firstIndex, firstTerm)

	// make dummy entry.
	entries := make([]raftpd.Entry, 1)
	entries[0].Type = raftpd.EntryNormal
	entries[0].Index = firstIndex
	entries[0].Term = firstTerm
	return &LogHolder{
		id:          id,
		entries:     entries,
		lastApplied: firstIndex,
		commitIndex: firstIndex,
		lastStabled: firstIndex,
	}
}

// RebuildLogHolder construction log holder from exists log Entries.
// It required the first entry must be last applied entry from
// state machine, and also len(Entries) must great than zero.
func RebuildLogHolder(id uint64, entries []raftpd.Entry) *LogHolder {
	utils.Assert(len(entries) != 0, "required entries not empty")

	firstIndex := entries[0].Index
	firstTerm := entries[0].Term
	lastStabled := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term

	log.Debugf("%d rebuild log holder [idx: %d-%d, term: %d-%d]",
		id, firstIndex, lastStabled, firstTerm, lastTerm)

	// copy make unique constraint.
	dup := make([]raftpd.Entry, len(entries))
	copy(dup, entries)

	return &LogHolder{
		id:          id,
		entries:     dup,
		lastApplied: firstIndex,
		commitIndex: firstIndex,
		lastStabled: lastStabled,
	}
}

// Term return the Term of idx, if there no entry
// with these index, return InvalidTerm.
func (holder *LogHolder) Term(idx uint64) uint64 {
	lastIndex := holder.LastIndex()
	dummyIdx := holder.offset()
	if idx < dummyIdx || idx > lastIndex {
		return conf.InvalidTerm
	}
	return holder.entries[idx-dummyIdx].Term
}

// Slice return the Entries between [lo, hi), no included dummy entry.
func (holder *LogHolder) Slice(lo, hi uint64) []raftpd.Entry {
	holder.checkOutOfBounds(lo, hi)
	offset := holder.offset()
	l := lo - offset
	r := hi - offset
	entries := holder.entries[l:r]

	if len(entries) != 0 {
		utils.Assert(entries[0].Index == lo, "error index")
		utils.Assert(entries[len(entries)-1].Index == hi-1, "error index")
	}
	return entries
}

// IsUpToDate determines if the given (idx,term) log is more up-to-date
// by comparing the index and term of the last entry in the existing logs.
// If the logs have last entry with different terms, then the log with the
// later term is more up-to-datholder. If the logs end with the same term, then
// whichever log has the larger last_index is more up-to-datholder. If the logs are
// the same, the given log is up-to-datholder.
// Log Matching: if two logs contain an entry with the same index and term,
// then the logs are identical in all entries up through the given index. §5.3
func (holder *LogHolder) IsUpToDate(idx, term uint64) bool {
	return term > holder.LastTerm() || (term == holder.LastTerm() && idx >= holder.LastIndex())
}

// LastIndex return the last index of current Entries,
// it require `len(Entries)` great than zero, otherwise panic.
func (holder *LogHolder) LastIndex() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	length := len(holder.entries)
	actual := holder.entries[length-1].Index
	get := holder.offset() + uint64(length) - 1
	utils.Assert(actual == get, "bad Entries")
	return get
}

// FirstIndex return the first available entry in current holder.
func (holder *LogHolder) FirstIndex() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	return holder.offset() + 1
}

// LastTerm return the last term of current Entries.
func (holder *LogHolder) LastTerm() uint64 {
	return holder.Term(holder.LastIndex())
}

// CommitIndex return holder.commitIndex.
func (holder *LogHolder) CommitIndex() uint64 {
	return holder.commitIndex
}

// CompactTo drain all entries which has lower idx.
func (holder *LogHolder) CompactTo(to, term uint64) {
	// May need to rebuild the log, when recovering from a snapshot.
	// The following situations may occur:
	// 	- log entry conflict with exists
	// 	- to less than offset
	// 	- great than applied
	if holder.Term(to) != term || to <= holder.offset() || to > holder.lastApplied {
		log.Debugf("%d compact and rebuild : %d, term: %d", holder.id, to, term)
		entries := make([]raftpd.Entry, 1)
		entries[0].Index = to
		entries[0].Term = term
		holder.entries = entries
		holder.lastApplied = to
		holder.commitIndex = to
		holder.lastStabled = to
	} else {
		log.Debugf("%d compact to: %d, term: %d", holder.id, to, term)
		offset := holder.offset()
		utils.Assert(offset <= to, "%d compact idx: %d less than first index: %d",
			holder.id, to, offset)
		holder.entries = drain(holder.entries, int(to-offset))
	}
}

// CommitTo change commitIndex to `to`.
func (holder *LogHolder) CommitTo(to uint64) {
	if holder.commitIndex >= to {
		/* never decrease commit */
		return
	} else if holder.lastStabled < to {
		/* cannot commit unstable log entry */
		to = utils.MinUint64(to, holder.lastStabled)
	}

	utils.Assert(holder.LastIndex() >= to,
		"%d toCommit %d is out of range [last index: %d]",
		holder.id, to, holder.LastIndex())

	holder.commitIndex = to

	log.Debugf("%d commit entries to index: %d", holder.id, to)
}

// ApplyEntries change lastApplied to `to`, require to
// great than lastApplied, and less than or equals to commitIndex.
// It return the entries need to apply to state machine. Because
// stabled may less than commit, so lastApplied will be
// `min(commit, stabled)` after execution.
func (holder *LogHolder) ApplyEntries() []raftpd.Entry {
	//utils.Assert(holder.lastApplied < to && to <= holder.commitIndex,
	//	"%d apply to must in range (%d, %d]", holder.id, holder.lastApplied, holder.commitIndex)
	target := utils.MinUint64(holder.commitIndex, holder.lastStabled)

	if holder.lastApplied != target {
		log.Debugf("%d apply entries to index: %d", holder.id, target)
	}

	result := holder.Slice(holder.lastApplied+1, target+1)
	holder.lastApplied = target

	return result
}

// StableEntries mark all entries[stable:] as stabled,
// and return the entries need to stabled.
func (holder *LogHolder) StableEntries() []raftpd.Entry {
	lastStabled := holder.lastStabled
	lastIndex := holder.LastIndex()
	utils.Assert(lastStabled <= lastIndex,
		fmt.Sprintf("%d stabled: %d, lastIndex: %d",
			holder.id, lastStabled, lastIndex))

	entries := holder.Slice(lastStabled+1, lastIndex+1)
	holder.lastStabled = lastIndex
	return entries
}

// TryAppend check whether log is valid. if valid, it append entries,
// and returns the lastIndexOfEntries, otherwise return hinted log index.
func (holder *LogHolder) TryAppend(prevIdx, prevTerm uint64,
	entries []raftpd.Entry) (uint64, bool) {
	if holder.Term(prevIdx) == prevTerm {
		conflictIdx := holder.findConflict(entries)
		if conflictIdx == 0 {
			/* success, no conflict */
		} else if conflictIdx <= holder.commitIndex {
			log.Panicf("%d entry %d conflict with committed entry %d",
				holder.id, conflictIdx, holder.commitIndex)
		} else {
			offset := prevIdx + 1
			holder.truncateAndAppend(entries[conflictIdx-offset:])
		}

		return holder.LastIndex(), true
	}

	utils.Assert(prevIdx >= holder.commitIndex,
		"%d entry %d [Term: %d] conflict with committed entry Term: %d",
		holder.id, prevIdx, prevTerm, holder.Term(prevIdx))

	return holder.getHintIndex(prevIdx, prevTerm), false
}

// Append push entries at back, and return the new last index.
func (holder *LogHolder) Append(entries []raftpd.Entry) uint64 {
	if len(entries) == 0 {
		return holder.LastIndex()
	}

	prevIndex := entries[0].Index - 1
	utils.Assert(prevIndex >= holder.commitIndex,
		"%d after %d is out of range [committed: %d]",
		holder.id, prevIndex, holder.commitIndex)

	holder.entries = append(holder.entries, entries...)
	return holder.LastIndex()
}
