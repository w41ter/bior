package core

import (
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/log"
)

// [offset, lastApplied, commitIndex, stabled, lastIndex)
// +--------------+--------------+-------------+-------------+
// | wait compact |  wait apply  | wait commit | wait stable |
// +--------------+--------------+-------------+-------------+
// ^ offset       ^ Applied      ^ committed   ^ stabled     ^ last
//
// notice: stabled must great than commit index, but sometime
// we need to stable & send append parallel, so stabled will less
// than commitIndex sometimes. in order to keep consistency, lastApplied
// must less or equal to stabled.
//
// notice: there always has a dummy entry with empty data, it make
// the programming more easy, and we needn't offset field.
type LogHolder struct {
	id          uint64 // raft inner Id
	entries     []raftpd.Entry
	lastApplied uint64 // last index of entry has been applied
	commitIndex uint64 // last index of committed entry
	lastStabled uint64 // last index stable to storage
}

func MakeLogHolder(id uint64, firstIndex uint64, firstTerm uint64) *LogHolder {
	entries := make([]raftpd.Entry, 1)
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

// RebuildLogHolder construction log holder from exists log Entries,
// it required the first entry must be last applied entry from
// state machine, and also len(Entries) must great than zero.
func RebuildLogHolder(id uint64, entries []raftpd.Entry) *LogHolder {
	utils.Assert(len(entries) != 0, "required len(Entries) great than zero")

	dup := make([]raftpd.Entry, len(entries))
	copy(dup, entries)
	firstIndex := entries[0].Index
	return &LogHolder{
		id:          id,
		entries:     dup,
		lastApplied: firstIndex,
		commitIndex: firstIndex,
		lastStabled: firstIndex + (uint64)(len(entries)),
	}
}

// Term return the Term of idx, if there no entry
// with these index, return InvalidTerm.
func (holder *LogHolder) Term(idx uint64) uint64 {
	lastIndex := holder.lastIndex()
	dummyIdx := holder.offset()
	if idx < dummyIdx || idx > lastIndex {
		return InvalidTerm
	} else {
		return holder.entries[idx-dummyIdx].Term
	}
}

func (holder *LogHolder) CompactTo(to, term uint64) {
	if holder.Term(to) != term || to <= holder.offset() || to > holder.lastApplied {
		// log entry conflict with exists, or less than offset, or great than applied
		// so need to rebuild log
		entries := make([]raftpd.Entry, 1)
		entries[0].Index = to
		entries[0].Term = term
		holder.entries = entries
		holder.lastApplied = to
		holder.commitIndex = to
		holder.lastStabled = to
	} else {
		offset := holder.offset()
		utils.Assert(offset <= to, "%d compact idx: %d less than first index: %d",
			holder.id, to, offset)
		holder.entries = drain(holder.entries, int(to-offset))
	}
}

// ApplyTo change lastApplied to `to`, require to great than lastApplied,
// and less than or equals to commitIndex.
// it return the entries need to apply to state machine. because
// stabled may less than commit, so lastApplied will be min(commit, stabled)
// after execution.
func (holder *LogHolder) ApplyEntries() []raftpd.Entry {
	//utils.Assert(holder.lastApplied < to && to <= holder.commitIndex,
	//	"%d apply to must in range (%d, %d]", holder.id, holder.lastApplied, holder.commitIndex)
	target := utils.MinUint64(holder.commitIndex, holder.lastStabled)
	result := holder.Slice(holder.lastApplied+1, target+1)
	holder.lastApplied = target
	return result
}

func (holder *LogHolder) CommitTo(to uint64) {
	if holder.commitIndex >= to {
		/* never decrease commit */
		return
	} else if holder.lastStabled < to {
		/* cannot commit unstable log entry */
		to = utils.MinUint64(to, holder.lastStabled)
	}

	utils.Assert(holder.lastIndex() >= to, "%d toCommit %d is out of range [last index: %d]",
		holder.id, to, holder.lastIndex())

	holder.commitIndex = to
}

// StableEntries mark all entries[stable:] as stabled,
// and return the entries need to stabled.
func (holder *LogHolder) StableEntries() []raftpd.Entry {
	//utils.Assert(holder.commitIndex <= to, "%d try decrease stableTo less than commit", holder.id)

	entries := holder.Slice(holder.lastStabled+1, holder.lastIndex()+1)
	holder.lastStabled = holder.lastIndex()
	return entries
}

func (holder *LogHolder) TryAppend(prevIdx, prevTerm uint64,
	entries []raftpd.Entry) (uint64, bool) {
	lastIdxOfEntries := prevIdx + (uint64)(len(entries))
	if holder.Term(prevIdx) == prevTerm {
		conflictIdx := holder.findConflict(entries)
		if conflictIdx == 0 {
			/* success, no conflict */
		} else if conflictIdx <= holder.commitIndex {
			log.Panicf("%d entry %d conflict with committed entry %d",
				holder.id, conflictIdx, holder.commitIndex)
		} else {
			offset := prevIdx + 1
			holder.Append(entries[conflictIdx-offset:])
		}

		//holder.CommitTo(utils.MinUint64(leaderCommittedIdx, lastIdxOfEntries))

		return lastIdxOfEntries, true
	} else {
		utils.Assert(prevIdx > holder.commitIndex,
			"%d entry %d [Term: %d] conflict with committed entry Term: %d",
			holder.id, prevIdx, prevTerm, holder.Term(prevIdx))

		return holder.getHintIndex(prevIdx, prevTerm), false
	}
}

func (holder *LogHolder) Append(entries []raftpd.Entry) uint64 {
	if len(entries) == 0 {
		return holder.lastIndex()
	}

	prevIndex := entries[0].Index - 1
	utils.Assert(prevIndex >= holder.commitIndex,
		"%d after %d is out of range [committed: %d]",
		holder.id, prevIndex, holder.commitIndex)

	holder.truncateAndAppend(entries)
	return holder.lastIndex()
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

// is_up_to_date determines if the given (lastIndex,Term) log is more up-to-date
// by comparing the index and Term of the last entry in the existing logs.
// If the logs have last entry with different terms, then the log with the
// later Term is more up-to-datholder. If the logs end with the same Term, then
// whichever log has the larger last_index is more up-to-datholder. If the logs are
// the same, the given log is up-to-datholder.
func (holder *LogHolder) IsUpToDate(idx, term uint64) bool {
	return term > holder.lastTerm() || (term == holder.lastTerm() && idx >= holder.lastIndex())
}

func (holder *LogHolder) checkOutOfBounds(lo, hi uint64) {
	utils.Assert(lo <= hi, "%d invalid slice %d > %d", holder.id, lo, hi)

	lower := holder.firstIndex()
	upper := holder.lastIndex() + 1
	utils.Assert(!(lo < lower || hi > upper),
		"%d slice[%d, %d] out of bound[%d, %d]",
		holder.id, lo, hi, lower, upper)
}

func (holder *LogHolder) truncateAndAppend(entries []raftpd.Entry) {
	if len(entries) == 0 {
		return
	}

	lastIndex := holder.lastIndex()
	after := entries[0].Index
	if after == lastIndex+1 {
		// after is the next index in the self.Entries, append directly
	} else if after <= holder.offset() {
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the Entries
		holder.entries = make([]raftpd.Entry, 0)
	} else {
		holder.checkOutOfBounds(holder.firstIndex(), after)
		holder.entries = holder.entries[:after-holder.offset()]
	}
	holder.entries = append(holder.entries, entries...)

	holder.validateConsistency()
}

// findConflict return the first index which Entries[i].Term is not equal
// to holder.Term(Entries[i].Index), if all Term with same index are equals,
// return zero.
func (holder *LogHolder) findConflict(entries []raftpd.Entry) uint64 {
	for i := 0; i < len(entries); i++ {
		entry := &entries[i]
		if holder.Term(entry.Index) != entry.Term {
			if entry.Index <= holder.lastIndex() {
				log.Infof("%d found conflict at index %d, "+
					"[existing Term: %d, conflicting Term: %d]",
					holder.id, entry.Index, holder.Term(entry.Index), entry.Term)
			}
			return entry.Index
		}
	}
	return 0
}

func (holder *LogHolder) getHintIndex(prevIdx, prevTerm uint64) uint64 {
	utils.Assert(prevIdx != InvalidIndex && prevTerm != InvalidTerm,
		"%d get hint index with invalid idx or Term", holder.id)

	idx := prevIdx
	term := holder.Term(idx)
	for idx > InvalidIndex {
		if holder.Term(idx) != term {
			return utils.MaxUint64(holder.commitIndex, idx)
		}
		idx--
	}
	return holder.commitIndex
}

// lastIndex return the last index of current Entries,
// it require len(Entries) great than zero, otherwise panic.
func (holder *LogHolder) lastIndex() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	length := len(holder.entries)
	actual := holder.entries[length-1].Index
	get := holder.offset() + uint64(length) - 1
	utils.Assert(actual == get, "bad Entries")
	return get
}

func (holder *LogHolder) lastTerm() uint64 {
	return holder.Term(holder.lastIndex())
}

// offset return the dummy entry's index
func (holder *LogHolder) offset() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	return holder.entries[0].Index
}

// firstIndex return the first available entry in current holder
func (holder *LogHolder) firstIndex() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	return holder.offset() + 1
}

func (holder *LogHolder) validateConsistency() {
	if len(holder.entries) > 0 {
		for i := 0; i < len(holder.entries)-1; i++ {
			utils.Assert(holder.entries[i].Index+1 == holder.entries[i+1].Index,
				"%d index:%d at:%d not sequences", holder.id, holder.entries[i].Index, i)
		}
	}
}

// drain like memmov(entries, entreis + to, len)
func drain(entries []raftpd.Entry, to int) []raftpd.Entry {
	if len(entries) == 0 {
		return entries
	}

	offset := to
	length := len(entries) - to
	for i := 0; i < length; i++ {
		entries[i] = entries[i+offset]
	}
	entries = entries[:length]
	return entries
}
