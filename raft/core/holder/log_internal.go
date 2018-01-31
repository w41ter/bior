package holder

import (
	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft/core/conf"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils"
)

func (holder *LogHolder) checkOutOfBounds(lo, hi uint64) {
	utils.Assert(lo <= hi, "%d invalid slice %d > %d", holder.id, lo, hi)

	lower := holder.FirstIndex()
	upper := holder.LastIndex() + 1
	utils.Assert(!(lo < lower || hi > upper),
		"%d slice[%d, %d] out of bound[%d, %d]",
		holder.id, lo, hi, lower, upper)
}

func (holder *LogHolder) truncateAndAppend(entries []raftpd.Entry) {
	if len(entries) == 0 {
		return
	}

	lastIndex := holder.LastIndex()
	after := entries[0].Index
	if after == lastIndex+1 {
		// after is the next index in the self.Entries, append directly
	} else if after <= holder.offset() {
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the Entries
		holder.entries = make([]raftpd.Entry, 0)
	} else {
		holder.checkOutOfBounds(holder.FirstIndex(), after)
		holder.entries = holder.entries[:after-holder.offset()]
	}
	holder.entries = append(holder.entries, entries...)

	holder.validateConsistency()
}

// findConflict return the first index which Entries[i].Term is not equal
// to `holder.Term(Entries[i].Index)`, if all Term with same index are equals,
// return zero.
func (holder *LogHolder) findConflict(entries []raftpd.Entry) uint64 {
	for i := 0; i < len(entries); i++ {
		entry := &entries[i]
		if holder.Term(entry.Index) != entry.Term {
			if entry.Index <= holder.LastIndex() {
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
	utils.Assert(prevIdx != conf.InvalidIndex && prevTerm != conf.InvalidTerm,
		"%d get hint index with invalid idx or Term", holder.id)

	idx := prevIdx
	term := holder.Term(idx)
	for idx > conf.InvalidIndex {
		if holder.Term(idx) != term {
			return utils.MaxUint64(holder.commitIndex, idx)
		}
		idx--
	}
	return holder.commitIndex
}

// offset return the dummy entry's index.
func (holder *LogHolder) offset() uint64 {
	utils.Assert(len(holder.entries) != 0, "require len(holder.Entries) great than zero")
	return holder.entries[0].Index
}

func (holder *LogHolder) validateConsistency() {
	if len(holder.entries) > 0 {
		for i := 0; i < len(holder.entries)-1; i++ {
			utils.Assert(holder.entries[i].Index+1 == holder.entries[i+1].Index,
				"%d index:%d at:%d not sequences", holder.id, holder.entries[i].Index, i)
		}
	}
}

// drain like memmov(entries, entreis + to, len).
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
