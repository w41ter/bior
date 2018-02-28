package holder

import (
	"testing"

	"github.com/thinkermao/bior/raft/proto"
)

func makeEntry(idx, term uint64) raftpd.Entry {
	return raftpd.Entry{
		Index: idx,
		Term:  term,
	}
}

func makeEntries(idxs ...uint64) []raftpd.Entry {
	entries := []raftpd.Entry{}
	for _, i := range idxs {
		entries = append(entries, makeEntry(i, i))
	}
	return entries
}

func compareEntry(a, b raftpd.Entry) bool {
	return a.Term == b.Term && a.Index == b.Index
}

func compareEntries(t *testing.T, i int, a, want []raftpd.Entry) {
	if len(a) != len(want) {
		t.Fatalf("#%d: len(entries) want: %d, get: %d",
			i, len(want), len(a))
	}
	for j := 0; j < len(a); j++ {
		if !compareEntry(a[j], want[j]) {
			t.Fatalf("#%d: ents[%d] want: %v, get: %v",
				i, j, want[j], a[j])
		}
	}
}

func TestMakeLogHolder(t *testing.T) {
	tests := []LogHolder{
		{1, 1, 1, 1, []raftpd.Entry{makeEntry(1, 1)}},
		{1, 2, 2, 2, []raftpd.Entry{makeEntry(2, 2)}},
	}

	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		e := MakeLogHolder(test.id, test.entries[0].Index, test.entries[0].Term)
		if e.id != test.id ||
			e.lastStabled != test.lastStabled ||
			e.lastApplied != test.lastApplied ||
			e.CommitIndex() != test.CommitIndex() ||
			len(e.entries) != len(test.entries) ||
			e.entries[0].Index != test.entries[0].Index ||
			e.entries[0].Term != test.entries[0].Term {
			t.Fatalf("#%d: make log holder failed", i)
		}
	}
}

func TestRebuildLogHolder(t *testing.T) {
	type param struct {
		entries   []raftpd.Entry
		id        uint64
		stable    uint64
		apply     uint64
		committed uint64
	}

	tests := []param{
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}, 0x1, 3, 1, 1},
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(3, 3)}, 0x1, 3, 1, 1},
	}

	for i, test := range tests {
		holder := RebuildLogHolder(test.id, test.entries)
		if holder.lastApplied != test.apply {
			t.Fatalf("#%d: last applied want: %d, but get: %d",
				i, test.apply, holder.lastApplied)
		}

		if holder.lastStabled != test.stable {
			t.Fatalf("#%d: last stabled want: %d, but get: %d",
				i, test.stable, holder.lastStabled)
		}

		if holder.CommitIndex() != test.committed {
			t.Fatalf("#%d: commit index want: %d, but get: %d",
				i, test.committed, holder.CommitIndex())
		}
	}
}

func TestLogHolder_Append(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}
	tests := []struct {
		entries []raftpd.Entry
		widx    uint64
		wents   []raftpd.Entry
	}{
		// empty
		{[]raftpd.Entry{}, 3, []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}},
		// non-empty
		{[]raftpd.Entry{makeEntry(4, 4)}, 4, []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4)}},
	}

	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		e := RebuildLogHolder(1, prevEntries)
		idx := e.Append(test.entries)
		if idx != test.widx {
			t.Fatalf("#%d: last_index = %d, want %d", i, idx, test.widx)
		}
		for j := range e.entries {
			en := e.entries[j]
			if en.Term != test.wents[j].Term || en.Index != test.wents[j].Index {
				t.Fatalf("#%d: entry not expect %d [%d Term: %d] != [%d Term: %d]",
					i, j, en.Index, en.Term, test.wents[j].Index, test.wents[j].Term)
			}
		}
	}
}

func TestLogHolder_ApplyEntries(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}
	tests := []struct {
		commit, apply, stable uint64
		wants                 []raftpd.Entry
	}{
		{3, 3, 3, []raftpd.Entry{}},
		{3, 1, 2, []raftpd.Entry{makeEntry(2, 2)}},
		{2, 1, 3, []raftpd.Entry{makeEntry(2, 2)}},
		{3, 1, 3, []raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3)}},
		{3, 2, 3, []raftpd.Entry{makeEntry(3, 3)}},
	}

	for i, test := range tests {
		e := RebuildLogHolder(1, prevEntries)
		e.commitIndex = test.commit
		e.lastApplied = test.apply
		e.lastStabled = test.stable
		ents := e.ApplyEntries()
		compareEntries(t, i, ents, test.wants)
	}
}

func TestLogHolder_CommitTo(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}
	e := RebuildLogHolder(1, prevEntries)
	e.CommitTo(3)
	e.CommitTo(2)
	e.CommitTo(4)
}

func TestLogHolder_CompactTo(t *testing.T) {
	prevEntries := []raftpd.Entry{
		makeEntry(2, 2),
		makeEntry(3, 3),
		makeEntry(4, 4),
	}

	tests := []struct {
		idx, term uint64
		wants     []raftpd.Entry
	}{
		// conflict
		{2, 3, []raftpd.Entry{makeEntry(2, 3)}},
		// less
		{1, 1, []raftpd.Entry{makeEntry(1, 1)}},
		// great
		{5, 5, []raftpd.Entry{makeEntry(5, 5)}},
		// normal
		{3, 3, []raftpd.Entry{makeEntry(3, 3), makeEntry(4, 4)}},
	}

	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		e := RebuildLogHolder(1, prevEntries)
		e.commitIndex = 3
		e.lastApplied = 3
		e.CompactTo(test.idx, test.term)
		compareEntries(t, i, e.entries, test.wants)
	}
}

func TestLogHolder_IsUpToDate(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}
	e := RebuildLogHolder(1, prevEntries)
	tests := []struct {
		idx    uint64
		term   uint64
		result bool
	}{
		// greater term, ignore lastIndex
		{e.LastIndex() - 1, 4, true},
		{e.LastIndex(), 4, true},
		{e.LastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{e.LastIndex() - 1, 2, false},
		{e.LastIndex(), 2, false},
		{e.LastIndex() + 1, 2, false},
		// equal term, lager lastIndex wins
		{e.LastIndex() - 1, 3, false},
		{e.LastIndex(), 3, true},
		{e.LastIndex() + 1, 3, true},
	}
	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		result := e.IsUpToDate(test.idx, test.term)
		if result != test.result {
			t.Fatalf("#%d: uptodate = %v, want %v", i, result, test.result)
		}
	}
}

func TestLogHolder_Slice(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4)}
	tests := []struct {
		lo    uint64
		hi    uint64
		wents []raftpd.Entry
	}{
		{2, 3, []raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3)}},
		{2, 2, []raftpd.Entry{}},
		{2, 4, []raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4)}},
	}

	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		e := RebuildLogHolder(1, prevEntries)
		entries := e.Slice(test.lo, test.hi)
		for j := range entries {
			en := entries[j]
			if en.Term != test.wents[j].Term || en.Index != test.wents[j].Index {
				t.Fatalf("#%d: entry not expect %d [%d Term: %d] != [%d Term: %d]",
					i, j, en.Index, en.Term, test.wents[j].Index, test.wents[j].Term)
			}
		}
	}
}

func TestLogHolder_StableEntries(t *testing.T) {
	prevEntries := []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}
	tests := []struct {
		stable uint64
		wants  []raftpd.Entry
	}{
		{3, []raftpd.Entry{}},
		{2, []raftpd.Entry{makeEntry(3, 3)}},
		{1, []raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3)}},
	}

	for i, test := range tests {
		e := RebuildLogHolder(1, prevEntries)
		e.lastStabled = test.stable
		ents := e.StableEntries()
		compareEntries(t, i, ents, test.wants)
	}
}

func TestLogHolder_Term(t *testing.T) {
	offset, num := uint64(100), uint64(100)

	entries := make([]raftpd.Entry, 0)
	for i := uint64(0); i < num; i++ {
		entries = append(entries, makeEntry(offset+i, i+1))
	}

	e := RebuildLogHolder(1, entries)

	tests := []struct {
		index uint64
		term  uint64
	}{
		{offset - 1, 0},
		{offset, 1},
		{offset + num/2, num/2 + 1},
		{offset + num - 1, num},
		{offset + num, 0},
	}

	for i := 0; i < len(tests); i++ {
		term := e.Term(tests[i].index)
		if term != tests[i].term {
			t.Fatalf("#%d: at = %d, want = %d, get = %d",
				i, tests[i].index, tests[i].term, term)
		}
	}
}

func TestLogHolder_TryAppend(t *testing.T) {
	tests := []struct {
		origin          []raftpd.Entry
		entries         []raftpd.Entry
		prvIdx, prvTerm uint64
		wents           []raftpd.Entry
		widx            uint64
		wres            bool
	}{
		// empty
		{makeEntries(1, 2), makeEntries(), 2, 2, makeEntries(1, 2), 2, true},
		// append
		{makeEntries(1), makeEntries(2), 1, 1, makeEntries(1, 2), 2, true},
		// truncate and append
		{makeEntries(1, 2, 3), makeEntries(3), 1, 1, makeEntries(1, 2, 3), 2, true}, // should be 3
		// conflict
		//{makeEntries(1), makeEntries(), 1, 2, makeEntries(1), conf.InvalidIndex, false},
	}

	for i, test := range tests {
		holder := RebuildLogHolder(1, test.origin)
		idx, res := holder.TryAppend(test.prvIdx, test.prvTerm, test.entries)
		compareEntries(t, i, holder.entries, test.wents)
		if idx != test.widx && res != test.wres {
			t.Fatalf("error")
		}
	}
}

func TestDrain(t *testing.T) {
	type param struct {
		entries []raftpd.Entry
		to      int
		want    []raftpd.Entry
	}

	tests := []param{
		{[]raftpd.Entry{}, 0, []raftpd.Entry{}},
		{[]raftpd.Entry{makeEntry(1, 1)}, 0, []raftpd.Entry{makeEntry(1, 1)}},
		{[]raftpd.Entry{makeEntry(1, 1)}, 1, []raftpd.Entry{}},
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 1)}, 1, []raftpd.Entry{makeEntry(2, 1)}},
	}

	for i := 0; i < len(tests); i++ {
		test := &tests[i]
		entries := drain(test.entries, test.to)

		if len(entries) != len(test.want) {
			t.Fatalf("#%d: len: %d, want: %d", i, len(entries), len(test.want))
		}

		for j := 0; j < len(entries); j++ {
			if entries[j].Index != test.want[j].Index ||
				entries[j].Term != test.want[j].Term {
				t.Fatalf("#%d: idx: %d, term: %d, want idx: %d, want term: %d",
					i, entries[j].Index, entries[j].Term, test.want[j].Index, test.want[j].Term)
			}
		}
	}
}
