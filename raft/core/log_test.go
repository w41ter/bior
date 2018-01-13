package core

import "testing"
import (
	"fmt"
	"raft/proto"
)

func makeEntry(idx, term uint64) raftpd.Entry {
	return raftpd.Entry{
		Index: idx,
		Term:  term,
	}
}

func compareEntry(a, b raftpd.Entry) bool {
	return a.Term == b.Term && a.Index == b.Index
}

func compareEntries(t *testing.T, i int, a, want []raftpd.Entry) {
	if len(a) != len(want) {
		t.Errorf("#%d: len(entries) want: %d, get: %d",
			i, len(want), len(a))
	}
	for j := 0; j < len(a); j++ {
		if !compareEntry(a[j], want[j]) {
			t.Errorf("#%d: ents[%d] want: %v, get: %v",
				i, j, want[j], a[j])
		}
	}
}

func TestMakeLogHolder(t *testing.T) {
	tests := []LogHolder{
		{1, []raftpd.Entry{makeEntry(1, 1)}, 1, 1, 1},
		{1, []raftpd.Entry{makeEntry(2, 2)}, 2, 2, 2},
	}

	for i := 0; i < len(tests); i++ {
		t := &tests[i]
		e := MakeLogHolder(t.id, t.entries[0].Index, t.entries[0].Term)
		if e.id != t.id || e.lastStabled != t.lastStabled ||
			e.lastApplied != t.lastApplied || e.commitIndex != t.commitIndex ||
			len(e.entries) != len(t.entries) || e.entries[0].Index != t.entries[0].Index ||
			e.entries[0].Term != t.entries[0].Term {
			panic(fmt.Sprintf("#%d: make log holder failed", i))
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

	// TODO:
}

func TestLogHolder_getHintIndex(t *testing.T) {
	type param struct {
		entries []raftpd.Entry
		idx     uint64
		term    uint64
		want    uint64
	}

	tests := []param{
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2)}, 2, 1, 1},
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}, 3, 2, 2},
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 2)}, 3, 3, 1},
		{[]raftpd.Entry{makeEntry(1, 2), makeEntry(2, 2)}, 2, 3, 1},
	}

	for i := 0; i < len(tests); i++ {
		t := &tests[i]
		e := RebuildLogHolder(1, t.entries)
		get := e.getHintIndex(t.idx, t.term)
		if get != t.want {
			panic(fmt.Errorf("#%d: get: %d, want: %d", i, get, t.want))
		}
	}
}

func TestLogHolder_findConflict(t *testing.T) {
	previousEntries := []raftpd.Entry{
		makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3),
	}

	tests := []struct {
		entries  []raftpd.Entry
		conflict uint64
	}{
		// no conflict, empty Entries
		{[]raftpd.Entry{}, 0},
		{[]raftpd.Entry{}, 0},
		// no conflict
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}, 0},
		{[]raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3)}, 0},
		{[]raftpd.Entry{makeEntry(3, 3)}, 0},
		// no conflict, but has new Entries
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 5)}, 4},
		{[]raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4},
		{[]raftpd.Entry{makeEntry(3, 3), makeEntry(4, 4), makeEntry(5, 4)}, 4},
		{[]raftpd.Entry{makeEntry(4, 4), makeEntry(5, 5)}, 4},
		// conflicts with existing Entries
		{[]raftpd.Entry{makeEntry(1, 4), makeEntry(2, 4)}, 1},
		{[]raftpd.Entry{makeEntry(2, 1), makeEntry(3, 4), makeEntry(4, 4)}, 2},
		{[]raftpd.Entry{makeEntry(3, 1), makeEntry(4, 2), makeEntry(5, 4), makeEntry(6, 4)}, 3},
	}
	for i := 0; i < len(tests); i++ {
		t := tests[i]
		e := RebuildLogHolder(1, previousEntries)
		conflict := e.findConflict(t.entries)
		if conflict != t.conflict {
			panic(fmt.Errorf("#%d: conflict = %d, want %d", i, conflict, t.conflict))
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
		{[]raftpd.Entry{}, 3, []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)}},
		{[]raftpd.Entry{makeEntry(3, 2)}, 3, []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 2)}},
		// conflicts with index 2
		{[]raftpd.Entry{makeEntry(2, 3)}, 2, []raftpd.Entry{makeEntry(1, 1), makeEntry(2, 3)}},
		// conflicts with index 3
		{[]raftpd.Entry{makeEntry(3, 4)}, 3, []raftpd.Entry{
			makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 4),
		}},
	}

	for i := 0; i < len(tests); i++ {
		t := &tests[i]
		e := RebuildLogHolder(1, prevEntries)
		idx := e.Append(t.entries)
		if idx != t.widx {
			panic(fmt.Errorf("#%d: last_index = %d, want %d", i, idx, t.widx))
		}
		for j := range e.entries {
			en := e.entries[j]
			if en.Term != t.wents[j].Term || en.Index != t.wents[j].Index {
				panic(fmt.Errorf("#%d: entry not expect %d [%d Term: %d] != [%d Term: %d]",
					i, j, en.Index, en.Term, t.wents[j].Index, t.wents[j].Term))
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
}

func TestLogHolder_CompactTo(t *testing.T) {
	prevEntries := []raftpd.Entry{
		makeEntry(2, 2),
		makeEntry(3, 3),
		makeEntry(4, 4),
	}

	tests := []struct {
		idx, term uint64
		wants []raftpd.Entry
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
		{e.lastIndex() - 1, 4, true},
		{e.lastIndex(), 4, true},
		{e.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{e.lastIndex() - 1, 2, false},
		{e.lastIndex(), 2, false},
		{e.lastIndex() + 1, 2, false},
		// equal term, lager lastIndex wins
		{e.lastIndex() - 1, 3, false},
		{e.lastIndex(), 3, true},
		{e.lastIndex() + 1, 3, true},
	}
	for i := 0; i < len(tests); i++ {
		t := &tests[i]
		result := e.IsUpToDate(t.idx, t.term)
		if result != t.result {
			panic(fmt.Errorf("#%d: uptodate = %v, want %v", i, result, t.result))
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
		t := &tests[i]
		e := RebuildLogHolder(1, prevEntries)
		entries := e.Slice(t.lo, t.hi)
		for j := range entries {
			en := entries[j]
			if en.Term != t.wents[j].Term || en.Index != t.wents[j].Index {
				panic(fmt.Errorf("#%d: entry not expect %d [%d Term: %d] != [%d Term: %d]",
					i, j, en.Index, en.Term, t.wents[j].Index, t.wents[j].Term))
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
			panic(fmt.Errorf("#%d: at = %d, want = %d, get = %d",
				i, tests[i].index, tests[i].term, term))
		}
	}
}

func TestLogHolder_TryAppend(t *testing.T) {

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
		t := &tests[i]
		entries := drain(t.entries, t.to)

		if len(entries) != len(t.want) {
			panic(fmt.Sprintf("#%d: len: %d, want: %d", i, len(entries), len(t.want)))
		}

		for j := 0; j < len(entries); j++ {
			if entries[j].Index != t.want[j].Index ||
				entries[j].Term != t.want[j].Term {
				panic(fmt.Sprintf("#%d: idx: %d, term: %d, want idx: %d, want term: %d",
					i, entries[j].Index, entries[j].Term, t.want[j].Index, t.want[j].Term))
			}
		}
	}
}
