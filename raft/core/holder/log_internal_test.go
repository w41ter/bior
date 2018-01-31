package holder

import "testing"
import "github.com/thinkermao/bior/raft/proto"

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
		test := &tests[i]
		e := RebuildLogHolder(1, test.entries)
		get := e.getHintIndex(test.idx, test.term)
		if get != test.want {
			t.Fatalf("#%d: get: %d, want: %d", i, get, test.want)
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
		test := tests[i]
		e := RebuildLogHolder(1, previousEntries)
		conflict := e.findConflict(test.entries)
		if conflict != test.conflict {
			t.Fatalf("#%d: conflict = %d, want %d", i, conflict, test.conflict)
		}
	}
}

func TestLogHolder_truncateAndAppend(t *testing.T) {
	tests := []struct {
		origin  []raftpd.Entry
		entries []raftpd.Entry
		wents   []raftpd.Entry
	}{
		// empty
		{makeEntries(1, 2, 3), makeEntries(), makeEntries(1, 2, 3)},
		// append
		{makeEntries(1, 2), makeEntries(3, 4), makeEntries(1, 2, 3, 4)},
		// replace
		{makeEntries(3, 4, 5), makeEntries(1, 2), makeEntries(1, 2)},
		// truncate and append
		{makeEntries(1, 2, 3, 4, 5), makeEntries(3, 4, 5, 6), makeEntries(1, 2, 3, 4, 5, 6)},
	}
	for i, test := range tests {
		holder := RebuildLogHolder(1, test.origin)
		holder.truncateAndAppend(test.entries)
		compareEntries(t, i, holder.entries, test.wents)
	}
}

func Test_drain(t *testing.T) {
	tests := []struct {
		entries []raftpd.Entry
		to      int
		wents   []raftpd.Entry
	}{
		// empty entries
		{[]raftpd.Entry{}, 1, []raftpd.Entry{}},
		// drain all
		{[]raftpd.Entry{makeEntry(1, 1)}, 1, []raftpd.Entry{}},
		// drain 1
		{[]raftpd.Entry{makeEntry(1, 1), makeEntry(2, 2), makeEntry(3, 3)},
			1, []raftpd.Entry{makeEntry(2, 2), makeEntry(3, 3)}},
	}

	for i, test := range tests {
		ents := drain(test.entries, test.to)
		compareEntries(t, i, ents, test.wents)
	}
}
