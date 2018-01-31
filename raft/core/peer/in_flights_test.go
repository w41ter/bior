package peer

import "testing"

func TestInFlights_cap(t *testing.T) {
	tests := []struct {
		cap, wcap uint
	}{
		{10, 10},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := makeInFlights(test.cap)
		if inf.cap() != test.wcap {
			t.Errorf("#%d: cap wrong get: %d, want: %d",
				i, inf.cap(), test.wcap)
		}
	}
}

func TestInFlights_full(t *testing.T) {
	tests := []struct {
		count uint
		w     bool
	}{
		{1, false},
		{10, true},
	}
	inf := makeInFlights(10)
	for i := 0; i < len(tests); i++ {
		inf.count = tests[i].count
		if inf.full() != tests[i].w {
			t.Errorf("#%d: full wrong, want: %v, get: %v",
				i, tests[i].w, inf.full())
		}
	}
}

func TestInFlights_freeTo(t *testing.T) {
	tests := []struct {
		start, count   uint
		buffer         []uint64
		to             uint64
		wstart, wcount uint
	}{
		// stale
		{0, 3, []uint64{1, 2, 3, 4}, 0, 0, 3},
		// free
		{0, 3, []uint64{1, 2, 3, 4}, 1, 1, 2},
		// free all
		{0, 3, []uint64{1, 2, 3, 4}, 3, 0, 0},
		// great
		{0, 3, []uint64{1, 2, 3, 4}, 4, 0, 0},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := inFlights{
			start:  test.start,
			count:  test.count,
			buffer: test.buffer,
		}
		inf.freeTo(test.to)
		if inf.start != test.wstart {
			t.Errorf("#%d: wrong freeTo, want start: %d, get: %d",
				i, test.wstart, inf.start)
		}
		if inf.count != test.wcount {
			t.Errorf("#%d: wrong freeTo, want count: %d, get: %d",
				i, test.wcount, inf.count)
		}
	}
}

func TestInFlights_reset(t *testing.T) {
	inf := makeInFlights(10)
	inf.count = 10
	inf.start = 5

	inf.reset()
	if inf.count != 0 || inf.start != 0 {
		t.Error("wrong reset")
	}
}

func TestInFlights_mod(t *testing.T) {
	var cap uint = 10
	tests := []struct {
		v, m uint
	}{
		{1, 1},
		{10, 0},
		{20, 0},
	}

	for i := 0; i < len(tests); i++ {
		test := tests[i]
		inf := makeInFlights(cap)
		get := inf.mod(test.v)
		if get != test.m {
			t.Errorf("#%d: mod wrong, want: %d, get: %d",
				i, test.m, get)
		}
	}
}

// func TestInFlights_add(t *testing.T) {

// }
