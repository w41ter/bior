package peer

import "github.com/thinkermao/bior/utils"

type inFlights struct {
	start  uint
	count  uint
	buffer []uint64
}

func makeInFlights(cap uint) inFlights {
	return inFlights{
		start:  0,
		count:  0,
		buffer: make([]uint64, cap),
	}
}

func (i *inFlights) full() bool {
	return i.count == i.cap()
}

func (i *inFlights) cap() uint {
	return uint(len(i.buffer))
}

func (i *inFlights) mod(idx uint) uint {
	for idx >= i.cap() {
		idx -= i.cap()
	}
	return idx
}

// add adds an inFlight into inFlights
func (i *inFlights) add(inFlight uint64) {
	utils.Assert(!i.full(), "cannot add into a full inFlights")

	next := i.mod(i.start + i.count)

	utils.Assert(next <= uint(len(i.buffer)), "out of range")

	i.buffer[next] = inFlight
	i.count++
}

// freeTo frees the inFlights smaller or equal to the given `to` flight.
func (i *inFlights) freeTo(to uint64) {
	if i.count == 0 || to < i.buffer[i.start] {
		// out of the left side of the window
		return
	}

	for j := uint(0); j < i.count; j++ {
		idx := i.mod(i.start + j)
		if to >= i.buffer[idx] {
			continue
		}

		// found the first large inflight
		// free i inflights and set new start index
		i.count -= j
		i.start = idx
		return
	}
	// all need free
	i.reset()
}

func (i *inFlights) freeFirstOne() {
	i.freeTo(i.buffer[i.start])
}

func (i *inFlights) reset() {
	i.count = 0
	i.start = 0
}
