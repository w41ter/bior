package core

type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type readIndexStatus struct {
	index   uint64
	to      uint64
	context []byte
	acks    map[uint64]struct{}
}

type readOnly struct {
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string
}

func makeReadOnly() *readOnly {
	return &readOnly{
		pendingReadIndex: make(map[string]*readIndexStatus),
		readIndexQueue:   make([]string, 0),
	}
}

func (ro *readOnly) addRequest(index uint64, to uint64, context []byte) {
	ctx := string(context)
	if _, ok := ro.pendingReadIndex[ctx]; ok {
		return
	}
	ro.pendingReadIndex[ctx] = &readIndexStatus{
		index:   index,
		to:      to,
		context: context,
		acks:    make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)
}

func (ro *readOnly) receiveAck(from uint64, context []byte) int {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return 0
	}

	rs.acks[from] = struct{}{}
	// add one to include an ack from local node
	return len(rs.acks) + 1
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(context []byte) []*readIndexStatus {
	var i int
	var found bool

	ctx := string(context)
	rss := []*readIndexStatus{}
	waitRemoved := []string{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		waitRemoved = append(waitRemoved, okctx)
		if okctx == ctx {
			found = true
			break
		}
	}

	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, ctx := range waitRemoved {
			delete(ro.pendingReadIndex, ctx)
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
