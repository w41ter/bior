package read

type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type ReadIndexStatus struct {
	Index   uint64
	To      uint64
	Context []byte
	Acks    map[uint64]struct{}
}

type ReadOnly struct {
	pendingReadIndex map[string]*ReadIndexStatus
	readIndexQueue   []string
}

func MakeReadOnly() *ReadOnly {
	return &ReadOnly{
		pendingReadIndex: make(map[string]*ReadIndexStatus),
		readIndexQueue:   make([]string, 0),
	}
}

func (ro *ReadOnly) AddRequest(index uint64, to uint64, context []byte) {
	ctx := string(context)
	if _, ok := ro.pendingReadIndex[ctx]; ok {
		return
	}
	ro.pendingReadIndex[ctx] = &ReadIndexStatus{
		Index:   index,
		To:      to,
		Context: context,
		Acks:    make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)
}

func (ro *ReadOnly) ReceiveAck(from uint64, context []byte) int {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return 0
	}

	rs.Acks[from] = struct{}{}
	// add one to include an ack from local node
	return len(rs.Acks) + 1
}

// Advance advances the read only request queue kept by the ReadOnly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *ReadOnly) Advance(context []byte) []*ReadIndexStatus {
	var i int
	var found bool

	ctx := string(context)
	rss := []*ReadIndexStatus{}
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
// request in ReadOnly struct.
func (ro *ReadOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
