package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/bior/raft"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/utils/pd"
	"github.com/thinkermao/network-simu-go"
)

const ElectionTimeout = 1000
const HeartbeatTimeout = 100
const tickSize = 25
const MaxSizePerMsg = 64 * 1024 * 1024 // 64MB

// AppCallback Used by config to check applied entries.
type AppCallback interface {
	CheckApply(id, index, value int) error
}

// a simple application base on raft,
type application struct {
	id      uint64
	handler network.Handler
	walDir  string

	rfMutex sync.Mutex // lock for raft.
	rf      *raft.Raft
	persist *Persister

	logMutex sync.Mutex
	applyErr error       // from apply channel readers
	logs     map[int]int // copy of each server's committed entries
	logIndex uint64
	logTerm  uint64
	callback AppCallback
}

// MakeApp return instance of Application.
func MakeApp(
	walDir string,
	handler network.Handler,
	callback AppCallback,
) Application {
	id := uint64(handler.ID())
	app := &application{
		id:      id,
		handler: handler,
		logs:    make(map[int]int),
	}

	app.callback = callback
	app.walDir = walDir
	app.handler.BindReceiver(app.handleMessage)

	return app
}

func (app *application) getRaft() *raft.Raft {
	app.rfMutex.Lock()
	defer app.rfMutex.Unlock()
	return app.rf
}

func (app *application) getPersist() *Persister {
	app.rfMutex.Lock()
	defer app.rfMutex.Unlock()
	return app.persist
}

func (app *application) handleMessage(from int, data []byte) {
	rf := app.getRaft()
	if rf == nil {
		return
	}

	var msg raftpd.Message
	pd.MustUnmarshal(&msg, data)

	log.Debugf("app id: %d received: %v", app.id, msg)

	rf.Step(&msg)
}

func (app *application) Send(to uint64, msg pd.Messager) error {
	log.Debugf("app id: %d send: %v", app.id, msg)
	data := pd.MustMarshal(msg)
	return app.handler.Call(int(to), data)
}

//
// allocate new raft object, rebuild from exists
// wal log.
func (app *application) Start(nodes []uint64) error {
	var err error
	var rf *raft.Raft
	if app.createPersist() {
		rf, err = raft.MakeRaft(app.id, nodes,
			ElectionTimeout, HeartbeatTimeout,
			tickSize, MaxSizePerMsg, app.walDir, app, app)
	} else {
		snapshot := app.ReadSnapshot()
		meta := raft.Metadata{
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
		}
		rf, err = raft.RebuildRaft(app.id, meta,
			nodes, ElectionTimeout, HeartbeatTimeout,
			tickSize, MaxSizePerMsg, app.walDir, app, app)
		app.restoreFromSnapshot(snapshot)
	}

	app.rfMutex.Lock()
	defer app.rfMutex.Unlock()

	app.rf = rf

	return err
}

func (app *application) createPersist() bool {
	app.rfMutex.Lock()
	defer app.rfMutex.Unlock()

	if app.persist == nil {
		app.persist = new(Persister)
		return true
	}
	return false
}

//
// release raft object of current application.
//
func (app *application) Shutdown() {
	app.rfMutex.Lock()
	rf := app.rf
	app.rf = nil
	app.rfMutex.Unlock()

	if rf != nil {
		rf.Kill()
	}
}

func (app *application) LogLength() int {
	app.logMutex.Lock()
	defer app.logMutex.Unlock()

	return len(app.logs)
}

func (app *application) LogAt(index int) (int, bool) {
	app.logMutex.Lock()
	defer app.logMutex.Unlock()

	value, ok := app.logs[index]
	if !ok {
		return 0, false
	}
	return value, true
}

func (app *application) Kill() {
	rf := app.getRaft()

	if rf == nil {
		return
	}

	rf.Kill()
}

func (app *application) Propose(num int) (uint64, uint64, bool) {
	rf := app.getRaft()

	if rf == nil {
		return 0, 0, false
	}

	bytes := [8]byte{}
	binary.LittleEndian.PutUint64(bytes[:], uint64(num))
	idx, term, isLeader := rf.Write(bytes[:])
	return idx, term, isLeader
}

func (app *application) GetState() (uint64, bool) {
	rf := app.getRaft()

	if rf == nil {
		return 0, false
	}

	return rf.GetState()
}

func (app *application) ApplyError() error {
	app.logMutex.Lock()
	defer app.logMutex.Unlock()

	return app.applyErr
}

func (app *application) ID() int {
	return app.handler.ID()
}

// must ensure persist field exists.
func (app *application) restoreFromSnapshot(snapshot *raftpd.Snapshot) {
	app.logMutex.Lock()
	defer app.logMutex.Unlock()

	app.logIndex = snapshot.Metadata.Index
	app.logTerm = snapshot.Metadata.Term

	if snapshot.Data != nil {
		var logs map[int]int
		buf := bytes.NewBuffer(snapshot.Data)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&logs); err != nil {
			panic("decode logs failed")
		}
		app.logs = logs
	} else {
		app.logs = make(map[int]int)
	}
}

// save to snapshot encode logs to bytes, and save it to snapshot.
func (app *application) saveToSnapshot() *raftpd.Snapshot {
	persist := app.getPersist()

	if persist == nil {
		panic("read snapshot, but persist is nil")
	}

	app.logMutex.Lock()
	defer app.logMutex.Unlock()

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(app.logs); err != nil {
		panic("encode logs failed")
	}

	snapshot := &raftpd.Snapshot{
		Metadata: raftpd.SnapshotMetadata{
			Index: app.logIndex,
			Term:  app.logTerm,
		},
		Data: data.Bytes(),
	}
	persist.SaveSnapshot(snapshot)

	return snapshot
}

func (app *application) GenSnapshot() (uint64, uint64) {
	snapshot := app.saveToSnapshot()
	rf := app.getRaft()
	if rf != nil {
		rf.Compact(snapshot)
	}
	return snapshot.Metadata.Index, snapshot.Metadata.Term
}

func (app *application) IsCrash() bool {
	rf := app.getRaft()
	return rf == nil
}
