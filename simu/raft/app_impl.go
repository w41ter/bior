package raft

import (
	"encoding/binary"
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
	bytes := pd.MustMarshal(msg)
	return app.handler.Call(int(to), bytes)
}

//
// allocate new raft object, rebuild from exists
// wal log.
func (app *application) Start(nodes []uint64) error {
	app.rfMutex.Lock()
	defer app.rfMutex.Unlock()

	var err error
	var rf *raft.Raft
	if app.persist != nil {
		snapshot := app.ReadSnapshot()
		rf, err = raft.RebuildRaft(app.id, snapshot.Metadata.Index,
			nodes, ElectionTimeout, HeartbeatTimeout,
			tickSize, app.walDir, app, app)
	} else {
		rf, err = raft.MakeRaft(app.id, nodes,
			ElectionTimeout, HeartbeatTimeout,
			tickSize, app.walDir, app, app)
		app.persist = new(Persister)
	}

	app.rf = rf

	return err
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
