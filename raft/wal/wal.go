package wal

import (
	"errors"
	"github.com/thinkermao/bior/raft/proto"
	"github.com/thinkermao/bior/raft/wal/proto"
	"github.com/thinkermao/bior/utils"
	"github.com/thinkermao/bior/utils/log"
	"github.com/thinkermao/bior/utils/pd"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	RecordMetadata int32 = iota
	RecordEntry
	RecordState
)

var (
	SegmentSize int64 = 64 * 1000 * 1000 // 64MB
	crcTable          = crc32.MakeTable(crc32.Castagnoli)
)
var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrMetadataConflict = errors.New("wal: conflicting metadata found")
	ErrFileNotFound     = errors.New("wal: file not found")
	ErrCRCMismatch      = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound = errors.New("wal: snapshot not found")
)

type Wal struct {
	walDir         string
	lastEntryIndex uint64
	files          []*os.File
	closer         func()

	enc *encoder
	dec *decoder
}

func CreateWal(walDir string, firstIndex uint64) (*Wal, error) {
	// TODO: check exists file
	name := filepath.Join(walDir, walName(0, 0))

	files := make([]*os.File, 0)
	if file, err := os.Create(name); err != nil {
		return nil, err
	} else {
		files = append(files, file)
	}
	closer := func() { closeAll(files...) }

	wal := &Wal{
		walDir:         walDir,
		lastEntryIndex: firstIndex,
		closer:         closer,
		files:          files,
	}

	// FIXME:
	wal.enc = makeEncoder(wal.tailFile())

	return wal, nil
}

// Open for write
func Open(walDir string, lsn uint64) (*Wal, error) {
	names, err := readAllWalNames(walDir)
	if err != nil {
		return nil, err
	}

	index, ok := searchIndex(names, lsn)
	if !ok || !isValidSequences(names[index:]) {
		return nil, ErrFileNotFound
	}

	files := make([]*os.File, 0)
	for i := index; i < len(names); i++ {
		path := filepath.Join(walDir, names[i])
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			closeAll(files...)
			return nil, err
		}
		files = append(files, f)
	}

	closer := func() { closeAll(files...) }

	wal := &Wal{
		walDir:         walDir,
		lastEntryIndex: lsn,
		closer:         closer,
		files:          files,
	}

	// FIXME:
	wal.dec = makeDecoder(wal.files)

	return wal, nil
}

func (wal *Wal) ReadAll() (state raftpd.HardState, entries []raftpd.Entry, err error) {
	utils.Assert(wal.dec != nil, "must be open mode")

	offset := wal.lastEntryIndex
	record := walpd.Record{}
	for err := wal.dec.decode(&record); err == nil; err = wal.dec.decode(&record) {
		switch record.Type {
		case RecordEntry:
			var entry raftpd.Entry
			pd.MustUnmarshal(&entry, record.Data)
			if entry.Index >= offset {
				entries = append(entries[:entry.Index-offset-1], entry)
			}
			wal.lastEntryIndex = entry.Index
		case RecordMetadata:
			// TODO:
		case RecordState:
			pd.MustMarshal(&state)
		default:
			log.Panicf("open file with unkonwn record type")
		}
	}

	/* translate to encode mode */
	if wal.closer != nil {
		wal.closer()
		wal.closer = nil
	}
	wal.dec = nil

	// TODO: tail file must exists
	wal.enc = makeEncoder(wal.tailFile())

	return
}

func (wal *Wal) fileRotation() error {
	nextSequence, nextIndex := wal.lastSequence(), wal.lastEntryIndex
	file, err := os.Create(filepath.Join(wal.walDir, walName(nextSequence, nextIndex)))
	if err != nil {
		return err
	}

	wal.files = append(wal.files, file)
	wal.closer = func() { closeAll(wal.files...) }
	wal.enc = makeEncoder(file)
	return nil
}

func (wal *Wal) Save(state *raftpd.HardState, entries []raftpd.Entry) error {
	if err := wal.saveState(state); err != nil {
		return err
	}

	for i := 0; i < len(entries); i++ {
		if err := wal.saveEntry(&entries[i]); err != nil {
			return err
		}
	}

	if err := wal.sync(); err != nil {
		return err
	}

	if curOff, err := wal.tailFile().Seek(0, io.SeekCurrent); err != nil {
		return err
	} else if curOff >= SegmentSizeBytes {
		return wal.fileRotation()
	}

	return nil
}

func (wal *Wal) saveState(state *raftpd.HardState) error {
	b := pd.MustMarshal(state)
	record := walpd.Record{Type: RecordState, Data: b}
	return wal.enc.encode(&record)
}

func (wal *Wal) saveEntry(entry *raftpd.Entry) error {
	b := pd.MustMarshal(entry)
	record := walpd.Record{Type: RecordEntry, Data: b}
	if err := wal.enc.encode(&record); err != nil {
		return err
	}
	wal.lastEntryIndex = entry.Index
	return nil
}

func (wal *Wal) sync() error {
	return wal.enc.flush()
}

func (wal *Wal) tailFile() *os.File {
	utils.Assert(len(wal.files) != 0, "file must no empty")

	return wal.files[len(wal.files)-1]
}

func (wal *Wal) lastSequence() uint64 {
	t := wal.tailFile()
	if t == nil {
		return 0
	}
	seq, _, err := parseWalName(filepath.Base(t.Name()))
	if err != nil {
		log.Fatalf("bad wal name %s (%v)", t.Name(), err)
	}
	return seq
}

func closeAll(files ...*os.File) {
	for i := 0; i < len(files); i++ {
		files[i].Close()
	}
}
