package wal

import (
	"github.com/thinkermao/bior/raft/wal/proto"
	"github.com/thinkermao/bior/utils/pd"
	"os"
)

type encoder struct {
	file *os.File
}

func makeEncoder(file *os.File) *encoder {
	return &encoder{
		file: file,
	}
}

func (e *encoder) encode(record *walpd.Record) error {
	bytes, err := pd.Marshal(record)
	if err != nil {
		return err
	}

	length := (int32)(len(bytes))
	paddingBytes := ceil(length, frameSizeBytes)
	padding := make([]byte, paddingBytes-length)
	if _, err := e.file.Write(bytes); err != nil {
		return err
	}
	if _, err := e.file.Write(padding); err != nil {
		return err
	}
	return nil
}

func (e *encoder) flush() error {
	return e.file.Sync()
}
