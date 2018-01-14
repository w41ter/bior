package wal

import (
	"bufio"
	"encoding/binary"
	"github.com/thinkermao/bior/raft/wal/proto"
	"github.com/thinkermao/bior/utils/pd"
	"hash/crc32"
	"io"
	"os"
)

const frameSizeBytes = 8

type decoder struct {
	brs          []*bufio.Reader
	lastValidOff int32
}

func makeDecoder(files []*os.File) *decoder {
	readers := make([]*bufio.Reader, len(files))
	for i := range files {
		readers[i] = bufio.NewReader(files[i])
	}
	return &decoder{
		brs:          readers,
		lastValidOff: 0,
	}
}

func (d *decoder) decode(record *walpd.Record) error {
	record.Reset()
	if len(d.brs) == 0 {
		return io.EOF
	}

	length, err := readInt32(d.brs[0])
	if err == io.EOF || (err == nil && length == 0) {
		// hit end of file or preallocated space
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.lastValidOff = 0
		return d.decode(record)
	}
	if err != nil {
		return err
	}

	paddingBytes := ceil(length, frameSizeBytes)
	data := make([]byte, length+paddingBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := pd.Unmarshal(record, data[:length]); err != nil {
		return err
	}

	crc := crc32.Checksum(record.Data, crcTable)
	if record.Crc != crc {
		return ErrCRCMismatch
	}

	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += length + paddingBytes
	return nil
}

func ceil(length int32, padding int32) int32 {
	return (length + padding - 1) / padding
}

func readInt32(r io.Reader) (int32, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
