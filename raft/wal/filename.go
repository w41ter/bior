package wal

import (
	"errors"
	"fmt"
	"github.com/thinkermao/bior/utils/log"
	"strings"
)

var (
	badWalName = errors.New("bad wal name")
)

type FileName struct {
	sequence uint64
	index    uint64
}

func parseWalName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, badWalName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

func filterTempFiles(names []string) []string {
	result := make([]string, 0)
	for i := 0; i < len(names); i++ {
		if _, _, err := parseWalName(names[i]); err != nil {
			// TODO: log skip tmp files
			continue
		}
		result = append(result, names[i])
	}
	return result
}

func readAllWalNames(dir string) ([]string, error) {
	names, err := readDir(dir)
	if err != nil {
		return nil, err
	}

	names = filterTempFiles(names)
	if len(names) == 0 {
		return nil, ErrFileNotFound
	}
	return names, nil
}

func isValidSequences(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWalName(name)
		if err != nil {
			log.Panicf("parse correct name should never fail: %v", err)
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWalName(name)
		if err != nil {
			log.Panicf("parse correct name should never fail: %v", err)
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}
