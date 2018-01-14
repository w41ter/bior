package wal

import "strings"
import "os"
import "sort"
import "path/filepath"

func clearAllFilesEndsWith(dir string, suffix string) error {
	names, err := readDir(dir)
	if err != nil {
		return err
	}

	for _, name := range names {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		path := filepath.Join(dir, name)
		/* ignore return value */
		os.Remove(path)
	}
	return nil
}

// readDir returns the filenames in the given directory in sorted order.
func readDir(dirPath string) ([]string, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}
