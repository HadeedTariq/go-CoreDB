package lsm

import (
	"fmt"
	"os"
	"path"
	"strconv"
)

const (
	diskTableNumFileName         = "maxdisktable"
	diskTableDataFileName        = "data.db"
	diskTableIndexFileName       = "index.db"
	diskTableSparseIndexFileName = "sparse.db"
	newDiskTableFlag             = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

func createDiskTable(memTable *memTable, dbDir string, index, sparseKeyDistance int) error {
	prefix := strconv.Itoa(index) + "-"

	w, err := newDiskTableWriter(dbDir, prefix, sparseKeyDistance)

	if err != nil {
		return fmt.Errorf("failed to create disk table writer: %w", err)
	}

	for it := memTable.iterator(); it.hasNext(); {
		key, value := it.next()
		if err := w.write(key, value); err != nil {
			return fmt.Errorf("failed to write to disk table %d: %w", index, err)
		}
	}
	return nil
}

type disktableWriter struct {
	dataFile        *os.File
	indexFile       *os.File
	sparseIndexFile *os.File

	sparseKeyDistance         int
	keyNum, dataPos, indexPos int
}

// ~ according to me what this particular function is doing as we have the many files for the sstable
func newDiskTableWriter(dbDir, prefix string, sparseKeyDistance int) (*disktableWriter, error) {
	dataPath := path.Join(dbDir, prefix+diskTableDataFileName)

	dataFile, err := os.OpenFile(dataPath, newDiskTableFlag, 0600)

	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataPath, err)
	}

	indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)

	indexFile, err := os.OpenFile(indexPath, newDiskTableFlag, 0600)

	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)

	sparseIndexFile, err := os.OpenFile(sparseIndexPath, newDiskTableFlag, 0600)

	if err != nil {
		return nil, fmt.Errorf("failed to open sparse index file %s: %w", sparseIndexPath, err)
	}

	return &disktableWriter{
		dataFile:          dataFile,
		indexFile:         indexFile,
		sparseIndexFile:   sparseIndexFile,
		sparseKeyDistance: sparseKeyDistance,
		keyNum:            0,
		dataPos:           0,
		indexPos:          0,
	}, nil
}

func (w *disktableWriter) write(key, value []byte) error {
	dataBytes, err := encode(key, value, w.dataFile)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, w.dataPos, w.indexFile)
	if err != nil {
		return fmt.Errorf("failed to write to the index file: %w", err)
	}

	if w.keyNum%w.sparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, w.indexPos, w.sparseIndexFile); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}
	}

	w.dataPos += dataBytes
	w.indexPos += indexBytes
	w.keyNum++

	return nil
}
