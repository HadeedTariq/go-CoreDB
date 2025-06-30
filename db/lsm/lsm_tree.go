package lsm

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
)

const (
	MaxKeySize   = math.MaxUint16
	MaxValueSize = math.MaxUint16
)

const (
	walFileName                  = "wal.db"
	defaultMemTableThreshold     = 64000
	defaultSparseKeyDistance     = 128
	defaultDiskTableNumThreshold = 10
)

var (
	ErrKeyRequired   = errors.New("key required")
	ErrValueRequired = errors.New("value required")
	ErrKeyTooLarge   = errors.New("key too large")
	ErrValueTooLarge = errors.New("value too large")
)

type LSMTree struct {
	dbDir string

	wal *os.File

	maxDiskTableIndex int

	diskTableNum int

	memTable *memTable

	memTableThreshold int

	diskTableNumThreshold int

	sparseKeyDistance int
}

func MemTableThreshold(memTableThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.memTableThreshold = memTableThreshold
	}
}

func SparseKeyDistance(sparseKeyDistance int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

func DiskTableNumThreshold(diskTableNumThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.diskTableNumThreshold = diskTableNumThreshold
	}
}

func Open(dbDir string, options ...func(*LSMTree)) (*LSMTree, error) {

	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s doesn't exist", dbDir)
	}

	walPath := path.Join(dbDir, walFileName)

	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", walPath, err)
	}

	memTable, err := loadMemTable(wal)

	if err != nil {
		return nil, fmt.Errorf("failed to load entries from %s: %w", walPath, err)
	}

	diskTableNum, maxDiskTableIndex, err := readDiskTableMeta(dbDir)

	if err != nil {
		return nil, fmt.Errorf("failed to read disk table meta: %w", err)
	}

	t := &LSMTree{
		wal:                   wal,
		memTable:              memTable,
		dbDir:                 dbDir,
		maxDiskTableIndex:     maxDiskTableIndex,
		memTableThreshold:     defaultMemTableThreshold,
		sparseKeyDistance:     defaultSparseKeyDistance,
		diskTableNum:          diskTableNum,
		diskTableNumThreshold: defaultDiskTableNumThreshold,
	}

	for _, option := range options {
		option(t)
	}

	return t, nil

}

func (t *LSMTree) Close() error {
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", t.wal.Name(), err)
	}
	return nil
}

// ~ so now I am gonna write the db putting mechanism

func (t *LSMTree) Put(key []byte, value []byte) error {
	// ~ so in case of lsm tree we doesn't directly write to the disk we write to the mem table for durability use the wal mechanism
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) == 0 {
		return ErrValueRequired
	} else if uint64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

}
