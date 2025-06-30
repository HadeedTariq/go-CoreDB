package lsm

import redblacktree "github.com/HadeedTariq/go-CoreDB/db/lsm/red-black-tree"

// ~ so mem table is usually the skip list or the red black tree
type memTable struct {
	data *redblacktree.Tree
	b    int
}

func newMemTable() *memTable {
	return &memTable{data: redblacktree.New(), b: 0}
}

// ~ so as all of the things of the mem table work on the red black tree so the red black tree data structure that I write is work as the abstraction there

func (mt *memTable) put(key, value []byte) error {
	oldVal, exists := mt.data.Put(key, value)

	if exists {
		// ~ then basically I have to update the b according to the new value len
		mt.b += -len(oldVal) + len(value)
	} else {
		mt.b += len(key) + len(value)
	}

	return nil
}

func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.data.Get(key)
}

func (mt *memTable) delete(key []byte) error {
	value, exists := mt.data.Put(key, nil)

	if !exists {
		mt.b += len(key)
	} else {
		mt.b -= len(value)
	}

	return nil
}

func (mt *memTable) bytes() int {
	return mt.b
}

func (mt *memTable) clear() {
	mt.data = redblacktree.New()
	mt.b = 0
}

func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{mt.data.Iterator()}
}

// MemTable iterator.
type memTableIterator struct {
	it *redblacktree.Iterator
}

// hasNext returns true if there is next element.
func (it *memTableIterator) hasNext() bool {
	return it.it.HasNext()
}

// next returns the current key and value and advances the iterator position.
func (it *memTableIterator) next() ([]byte, []byte) {
	return it.it.Next()
}
