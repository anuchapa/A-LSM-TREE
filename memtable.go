package myLSMTree

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/anuchapa/skiplist"
)

type memTableStruct struct {
	mu       sync.RWMutex
	skiplist *skiplist.SkipList[[]byte, []byte]
	size     int
}

func newMemTable() *memTableStruct {
	return &memTableStruct{
		skiplist: skiplist.NewSkipList[[]byte,
			[]byte](bytes.Compare),
	}
}

func (m *memTableStruct) Put(key []byte, value []byte) {
	m.mu.Lock()
	m.skiplist.Insert(key, value)
	m.size+= len(key)+len(value)
	m.mu.Unlock()
}

func (m *memTableStruct) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.skiplist.Size() == 0 {
		return nil, fmt.Errorf("Memtable size: 0")
	}

	node := m.skiplist.Find(key)
	if node == nil {
		return nil, fmt.Errorf("This key is not found from RAM")
	} else {
		value := node.Value
		return value, nil
	}
}
