package myLSMTree

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/anuchapa/skiplist"
)

type memTableStruct struct {
	mu       sync.Mutex
	skiplist *skiplist.SkipList[[]byte, []byte]
	size     int32
}

func newMemTable() *memTableStruct {
	return &memTableStruct{
		skiplist: skiplist.NewSkipList[[]byte,
			[]byte](bytes.Compare),
	}
}

func (m *memTableStruct) put(key []byte, value []byte) {
	m.mu.Lock()
	m.skiplist.Insert(key, value)
	m.mu.Unlock()
}

func (m *memTableStruct) get(key []byte) ([]byte, error) {
	m.mu.Lock()
	if m.skiplist.Size() == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("Memtable size: 0")
	}

	node := m.skiplist.Find(key)
	if node == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("This key is not found from RAM")
	} else {
		value := node.Value
		m.mu.Unlock()
		return value, nil
	}
}
