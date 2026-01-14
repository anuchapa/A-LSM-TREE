package myLSMTree

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"

)

type LSMTree struct {
	mu          sync.RWMutex
	memTables   *memTableStruct
	ssTables    [][]*ssTableStruct
	path        string
	maxSSTLevel int
	insertC     chan DataModel
	flushC      chan *memTableStruct
	compactC    chan bool
	//readC       chan []byte
	fileCount []int
}

func newLSMTree(path string) *LSMTree {
	return &LSMTree{
		memTables: newMemTable(),
		ssTables:  make([][]*ssTableStruct, 0),
		path:      path,
		fileCount: make([]int, 0),
		insertC:   make(chan DataModel),
		flushC:    make(chan *memTableStruct),
		compactC:  make(chan bool),
	}
}

func (t *LSMTree) get(key []byte) []byte {
	value, err := t.memTables.get(key)
	if err != nil {
		for _, sstLevel := range t.ssTables {
			for _, sst := range sstLevel {
				value, err = sst.get(key)
				if err == nil {
					break
				}
			}
		}
	}
	return value
}

func (t *LSMTree) insert(limit int) {
	go t.flushBackgound()
	for req := range t.insertC {
		t.memTables.put(req.key, req.value)
		size := t.memTables.skiplist.Size()
		if size >= limit {
			immuMemtable := t.memTables
			t.memTables = newMemTable()
			t.flushC <- immuMemtable
		}
	}
}

func (t *LSMTree) flushBackgound() {
	flushPath := t.path + "L0/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
		t.fileCount = append(t.fileCount, 0)
		t.ssTables = append(t.ssTables, make([]*ssTableStruct, 1))
	}

	for immuMemtable := range t.flushC {
		t.flush(immuMemtable, flushPath+formatID(t.fileCount[0]+1, 5)+".sst")
		t.fileCount[0]++
		if t.fileCount[0] >= 2 {
			go t.compactionStart()
		}
	}
}

func (t *LSMTree) compactionStart() {
	t.nKeyMerge(0)
}

func (t *LSMTree) nKeyMerge(level int) {
	fileLevel := level + 1
	flushPath := t.path + "L" + strconv.Itoa(fileLevel) + "/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
		t.fileCount = append(t.fileCount, 0)
		t.ssTables = append(t.ssTables, make([]*ssTableStruct, 1))
	}

	tempFile, err := os.OpenFile(flushPath+formatID(t.fileCount[fileLevel]+1, 5)+".sst", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}

	defer tempFile.Close()

	heap := make(KeyValueHeap, 0, len(t.ssTables[level]))
	for i, sst := range t.ssTables[level] {
		heap = append(heap, &KeyValueStruct{File: sst.file, Index: i, footer: getFooter(sst.file)})
		heap[i].Nex()
	}
	HeapInit(&heap)

	block := make([]byte, 0, 4096)
	offset := 0
	offsetBuf := make([]byte, 4)
	indexes := make([]byte, 0)
	var firstKey, lastKey []byte

	writer := &writeStruct{
		tempFile:  tempFile,
		offset:    &offset,
		block:     &block,
		indexes:   &indexes,
		offsetBuf: &offsetBuf,
		firstKey:  &firstKey,
		lastKey:   &lastKey,
	}

	for len(heap) > 0 {
		key, value := heap[0].Key, heap[0].Value
		writer.Write(key, value)

		for {
			if !heap[0].Nex() {
				Pop(&heap)
			}

			Fix(&heap, 0)

			if len(heap) < 1 || !bytes.Equal(key, heap[0].Key) {
				break
			}
		}
	}

	if len(block) > 0 {
		tempFile.Write(block)
		offset += len(block)
	}

	buf := make([]byte, 4)

	putUint32(buf, uint32(len(firstKey)))
	tempFile.Write(buf)
	tempFile.Write(firstKey)

	putUint32(buf, uint32(len(lastKey)))
	tempFile.Write(buf)
	tempFile.Write(lastKey)

	tempFile.Write(indexes)

	putUint32(offsetBuf, uint32(offset))
	tempFile.Write(offsetBuf)

	tempFile.Sync()
	tempFile.Close()

	t.fileCount[fileLevel]++
	t.ssTables[fileLevel] = append(t.ssTables[fileLevel], newSSTable(flushPath, fileLevel))

}



func (t *LSMTree) flush(mem *memTableStruct, path string) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}

	defer file.Close()

	var firstKey, key []byte
	offset := uint32(0)
	intdexs := make([]byte, 0)
	block := make([]byte, 0, 4096)
	lenBuf := make([]byte, 4)
	x := mem.skiplist.Head
	for x.Forward[0] != nil {
		x = x.Forward[0]
		key = x.Key
		value := x.Value
		lenBlock := uint32(len(block))
		lenKey := uint32(len(key))
		lenValue := uint32(len(value))
		if lenBlock+lenValue+lenKey+8 >= 4096 {
			file.Write(block)
			offset += lenBlock
			block = block[:0]
			lenBlock = 0
		}

		if lenBlock == 0 {
			putUint32(lenBuf, lenKey)
			intdexs = append(intdexs, lenBuf...)
			intdexs = append(intdexs, key...)
			putUint32(lenBuf, offset)
			intdexs = append(intdexs, lenBuf...)
		}

		putUint32(lenBuf, lenKey)
		block = append(block, lenBuf...)
		block = append(block, key...)
		if firstKey == nil {
			firstKey = make([]byte, 0, lenKey)
			firstKey = append(firstKey, key...)
		}
		putUint32(lenBuf, lenValue)
		block = append(block, lenBuf...)
		block = append(block, value...)

		// binary.Write(file, binary.BigEndian, uint32(len(key)))
		// file.Write(key)
		// binary.Write(file, binary.BigEndian, uint32(len(value)))
		// file.Write(value)
	}
	lastKey := key
	if len(block) > 0 {
		offset += uint32(len(block))
		file.Write(block)
	}

	putUint32(lenBuf, uint32(len(firstKey)))
	file.Write(lenBuf)
	file.Write(firstKey)

	putUint32(lenBuf, uint32(len(lastKey)))
	file.Write(lenBuf)
	file.Write(lastKey)

	file.Write(intdexs)
	putUint32(lenBuf, offset)
	file.Write(lenBuf)
	file.Sync()
	t.ssTables[0] = append(t.ssTables[0], newSSTable(path, 0))
}
