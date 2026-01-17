package myLSMTree

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
)

const delteStr = "[\\DeLeTe/]"

type LSMTree struct {
	wg           sync.WaitGroup
	mu           sync.RWMutex
	memTables    *memTableStruct
	immuMemtable []*memTableStruct
	ssTables     [][]*ssTableStruct
	path         string
	insertC      chan DataModel
	flushC       chan struct{}
	compactC     chan struct{}
	stopC        chan struct{}
	//readC       chan []byte
	//fileCount  []int
	nextFileID uint64
	conf       Configuration
}

func newLSMTree(name string) *LSMTree {
	conf := DefaultConfiguration()
	rootPath := conf.RootPath + name + "/sstable/"
	var levelDirs []os.DirEntry
	if _, err := os.Stat(rootPath); err == nil {
		fmt.Println("Folder already exists!")
		levelDirs, err = os.ReadDir(rootPath)
		if err != nil {
			fmt.Println(err)
		}

	} else if os.IsNotExist(err) {
		err := os.MkdirAll(rootPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return nil
		}
	}

	tree := &LSMTree{
		memTables:    newMemTable(),
		immuMemtable: make([]*memTableStruct, 0),
		ssTables:     make([][]*ssTableStruct, 0),
		path:         rootPath,
		//fileCount:    make([]int, 0),
		insertC:  make(chan DataModel),
		flushC:   make(chan struct{}),
		compactC: make(chan struct{}),
		stopC:    make(chan struct{}),
		conf:     conf,
	}

	for level, lvDir := range levelDirs {
		sstDir := rootPath + lvDir.Name() + "/"
		ssTFiles, err := os.ReadDir(sstDir)
		if err != nil {
			fmt.Println(err)
		}
		tree.ssTables = append(tree.ssTables, make([]*ssTableStruct, 0, 1))
		//tree.fileCount = append(tree.fileCount, 0)
		tree.ssTables[level] = make([]*ssTableStruct, 0)
		for _, sst := range ssTFiles {
			filePath := sstDir + sst.Name()
			ssTable := newSSTable(filePath, level)
			if ssTable == nil {
				fmt.Printf("Skip corrupt file %s", filePath)
				continue
			}
			tree.ssTables[level] = append(tree.ssTables[level], ssTable)
			//tree.fileCount[level]++
		}
	}
	tree.wg.Add(3)
	go tree.insertWorker()
	go tree.flushWorker()
	go tree.compactWorker()
	return tree
}

func (t *LSMTree) close() {
	close(t.insertC)
	close(t.stopC)

	t.wg.Wait()
	t.mu.Lock()
	for _, level := range t.ssTables {
		for _, sst := range level {
			sst.file.Close()
		}
	}
	// immuMemtable := t.memTables
	// t.immuMemtable = append(t.immuMemtable, immuMemtable)
	// t.memTables = newMemTable()
	t.mu.Unlock()
	//t.handleFlushJobs(t.path + "L0/")
}

func (t *LSMTree) Get(key []byte) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	value, err := t.memTables.Get(key)
	if err == nil {
		if bytes.Equal(value, []byte(delteStr)) {
			return nil
		}
		return value
	}

	err = fmt.Errorf("memTables not found")

	for i := len(t.immuMemtable) - 1; i >= 0; i-- {
		value, err = t.immuMemtable[i].Get(key)
		if err == nil {
			if bytes.Equal(value, []byte(delteStr)) {
				return nil
			}
			return value
		}
	}

	err = fmt.Errorf("immuMemtable not found")

	if err != nil {

		for _, sstLevel := range t.ssTables {
			for _, sst := range sstLevel {
				value, err = sst.Get(key)
				if err == nil {
					if bytes.Equal(value, []byte(delteStr)) {
						return nil
					}
					return value
				}
			}
		}
	}
	err = fmt.Errorf("sst not found")
	return nil
}

func (t *LSMTree) Insert(req DataModel) {
	t.memTables.mu.RLock()
	size := t.memTables.size
	t.memTables.mu.RUnlock()
	if size >= t.conf.SSTableSize*t.conf.MaxL0Files {
		t.mu.Lock()
		immuMemtable := t.memTables
		t.immuMemtable = append(t.immuMemtable, immuMemtable)
		t.memTables = newMemTable()
		t.mu.Unlock()
		t.flushC <- struct{}{}
	}
}

func (t *LSMTree) insertWorker() {
	defer t.wg.Done()
	for req := range t.insertC {
		t.Insert(req)
	}
}

func (t *LSMTree) flushWorker() {
	flushPath := t.path + "L0/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
		t.appenLevel()
	}

	defer t.wg.Done()
	for {
		select {
		case <-t.flushC:
			for t.handleFlushJobs(flushPath) {
				select {
				case t.compactC <- struct{}{}:
				case <-t.stopC:
					t.flushAll(flushPath)
					return
				default:
				}
			}
		case <-t.stopC:
			t.flushAll(flushPath)
			return
		}
	}
}

func (t *LSMTree) flushAll(flushPath string) {
	t.mu.Lock()
	immuMemtable := t.memTables
	t.immuMemtable = append(t.immuMemtable, immuMemtable)
	t.memTables = newMemTable()
	t.mu.Unlock()
	for t.handleFlushJobs(flushPath) {
	}
}

func (t *LSMTree) handleFlushJobs(flushPath string) bool {
	t.mu.Lock()
	if len(t.immuMemtable) == 0 {
		t.mu.Unlock()
		return false
	}
	job := t.immuMemtable[0]
	t.immuMemtable[0] = nil
	t.immuMemtable = t.immuMemtable[1:]
	t.nextFileID++
	t.mu.Unlock()
	path := flushPath + formatID(t.nextFileID, 20) + ".sst"
	t.flush(job, path)
	return true
	// go func(immu *memTableStruct, path string) {

	// }(job, path)
}

func (t *LSMTree) compactWorker() {
	defer t.wg.Done()
	for {
		select {
		case <-t.compactC:
			t.compactHandle()
		case <-t.stopC:
			for t.isNeedCompaction() {
				t.compactHandle()
			}
			//close(t.compactC)
			return
		}
	}

}

func (t *LSMTree) compactHandle() {
	for i := 0; i < len(t.ssTables); i++ {
		if i == t.conf.MaxLevel {
			return
		}
		t.compact(i)
	}
}

func (t *LSMTree) isNeedCompaction() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for i := 0; i < len(t.ssTables); i++ {
		count := len(t.ssTables[i])
		if i == 0 && count >= t.conf.MaxL0Files {
			return true
		} else if count >= t.conf.LevelMultiplier {
			return true
		}
	}

	return false

}

func (t *LSMTree) compact(level int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	count := len(t.ssTables[level])
	
	if level == 0 && count >= t.conf.MaxL0Files {
		t.nWayMerge(level, count)
	} else if count >= t.conf.LevelMultiplier {
		t.nWayMerge(level, count)
	}
}

func (t *LSMTree) getOverlapFiles(level int, n int) []*ssTableStruct {
	nextLevel := level + 1
	t.mu.RLock()
	sstCompact := make([]*ssTableStruct, 0, len(t.ssTables[level]))
	sstCompact = append(sstCompact, t.ssTables[level][:n]...)
	startRange := sstCompact[0].firstKey
	endRange := sstCompact[len(sstCompact)-1].lastKey
	if len(t.ssTables[nextLevel]) > 0 {
		for _, sst := range t.ssTables[nextLevel] {
			if bytes.Compare(sst.firstKey, endRange) <= 0 && bytes.Compare(sst.lastKey, startRange) >= 0 {
				sstCompact = append(sstCompact, sst)
			}
		}
	}
	t.mu.RUnlock()
	return sstCompact
}

func (t *LSMTree) appenLevel() {
	t.mu.Lock()
	//t.fileCount = append(t.fileCount, 0)
	t.ssTables = append(t.ssTables, make([]*ssTableStruct, 0, 1))
	t.mu.Unlock()
}

func (t *LSMTree) nWayMerge(level int, sstLen int) {
	fileLevel := level + 1
	flushPath := t.path + "L" + strconv.Itoa(fileLevel) + "/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
		t.appenLevel()
	}

	sstCompact := t.getOverlapFiles(level, sstLen)
	// sstCompact := make([]*ssTableStruct, 0, sstLen)
	// sstCompact = append(sstCompact, t.ssTables[level][:sstLen]...)
	heap := make(KeyValueHeap, 0, sstLen)
	for i, sst := range sstCompact {
		heap = append(heap, &KeyValueStruct{File: sst.file, Index: i, footer: getFooter(sst.file)})
		heap[i].Nex()
	}
	HeapInit(&heap)

	t.mu.Lock()
	t.nextFileID++
	tempFile, err := os.OpenFile(flushPath+formatID(t.nextFileID, 20)+".sst", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	t.mu.Unlock()
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}

	block := make([]byte, 0, t.conf.IndexBlockSize)
	offset := 0
	offsetBuf := make([]byte, 4)
	indexes := make([]byte, 0)
	var firstKey, lastKey []byte

	newFileCreated := []string{}

	writer := &writeStruct{
		tempFile:  tempFile,
		offset:    &offset,
		block:     &block,
		indexes:   &indexes,
		offsetBuf: &offsetBuf,
		firstKey:  &firstKey,
		lastKey:   &lastKey,
	}

	targetFileSize := t.conf.SSTableSize
	for i := 0; i < fileLevel; i++ {
		targetFileSize *= t.conf.LevelMultiplier
	}
	deleteByte := []byte(delteStr)
	for len(heap) > 0 {
		key, value := heap[0].Key, heap[0].Value
		if t.conf.MaxLevel-1 == level {
			if bytes.Equal(key[4:], deleteByte) {
				continue
			}
		}
		if writer.size() >= targetFileSize {
			t.mu.Lock()
			t.nextFileID++
			tempFile, err = os.OpenFile(flushPath+formatID(t.nextFileID, 20)+".sst", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			t.mu.Unlock()
			if err != nil {
				fmt.Println("Error opening file", err)
				return
			}
			newFileCreated = append(newFileCreated, writer.tempFile.Name())
			writer.SwitchFile(tempFile)
		}

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

	writer.WriteFooter()
	newFileCreated = append(newFileCreated, writer.tempFile.Name())
	tempFile.Close()
	// buf := make([]byte, 4)

	// putUint32(buf, uint32(len(firstKey)))
	// tempFile.Write(buf)
	// tempFile.Write(firstKey)

	// putUint32(buf, uint32(len(lastKey)))
	// tempFile.Write(buf)
	// tempFile.Write(lastKey)

	// tempFile.Write(indexes)

	// putUint32(offsetBuf, uint32(offset))
	// tempFile.Write(offsetBuf)

	// tempFile.Sync()
	// tempFile.Close()

	oldSstLNext := sstCompact[sstLen:]
	doDeleteNextL := map[string]bool{}

	for _, sst := range oldSstLNext {
		doDeleteNextL[sst.path] = true
	}

	t.mu.Lock()
	newSstNextL := make([]*ssTableStruct, 0)
	for _, current := range t.ssTables[fileLevel] {
		if !doDeleteNextL[current.path] {
			newSstNextL = append(newSstNextL, current)
		}
	}

	for _, newPath := range newFileCreated {
		newSstNextL = append(newSstNextL, newSSTable(newPath, fileLevel))
	}
	t.ssTables[fileLevel] = newSstNextL
	//t.fileCount[fileLevel] = len(t.ssTables[fileLevel])

	t.ssTables[level] = t.ssTables[level][sstLen:]
	//t.fileCount[level] = len(t.ssTables[level])
	t.mu.Unlock()

	for _, old := range sstCompact {
		old.obsolete = true
		old.CloseAndRemove()
	}

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
	block := make([]byte, 0, t.conf.IndexBlockSize)
	lenBuf := make([]byte, 4)
	x := mem.skiplist.Head

	for x.Forward[0] != nil {
		x = x.Forward[0]
		key = x.Key
		value := x.Value
		lenBlock := uint32(len(block))
		lenKey := uint32(len(key))
		lenValue := uint32(len(value))
		if lenBlock+lenValue+lenKey+8 >= uint32(t.conf.IndexBlockSize) {
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

	t.mu.Lock()
	//fmt.Println(t.fileCount[0])
	t.ssTables[0] = append(t.ssTables[0], newSSTable(path, 0))
	//t.fileCount[0]++
	t.mu.Unlock()
}
