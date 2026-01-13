package myLSMTree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/anuchapa/skiplist"
)

type DataModel struct {
	key   []byte
	value []byte
}

type indexBlock struct {
	key    []byte
	offset int64
}

type dataBlock struct {
	key    []byte
	offset int64
}

type ssTableStruct struct {
	mu       sync.RWMutex
	file     *os.File
	path     string
	level    int
	firstKey []byte
	lastKey  []byte
	dataTemp []*dataBlock
	indexes  []*indexBlock
}

func newSSTable(path string, level int) *ssTableStruct {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening SSTable.", err)
		return nil
	}

	indexes := make([]*indexBlock, 0)

	info, _ := file.Stat()

	footer := info.Size() - int64(4)

	buf := make([]byte, 4)

	file.ReadAt(buf, footer)                      //Get footer
	offset := int64(binary.BigEndian.Uint32(buf)) //Int32-[]byte convertion

	file.ReadAt(buf, offset)
	offset += 4
	keyLen := binary.BigEndian.Uint32(buf)
	firstKey := make([]byte, keyLen)
	file.ReadAt(firstKey, offset)
	offset += int64(keyLen)

	file.ReadAt(buf, offset)
	offset += 4
	keyLen = binary.BigEndian.Uint32(buf)
	lastKey := make([]byte, keyLen)
	file.ReadAt(lastKey, offset)
	offset += int64(keyLen)

	for offset < footer {

		file.ReadAt(buf, offset) //Read first index offset to get first index key length
		offset += 4
		keyLen := binary.BigEndian.Uint32(buf)
		bufIndexKey := make([]byte, keyLen)     //Buffer for read index key
		file.ReadAt(bufIndexKey, int64(offset)) //Read index key
		offset += int64(keyLen)

		file.ReadAt(buf, int64(offset)) //Read index offset
		offset += 4

		indexOffset := binary.BigEndian.Uint32(buf)
		indexes = append(indexes, &indexBlock{key: bufIndexKey, offset: int64(indexOffset)})
	}

	return &ssTableStruct{
		path:     path,
		file:     file,
		indexes:  indexes,
		level:    level,
		firstKey: firstKey,
		lastKey:  lastKey,
	}
}

func binarySearch(indexes []*indexBlock, key []byte) int {
	var target int
	left := 0
	right := len(indexes) - 1
	for left <= right {
		mid := left + (right-left)/2
		if bytes.Compare(key, indexes[mid].key) >= 0 {
			target = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return target
}

func (s *ssTableStruct) get(key []byte) ([]byte, error) {

	if bytes.Compare(key, s.firstKey) == -1 || bytes.Compare(key, s.lastKey) == 1 {
		return nil, fmt.Errorf("This key is not in this file.")
	}

	indexes := s.indexes
	file := s.file

	var offset int64 = 0
	target := binarySearch(indexes, key)
	offset = indexes[target].offset
	buf := make([]byte, 4)

	for {
		_, err := s.file.ReadAt(buf, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		offset += 4

		//binary.Read(bytes.NewReader(buf), binary.BigEndian, &keyLen)
		keyLen := binary.BigEndian.Uint32(buf)
		bufKey := make([]byte, keyLen)

		_, err = file.ReadAt(bufKey, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		offset += int64(keyLen)

		_, err = file.ReadAt(buf, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		offset += 4

		//binary.Read(bytes.NewReader(buf), binary.BigEndian, &valueLen)
		valueLen := binary.BigEndian.Uint32(buf)
		bufValue := make([]byte, valueLen)
		_, err = file.ReadAt(bufValue, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		offset += int64(valueLen)

		if bytes.Equal(bufKey, key) {
			return bufValue, nil
		}
	}
	return nil, fmt.Errorf("This key is not found from disk")

}

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

type LSMTree struct {
	mu          sync.RWMutex
	memTables   *memTableStruct
	ssTables    []*ssTableStruct
	path        string
	maxSSTLevel int
	insertC     chan DataModel
	flushC      chan *memTableStruct
	compactC    chan bool
	//readC       chan []byte
	fileCount int
}

func newLSMTree(path string) *LSMTree {
	return &LSMTree{
		memTables: newMemTable(),
		ssTables:  make([]*ssTableStruct, 0),
		path:      path,
		insertC:   make(chan DataModel),
		flushC:    make(chan *memTableStruct),
		compactC:  make(chan bool),
	}
}

func (t *LSMTree) get(key []byte) []byte {
	value, err := t.memTables.get(key)
	if err != nil {
		for _, sst := range t.ssTables {
			value, err = sst.get(key)
			if err == nil {
				break
			}
		}
	}
	return value
}

func (t *LSMTree) flusherStart(limit int) {
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

func formatID(id, width int) string {
	strID := strconv.Itoa(id)
	lenID := len(strID)

	var b strings.Builder
	for i := 0; i < width-lenID; i++ {
		b.WriteByte('0')
	}

	b.WriteString(strID)
	return b.String()
}

func (t *LSMTree) flushBackgound() {
	flushPath := t.path + "L0/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
	}

	for immuMemtable := range t.flushC {
		t.flush(immuMemtable, flushPath+formatID(t.fileCount+1, 5)+".sst")
		t.fileCount++
		if t.fileCount >= 2 {
			go t.compactionStart()
		}
	}
}

func (t *LSMTree) compactionStart() {
	sstLen := len(t.ssTables)
	for i := sstLen - 1; i > 0; i -= 2 {
		new := t.ssTables[i]
		old := t.ssTables[i-1]
		t.mearge(new, old)
	}
}

type writeStruct struct {
	tempFile                                     *os.File
	offset                                       *int
	block, indexes, offsetBuf, firstKey, lastKey *[]byte
}

func (w *writeStruct) Write(key, value []byte) {
	*w.offset += writeBlockToFile(w.tempFile, w.block, len(key)+len(key))
	if len(*w.block) == 0 {
		*w.indexes = append(*w.indexes, key...)
		putUint32(*w.offsetBuf, uint32(*w.offset))
		*w.indexes = append(*w.indexes, *w.offsetBuf...)
	}
	*w.block = append(*w.block, key...)
	*w.block = append(*w.block, value...)
	*w.lastKey = key[4:]
	if w.firstKey == nil {
		*w.firstKey = make([]byte, len(key[4:]))
		copy(*w.firstKey, key[4:])
	}
}

func (t *LSMTree) mearge(new, old *ssTableStruct) {

	flushPath := t.path + "L0/"
	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
		err = os.MkdirAll(flushPath, 0744)
		if err != nil {
			fmt.Println("Creating folder unsucess:", err)
			return
		}
	}

	tempFile, err := os.OpenFile(flushPath+"_temp.sst", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}

	defer tempFile.Close()

	newFile := new.file
	oldFile := old.file

	newFooter := getFooter(newFile)
	var newOffet int64 = 0

	oldFooter := getFooter(oldFile)
	var oldOffet int64 = 0

	keyNew, valueNew := getKeyValueBytes(newFile, &newOffet)
	keyOld, valueOld := getKeyValueBytes(oldFile, &oldOffet)

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

	for newOffet <= int64(newFooter) && oldOffet <= int64(oldFooter) {
		if bytes.Compare(keyNew[4:], keyOld[4:]) == -1 {
			// tempFile.Write(keyNew)
			// tempFile.Write(valueNew)

			// offset += writeBlockToFile(tempFile, &block, len(keyNew)+len(valueNew))
			// if len(block) == 0 {
			// 	indexes = append(indexes, keyNew...)
			// 	putUint32(offsetBuf, uint32(offset))
			// 	indexes = append(indexes, offsetBuf...)
			// }
			// block = append(block, keyNew...)
			// block = append(block, valueNew...)
			// lastKey = keyNew[4:]
			// if firstKey == nil {
			// 	firstKey = make([]byte, len(keyNew[4:]))
			// 	copy(firstKey, keyNew[4:])
			// }
			writer.Write(keyNew, valueNew)
			keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
		}

		if bytes.Compare(keyNew[4:], keyOld[4:]) == 1 {
			// tempFile.Write(keyOld)
			// tempFile.Write(valueOld)
			// fmt.Printf("%v %p\n",len(block),&block)
			// if offset > 0 {
			// 	fmt.Printf("%v %p\n",len(block),&block)
			// 	return
			// }
			// offset += writeBlockToFile(tempFile, &block, len(keyOld)+len(valueOld))
			// if len(block) == 0 {
			// 	indexes = append(indexes, keyNew...)
			// 	putUint32(offsetBuf, uint32(offset))
			// 	indexes = append(indexes, offsetBuf...)
			// }
			// block = append(block, keyOld...)
			// block = append(block, valueOld...)
			// lastKey = keyOld[4:]
			// if firstKey == nil {
			// 	firstKey = make([]byte, len(keyOld[4:]))
			// 	copy(firstKey, keyOld[4:])
			// }

			writer.Write(keyOld, valueOld)
			keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
		}

		if bytes.Equal(keyNew[4:], keyOld[4:]) {
			// tempFile.Write(keyNew)
			// tempFile.Write(valueNew)
			// offset += writeBlockToFile(tempFile, &block, len(keyNew)+len(valueNew))
			// if len(block) == 0 {
			// 	indexes = append(indexes, keyNew...)
			// 	putUint32(offsetBuf, uint32(offset))
			// 	indexes = append(indexes, offsetBuf...)
			// }
			// block = append(block, keyNew...)
			// block = append(block, valueNew...)
			// lastKey = keyNew[4:]
			// if firstKey == nil {
			// 	firstKey = make([]byte, len(valueNew[4:]))
			// 	copy(firstKey, valueNew[4:])
			// }
			writer.Write(keyNew, valueNew)
			keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
			keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
		}

	}

	for newOffet <= int64(newFooter) {
		// offset += writeBlockToFile(tempFile, &block, len(keyNew)+len(valueNew))
		// if len(block) == 0 {
		// 	indexes = append(indexes, keyNew...)
		// 	putUint32(offsetBuf, uint32(offset))
		// 	indexes = append(indexes, offsetBuf...)
		// }
		// block = append(block, keyNew...)
		// block = append(block, valueNew...)
		// lastKey = keyNew[4:]
		writer.Write(keyNew, valueNew)
		keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
	}

	for oldOffet <= int64(oldFooter) {
		// offset += writeBlockToFile(tempFile, &block, len(keyOld)+len(valueOld))
		// if len(block) == 0 {
		// 	indexes = append(indexes, keyNew...)
		// 	putUint32(offsetBuf, uint32(offset))
		// 	indexes = append(indexes, offsetBuf...)
		// }
		// block = append(block, keyOld...)
		// block = append(block, valueOld...)
		// lastKey = keyOld[4:]
		writer.Write(keyOld, valueOld)
		keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
	}
	if len(block) > 0 {
		tempFile.Write(block)
		offset += len(block)
	}
	tempFile.Write(firstKey)
	tempFile.Write(lastKey)
	tempFile.Write(indexes)
	putUint32(offsetBuf, uint32(offset))
	tempFile.Write(offsetBuf)
	tempFile.Sync()
}

func writeBlockToFile(file *os.File, block *[]byte, lenCheck int) int {
	offsetMove := len(*block)
	if offsetMove+lenCheck >= 4096 {
		file.Write(*block)
		*block = (*block)[:0]
		return offsetMove
	}
	return 0
}

func getFooter(file *os.File) uint32 {
	info, _ := file.Stat()
	buf := make([]byte, 4)
	file.ReadAt(buf, info.Size()-4)
	return binary.BigEndian.Uint32(buf)
}

func getKeyValueBytes(file *os.File, offset *int64) ([]byte, []byte) {
	key := getDataBytes(file, offset)
	value := getDataBytes(file, offset)
	return key, value
}

func getDataBytes(file *os.File, offset *int64) []byte {

	buf := make([]byte, 4)
	file.ReadAt(buf, *offset)
	(*offset) += 4
	dataLen := binary.BigEndian.Uint32(buf)
	dataBuf := make([]byte, dataLen)
	file.ReadAt(dataBuf, *offset)
	(*offset) += int64(dataLen)

	resultBuf := make([]byte, 0, dataLen+4)
	resultBuf = append(resultBuf, buf...)
	resultBuf = append(resultBuf, dataBuf...)

	return resultBuf
}

func putUint32(buf []byte, number uint32) {
	binary.BigEndian.PutUint32(buf, number)
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
	t.ssTables = append(t.ssTables, newSSTable(path, 0))
}

type logicalTable struct {
	tree     *LSMTree
	name     string
	rootPath string
}

func NewTable(name string) *logicalTable {
	rootPath := "./db_table/" + name + "/sstable/"
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
	tree := newLSMTree(rootPath)

	for level, lvDir := range levelDirs {
		sstDir := rootPath + lvDir.Name() + "/"
		ssTFiles, err := os.ReadDir(sstDir)
		if err != nil {
			fmt.Println(err)
		}
		for _, sst := range ssTFiles {
			filePath := sstDir + sst.Name()
			ssTable := newSSTable(filePath, level)
			if ssTable == nil {
				fmt.Printf("Skip corrupt file %s", filePath)
				continue
			}
			tree.ssTables = append(tree.ssTables, ssTable)
			tree.fileCount++
		}
	}
	table := &logicalTable{name: name, tree: tree}
	go tree.flusherStart(100000)
	return table
}

func (t *logicalTable) Insert(key, value []byte) {
	t.tree.insertC <- DataModel{key: key, value: value}
}

func (t *logicalTable) Get(key []byte) []byte {
	return t.tree.get(key)
}

func (t *logicalTable) Compact() {
	t.tree.compactionStart()
}

// type DataInsert struct {
// 	Tname string
// 	Data  DataModel
// }

//type LSMTreeEngine struct {
// 	tables map[string]*logicalTable
// 	status bool
// 	state  *EngineState
// }

// type EngineState struct {
// 	delete chan string
// 	insert chan *DataInsert
// 	read   chan *struct {
// 		table string
// 		key   []byte
// 	}
// }

// func newEngineState() *EngineState {
// 	return &EngineState{
// 		delete: make(chan string),
// 		insert: make(chan *DataInsert),
// 		read: make(chan *struct {
// 			table string
// 			key   []byte
// 		}),
// 	}
// }

// func NewEngine() *LSMTreeEngine {
// 	return &LSMTreeEngine{
// 		tables: map[string]*logicalTable{},
// 		state:  newEngineState(),
// 	}
// }

// func (e *LSMTreeEngine) EngineStart() {
// 	e.status = true
// 	go e.handleState()
// }

// func (e *LSMTreeEngine) handleState() {
// 	for {
// 		select {
// 		case name := <-e.state.delete:
// 			//fmt.Println("Engine:delete")
// 			delete(e.tables, name)
// 		case data := <-e.state.insert:
// 			//fmt.Println("Engine:insert")
// 			e.tables[data.Tname].tree.insertC <- data.Data
// 		case req := <-e.state.read:
// 			e.tables[req.table].tree.readData(req.key)
// 		}
// 	}
// }

// func (e *LSMTreeEngine) AddTable(name string) {

// 	if !e.status {
// 		fmt.Println("The Engine hasn't started.")
// 		return
// 	}
// 	rootPath := fmt.Sprintf("./SSTFile/table_%v", name)
// 	err := os.MkdirAll(rootPath, 0754)
// 	if err != nil {
// 		fmt.Println("Creating folder unsucess:", err)
// 		return
// 	} else {
// 		e.tables[name] = newTable(name, rootPath)
// 	}
// }

// func (e *LSMTreeEngine) DeleteTable(name string) {

// 	if !e.status {
// 		fmt.Println("The Engine hasn't started.")
// 		return
// 	}

// 	e.state.delete <- name
// }

// func (e *LSMTreeEngine) InsertData(data DataInsert) {

// 	if !e.status {
// 		fmt.Println("The Engine hasn't started.")
// 		return
// 	}
// 	e.state.insert <- &data
// }

// func (e *LSMTreeEngine) GetData(table string, key []byte) {

// 	if !e.status {
// 		fmt.Println("The Engine hasn't started.")
// 		return
// 	}

// 	e.state.read <- &struct {
// 		table string
// 		key   []byte
// 	}{table: table, key: key}
// }
