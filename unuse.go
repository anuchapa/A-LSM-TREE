package myLSMTree

// func (t *LSMTree) merge(new, old *ssTableStruct) {
// 	level := 0

// 	if new.level == old.level {
// 		level++
// 	} else {
// 		if new.level > old.level {
// 			level = new.level
// 		} else {
// 			level = old.level
// 		}
// 	}

// 	flushPath := t.path + "L" + strconv.Itoa(level) + "/"
// 	if _, err := os.Stat(flushPath); os.IsNotExist(err) {
// 		err = os.MkdirAll(flushPath, 0744)
// 		if err != nil {
// 			fmt.Println("Creating folder unsucess:", err)
// 			return
// 		}
// 		t.fileCount = append(t.fileCount, 0)
// 		t.ssTables = append(t.ssTables, make([]*ssTableStruct, 1))
// 	}

// 	tempFile, err := os.OpenFile(flushPath+formatID(t.fileCount[level]+1, 5)+".sst", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
// 	if err != nil {
// 		fmt.Println("Error opening file", err)
// 		return
// 	}

// 	defer tempFile.Close()

// 	newFile := new.file
// 	oldFile := old.file

// 	newFooter := getFooter(newFile)
// 	var newOffet int64 = 0

// 	oldFooter := getFooter(oldFile)
// 	var oldOffet int64 = 0

// 	keyNew, valueNew := getKeyValueBytes(newFile, &newOffet)
// 	keyOld, valueOld := getKeyValueBytes(oldFile, &oldOffet)

// 	block := make([]byte, 0, 4096)
// 	offset := 0
// 	offsetBuf := make([]byte, 4)
// 	indexes := make([]byte, 0)

// 	var firstKey, lastKey []byte

// 	writer := &writeStruct{
// 		tempFile:  tempFile,
// 		offset:    &offset,
// 		block:     &block,
// 		indexes:   &indexes,
// 		offsetBuf: &offsetBuf,
// 		firstKey:  &firstKey,
// 		lastKey:   &lastKey,
// 	}

// 	for newOffet <= int64(newFooter) && oldOffet <= int64(oldFooter) {
// 		if bytes.Compare(keyNew[4:], keyOld[4:]) == -1 {
// 			writer.Write(keyNew, valueNew)
// 			keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
// 		}

// 		if bytes.Compare(keyNew[4:], keyOld[4:]) == 1 {
// 			writer.Write(keyOld, valueOld)
// 			keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
// 		}

// 		if bytes.Equal(keyNew[4:], keyOld[4:]) {
// 			writer.Write(keyNew, valueNew)
// 			keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
// 			keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
// 		}

// 	}

// 	for newOffet <= int64(newFooter) {
// 		writer.Write(keyNew, valueNew)
// 		keyNew, valueNew = getKeyValueBytes(newFile, &newOffet)
// 	}

// 	for oldOffet <= int64(oldFooter) {
// 		writer.Write(keyOld, valueOld)
// 		keyOld, valueOld = getKeyValueBytes(oldFile, &oldOffet)
// 	}
// 	if len(block) > 0 {
// 		tempFile.Write(block)
// 		offset += len(block)
// 	}

// 	buf := make([]byte, 4)

// 	putUint32(buf, uint32(len(firstKey)))
// 	tempFile.Write(buf)
// 	tempFile.Write(firstKey)

// 	putUint32(buf, uint32(len(lastKey)))
// 	tempFile.Write(buf)
// 	tempFile.Write(lastKey)

// 	tempFile.Write(indexes)

// 	putUint32(offsetBuf, uint32(offset))
// 	tempFile.Write(offsetBuf)

// 	tempFile.Sync()
// 	tempFile.Close()

// 	t.fileCount[level]++
// 	t.ssTables[level] = append(t.ssTables[level], newSSTable(flushPath, level))
// }




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