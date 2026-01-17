package myLSMTree

import "fmt"

type logicalTable struct {
	tree     *LSMTree
	name     string
	rootPath string
}

func NewTable(name string) *logicalTable {
	tree := newLSMTree(name)
	table := &logicalTable{name: name, tree: tree}
	return table
}

func (t *logicalTable) Insert(key, value []byte) {
	//go t.tree.Insert(DataModel{key: key, value: value})
	req := DataModel{key: key, value: value}
	t.tree.memTables.Put(req.key, req.value)
	t.tree.insertC <- req
}

func (t *logicalTable) Remove(key []byte) {
	//go t.tree.Insert(DataModel{key: key, value: value})
	req := DataModel{key: key, value: []byte(delteStr)}
	t.tree.memTables.Put(req.key, req.value)
	t.tree.insertC <- req
}

func (t *logicalTable) Get(key []byte) []byte {
	return t.tree.Get(key)
}

func (t *logicalTable) Compact(level int) {
	if len(t.tree.ssTables) <= level {
		fmt.Printf("This level doesn't have a file")
	}
	count := len(t.tree.ssTables[level])
	t.tree.nWayMerge(level,count)
}

func (t *logicalTable) Flush(file string) {
	t.tree.immuMemtable = append(t.tree.immuMemtable, t.tree.memTables)
	t.tree.flush(t.tree.immuMemtable[0], t.tree.path+"L0/"+file+",sst")
}

func (t *logicalTable) SetConfig(conf Configuration) {
	t.tree.conf = conf
}


func (t *logicalTable) Close() {
	t.tree.close()
}