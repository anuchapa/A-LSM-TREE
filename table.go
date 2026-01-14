package myLSMTree

import (
	"fmt"
	"os"
)

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
		tree.ssTables = append(tree.ssTables, make([]*ssTableStruct, 1))
		tree.fileCount = append(tree.fileCount, 0)
		tree.ssTables[level] = make([]*ssTableStruct, 0)
		for _, sst := range ssTFiles {
			filePath := sstDir + sst.Name()
			ssTable := newSSTable(filePath, level)
			if ssTable == nil {
				fmt.Printf("Skip corrupt file %s", filePath)
				continue
			}
			tree.ssTables[level] = append(tree.ssTables[level], ssTable)
			tree.fileCount[level]++
		}
	}
	table := &logicalTable{name: name, tree: tree}
	go tree.insert(100000)
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