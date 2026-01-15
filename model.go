package myLSMTree

import (
	"os"
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
	if *w.firstKey == nil {
		*w.firstKey = make([]byte, len(key[4:]))
		copy(*w.firstKey, key[4:])
	}
}

func (w *writeStruct) WriteFooter() {
	buf := make([]byte, 4)

	putUint32(buf, uint32(len(*w.firstKey)))
	w.tempFile.Write(buf)
	w.tempFile.Write(*w.firstKey)

	putUint32(buf, uint32(len(*w.lastKey)))
	w.tempFile.Write(buf)
	w.tempFile.Write(*w.lastKey)

	w.tempFile.Write(*w.indexes)

	putUint32(*w.offsetBuf, uint32(*w.offset))
	w.tempFile.Write(*w.offsetBuf)

	w.tempFile.Sync()
}

func (w *writeStruct) Reset() {
	*w.offset = 0
	*w.firstKey = nil
	*w.lastKey = nil
}

func (w *writeStruct) SwitchFile(newFile *os.File) {
	w.WriteFooter()
	w.tempFile.Close()
	w.Reset()
	w.tempFile = newFile
}

func (w *writeStruct) size() int {
	if *w.firstKey == nil || *w.lastKey == nil {
		return 0
	}
	return *w.offset + len(*w.block) + len(*w.indexes) + len(*w.firstKey) + len(*w.lastKey) + len(*w.offsetBuf)
}
