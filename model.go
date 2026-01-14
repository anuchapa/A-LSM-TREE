package myLSMTree

import(
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
