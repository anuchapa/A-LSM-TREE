package myLSMTree

import(
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

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

	//fmt.Println(binary.BigEndian.Uint32(lastKey))

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
	//fmt.Println(s.firstKey, s.lastKey)
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