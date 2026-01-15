package myLSMTree

import(
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"bytes"
)

func formatID(id uint64, width int) string {
	strID := strconv.FormatUint(id,10)
	lenID := len(strID)

	var b strings.Builder
	for i := 0; i < width-lenID; i++ {
		b.WriteByte('0')
	}

	b.WriteString(strID)
	return b.String()
}

func putUint32(buf []byte, number uint32) {
	binary.BigEndian.PutUint32(buf, number)
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

func writeBlockToFile(file *os.File, block *[]byte, lenCheck int) int {
	offsetMove := len(*block)
	if offsetMove+lenCheck >= 4096 {
		file.Write(*block)
		*block = (*block)[:0]
		return offsetMove
	}
	return 0
}