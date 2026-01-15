package main

import (
	"encoding/binary"
	"fmt"
	"myLSMTree"
	"os"
	// "sync"
	// "time"
)

func main() {
	os.RemoveAll("./db_table")
	table := myLSMTree.NewTable("test")
	//table.Compact()

	for i := 0; i < 1000000; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := []byte(fmt.Sprintf("value%d", i))

	
		table.Insert(key, val)

		go func(k []byte) {
			res := table.Get(k)
			if res == nil {
				fmt.Printf("Found missing key: %d\n", binary.BigEndian.Uint32(k))
			}
		}(key)
	}

	fmt.Scanln()
	fmt.Println("Shut dounwing.....")

}
