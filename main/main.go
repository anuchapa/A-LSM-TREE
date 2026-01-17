package main

import (
	"encoding/binary"
	"fmt"
	"myLSMTree"
	"os"
	// "sync"
	// "time"
	//"strconv"
)

func main() {
	os.RemoveAll(myLSMTree.DefaultRootPath)
	table := myLSMTree.NewTable("test")
	i:=0
	for ;i < 10000000; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := []byte(fmt.Sprintf("value%d", i))
		table.Insert(key, val)
	}
	//fmt.Println(i)
	table.Close()
	fmt.Scanln()
	fmt.Println("Shut dounwing.....")

}
