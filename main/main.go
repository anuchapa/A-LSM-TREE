package main

import (
	"encoding/binary"
	"fmt"
	"myLSMTree"
)

func main() {
	// numbers := []uint32{15, 6, 9, 2, 8, 16, 19, 20, 100, 0}
	// buf := make([][]byte, len(numbers))
	// for i, n := range numbers {
	// 	buf[i] = binary.BigEndian.AppendUint32(buf[i], n)
	// }
	// h := (*myLSMTree.BytesHeap)(&buf)
	
	// myLSMTree.HeapInit(h)
	// for _, n := range *h {
	// 	fmt.Printf("%v ",binary.BigEndian.Uint32(n))
	// }

	// fmt.Println()

	// myLSMTree.Push(h,[]byte{0, 0, 0, 111})
	// for _, n := range *h {
	// 	fmt.Printf("%v ",binary.BigEndian.Uint32(n))
	// }

	// for i := 0; i < len(numbers); i++ {

	// 	fmt.Println(binary.BigEndian.Uint32(myLSMTree.Pop(h).([]byte)), h.Len())
	// }

	table := myLSMTree.NewTable("test")
	//table.Compact()
	// for i := 0; i < 300000; i++ {
	// 	buf := make([]byte, 4)
	// 	binary.BigEndian.PutUint32(buf, uint32(i))
	// 	table.Insert(buf, []byte(fmt.Sprintf("value%v", i)))
	// }

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(85198))
	result := table.Get(buf)
	fmt.Println(result)

	//time.Sleep(5*time.Second)
	//var result []byte
	// start := time.Now()
	// wg.Add(1)
	// go func() {
	// 	result = table.Get([]byte(fmt.Sprintf("key%v", 0)))
	// 	wg.Done()
	// }()
	// wg.Wait()
	// fmt.Println(time.Since(start))
	// fmt.Println(result)

	// start = time.Now()
	// wg.Add(1)
	// go func() {
	// 	result = table.Get([]byte(fmt.Sprintf("key%v", 99999)))
	// 	wg.Done()
	// }()
	// wg.Wait()
	// fmt.Println(time.Since(start))
	// fmt.Println(result)

	// wg.Add(1)
	// go func() {
	// 	result = table.Get([]byte(fmt.Sprintf("key%v", 0)))
	// 	wg.Done()
	// }()
	// wg.Wait()
	// fmt.Println(time.Since(start))
	// fmt.Println(result)

	//for j := 0; j < 5; j++ {
	// var totalDuration time.Duration
	// var max time.Duration = time.Duration(int64(0))
	// var min time.Duration = time.Duration(int64((^uint64(0)) >> 1))
	// var maxKey, minKey int
	// iterations := 200000
	// buf := make([]byte, 4*200000)
	// for i := 0; i < iterations; i += 2 {
	// 	start := time.Now()
	// 	//buf := make([]byte, 4)
	// 	s := 4*i
	// 	e := s+4
	// 	binary.BigEndian.PutUint32(buf[s:e], uint32(i))
	// 	_= table.Get(buf[s:e])
	// 	//fmt.Println(string(result))
	// 	//_ = table.Get([]byte(fmt.Sprintf("key%v", i)))
	// 	timeSince := time.Since(start)
	// 	if timeSince > 1*time.Second { // ถ้าช้าเกิน 1 วินาที ให้พ่น Log ทันที
	// 		fmt.Printf("Slow Key Detected: %d | Latency: %v | At: %v\n", i, timeSince, time.Now())
	// 	}
	// 	if timeSince >= max {
	// 		max = timeSince
	// 		maxKey = i
	// 	}
	// 	if timeSince <= min {
	// 		min = timeSince
	// 		minKey = i
	// 	}
	// 	totalDuration += timeSince

	// }
	// average := totalDuration / time.Duration(iterations)
	// //fmt.Printf("test: %d\n", j)
	// fmt.Printf("Total iterations: %d\n", iterations)
	// fmt.Printf("Average time per Get: %v\n", average)
	// fmt.Printf("Longest time: %v , key:%d\n", max, maxKey)
	// fmt.Printf("Shortest time: %v , key:%d\n", min, minKey)
	// fmt.Printf("Total duration: %v\n", totalDuration)
	// //}

	fmt.Scanln()
	fmt.Println("Shut dounwing.....")
}
