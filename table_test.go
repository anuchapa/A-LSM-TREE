package myLSMTree

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestTableMemTableInsertAndGet(t *testing.T) {
	os.RemoveAll(DefaultRootPath)
	key := make([][]byte, 500)
	value := [500]string{}

	table := NewTable("test")

	for i := 0; i < 500; i++ {
		key[i] = make([]byte, 4)
		binary.BigEndian.PutUint32(key[i], uint32(i))
		value[i] = "value" + strconv.Itoa(i)
		table.Insert(key[i], []byte(value[i]))
	}

	for i := 0; i < 500; i++ {
		result := table.Get(key[i])
		if result == nil {
			t.Errorf("Expected to find key: %d ,but got nil", i)
		}
		if string(result) != value[i] {
			t.Errorf("Key: %d is expected to find value: %s ,but got %s", i, value[i], result)
		}
	}

}

func TestTableMemTableInsertAndGetInstantly(t *testing.T) {
	var wg sync.WaitGroup
	os.RemoveAll(DefaultRootPath)
	key := make([][]byte, 500)
	value := [500]string{}

	table := NewTable("test")

	for i := 0; i < 500; i++ {
		key[i] = make([]byte, 4)
		binary.BigEndian.PutUint32(key[i], uint32(i))
		value[i] = "value" + strconv.Itoa(i)
		table.Insert(key[i], []byte(value[i]))
		wg.Add(1)
		go func(idx int) {
			result := table.Get(key[i])
			if result == nil {
				t.Errorf("Expected to find key: %d ,but got nil", i)
			}
			if string(result) != value[i] {
				t.Errorf("Key: %d is expected to find value: %s ,but got %s", i, value[i], result)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestTableMemTableRemove(t *testing.T) {
	os.RemoveAll(DefaultRootPath)
	key := make([][]byte, 500)
	value := [500]string{}

	table := NewTable("test")

	for i := 0; i < 500; i++ {
		key[i] = make([]byte, 4)
		binary.BigEndian.PutUint32(key[i], uint32(i))
		value[i] = "value" + strconv.Itoa(i)
		table.Insert(key[i], []byte(value[i]))
	}

	for i := 0; i < 500; i += 2 {
		table.Remove(key[i])
		result := table.Get(key[i])
		if result != nil || string(result) == value[i] {
			t.Errorf("Key: %d is expected to be nil ,but got %s", i, result)
		}
	}
}

func TestTreeFlush(t *testing.T) {
	os.RemoveAll(DefaultRootPath)
	table := NewTable("test")
	mockData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d", i)
		mockData[k] = v
		table.Insert([]byte(k), []byte(v))
		if i == 500/2 {
			table.Flush(strconv.Itoa(i))
		}
	}
	table.Flush(strconv.Itoa(500))

	for k, expectedV := range mockData {
		actualV := table.Get([]byte(k))
		if actualV == nil {
			t.Errorf("Key %s lost after compaction", k)
			continue
		}
		if string(actualV) != expectedV {
			t.Errorf("Key %s data corrupted. Expected %s, got %s", k, expectedV, string(actualV))
		}
	}

}

func TestTreeCompact(t *testing.T) {
	os.RemoveAll(DefaultRootPath)
	table := NewTable("test")
	mockData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d", i)
		mockData[k] = v
		table.Insert([]byte(k), []byte(v))
		if i == 500/2 {
			table.Flush(strconv.Itoa(i))
		}
	}
	table.Flush(strconv.Itoa(500))

	table.Compact(0)
	for k, expectedV := range mockData {
		actualV := table.Get([]byte(k))
		if actualV == nil {
			t.Errorf("Key %s lost after compaction", k)
			continue
		}
		if string(actualV) != expectedV {
			t.Errorf("Key %s data corrupted. Expected %s, got %s", k, expectedV, string(actualV))
		}
	}

}

func TestReOpen(t *testing.T) {
	os.RemoveAll(DefaultRootPath)
	table := NewTable("test")
	mockData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%04d", i)
		v := fmt.Sprintf("val-%d", i)
		mockData[k] = v
		table.Insert([]byte(k), []byte(v))
		if i == 500/2 {
			table.Flush(strconv.Itoa(i))
		}
	}
	table.Flush(strconv.Itoa(500))

	table.Close()
	table = NewTable("test")
	for k, expectedV := range mockData {
		actualV := table.Get([]byte(k))
		if actualV == nil {
			t.Errorf("Key %s lost after compaction", k)
			continue
		}
		if string(actualV) != expectedV {
			t.Errorf("Key %s data corrupted. Expected %s, got %s", k, expectedV, string(actualV))
		}
	}

}

func BenchmarkTableInsertVariableKeys(b *testing.B) {
	os.RemoveAll(DefaultRootPath)
	table := NewTable("bench_test")
	value := []byte("value-data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		table.Insert(key, value)
	}

	table.Close()
}
