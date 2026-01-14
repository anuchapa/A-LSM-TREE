package myLSMTree

import (
	"bytes"
	"os"
)

type KeyValueStruct struct {
	Key    []byte
	Value  []byte
	File   *os.File
	Offset int64
	footer uint32
	Index  int
}

func (k *KeyValueStruct) Nex() bool {
	if k.Offset == int64(k.footer) {
		return false
	}
	k.Key, k.Value = getKeyValueBytes(k.File, &k.Offset)
	return true
}

type KeyValueHeap []*KeyValueStruct

func (h *KeyValueHeap) Swap(i, j int) {
	heap := *h
	heap[i], heap[j] = heap[j], heap[i]
}

func (h *KeyValueHeap) Len() int {
	return len(*h)
}

func (h *KeyValueHeap) Cmp(i, j int) bool {
	heap := *h
	if bytes.Equal(heap[i].Key, heap[j].Key) {
		return heap[i].Index > heap[j].Index
	}
	return bytes.Compare(heap[i].Key, heap[j].Key) == -1
}

func (h *KeyValueHeap) Push(x any) {
	heap := *h
	xStruct := x.(*KeyValueStruct)
	*h = append(heap, xStruct)
}

func (h *KeyValueHeap) Pop() any {
	heap := *h
	n := len(heap) - 1
	last := heap[n]
	*h = heap[0:n]
	return last
}

type HeapInterface interface {
	Swap(i, j int)
	Push(x any)
	Pop() any
	Cmp(i, j int) bool
	Len() int
}

func HeapInit(h HeapInterface) {
	n := h.Len()

	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

func Push(h HeapInterface, x any) {
	h.Push(x)
	up(h, h.Len()-1)
}

func Pop(h HeapInterface) any {
	n := h.Len() - 1
	h.Swap(0, n)
	down(h, 0, n)
	return h.Pop()
}

func Fix(h HeapInterface, i int) {
	if down(h, i, h.Len()) {
		up(h, i)
	}
}

func up(h HeapInterface, i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.Cmp(i, parent) {
			break
		}
		h.Swap(i, parent)
		i = parent
	}
}

func down(h HeapInterface, start int, n int) bool {
	i := start
	for {
		cleft := 2*i + 1
		cright := cleft + 1

		min := cleft
		if cleft >= n {
			break
		}

		if cright < n && h.Cmp(cright, cleft) {
			min = cright
		}

		if !h.Cmp(min, i) {
			break
		}
		h.Swap(i, min)
		i = min
	}
	return i > start
}
