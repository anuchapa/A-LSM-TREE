
# MyLSM-Tree: A Journey into Storage Internals

A high-performance Key-Value storage engine implementation based on the **Log-Structured Merge-Tree (LSM-Tree)** architecture. Built from scratch in Go to explore low-level database internals, memory management, and concurrent systems.

## System Architecture

1. **MemTable**: In-memory data structure (Skip List) for fast, synchronous writes.
2. **Immutable MemTable**: A transition layer where full MemTables wait to be flushed as background tasks.
3. **SSTable (Sorted String Table)**: Persistent on-disk files storing sorted key-value pairs with a built-in Sparse Index.
4. **Multi-level Hierarchy**: Supports tiered storage levels (L0, L1, ...) to balance write amplification and read performance.

## Key Features

### 1. Hybrid Concurrency Model
To achieve high throughput while maintaining consistency, I implemented a hybrid approach:
- **Immediate Visibility**: Writes are applied to the MemTable synchronously, ensuring `Get` requests always see the latest data.
- **Worker-Pool Pattern**: Heavy operations like flushing to disk and compaction are handled by dedicated background Goroutines via channels.

### 2. Efficient N-Way Merge
Initially using a standard 2-way merge, I optimized the compaction process by implementing an **N-Way Merge**:
- Uses a **Min-Heap** to track the smallest keys across multiple SSTables.
- Reduces complexity to **O(N log K)**, where K is the number of files being merged.
- Significantly speeds up Level-0 to Level-1 compaction.

### 3. Sparse Indexing & Binary Search
Every SSTable includes an index block at the footer. Instead of scanning the entire file, the engine performs a **Binary Search** on the index to jump to the correct data block, minimizing Disk I/O.

## Lessons Learned

- **Concurrency Control**: Deep dived into Go's concurrency primitives (`sync.RWMutex`, `channels`). Solved complex race conditions between background flushes and foreground reads.
- **Disk I/O Optimization**: Learned how to structure binary data in files, manage file offsets, and use buffered writes to minimize system call overhead.
- **Probabilistic Data Structures**: Implemented **Skip List** as an alternative to self-balancing trees for its simplicity and efficiency in concurrent environments.
