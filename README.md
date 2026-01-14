# MyLSM-Tree: A Journey into Storage Internals
This is Key-Value Storage implementation practicing LSM-Tree is high-write database backbone by Golang from the ground for learning about How how database work in low-level

# MyLSM-Tree 
1. **MEMTabel**: Data structure place in memory to  recieve new adding data.
2. **SSTable**: File on disk that store data from MEMTabel in sorted order. when MEMTabel reaches to some size threshold, it is flushed to disk.
3. **Compaction**:When a number of SSTables grow. We have to merge small SSTables to large one. The data is merged based on the key and the latest value.
4. **Read processes**: Reading from MEMTable first if it not found then we find at latest SSTable and so on. 

# Lessons Learned

## Skip List 
**Skip List** is a probabilistic data structure designed to allow efficient search, insertion and deletion operation in sorted list. It is alternative balance tree but simpler to implementation. 
    

## **Byounde simple merging:** 
    Initially, I desided use standard 2-way merge for compaction. However merging only 2 files at time would make compection process ineffient as the number of SSTAble files grow , I implemented an N-way Merge to process all SSTables in a level simultaneously.

- **Efficient Multi-file Merging with Min-Heap:**To implemented N-way merging, I used a **Min-Heap** to track current keys from all active SSTables. This approch allow to find the minimum key across K file in O(log K). resulting in overall is O(N log K), This sinificantly optimizes and speed up compaction process


