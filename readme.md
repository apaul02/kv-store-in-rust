# IronKV: An Asynchronous LSM-Tree Key-Value Store

IronKV is a highly concurrent, persistent, in-memory key-value database built entirely in Rust. It implements a Log-Structured Merge-tree (LSM-Tree) storage engine and handles thousands of concurrent TCP connections utilizing the Tokio asynchronous runtime.

## 🚀 Features

* **Asynchronous Network I/O:** Built on top of `tokio`, capable of efficiently multiplexing thousands of concurrent client connections without the overhead of heavy OS threads (solving the C10k problem).
* **Thread-Safe Concurrency:** Utilizes `Arc` and `RwLock` to enable infinite concurrent readers while ensuring safe, race-free mutable access for writers.
* **LSM-Tree Storage Engine:** Bypasses physical RAM limitations by buffering writes in memory and flushing immutable, sorted datasets to disk.
  * **MemTable:** Uses a `BTreeMap` for fast, automatically sorted in-memory inserts.
  * **SSTables:** Periodically flushes the MemTable to immutable Sorted String Tables on disk.
* **Optimized Read Path ($O(\log n)$):**
  * **Bloom Filters:** Utilizes in-memory probabilistic data structures to achieve $O(1)$ negative lookups, completely bypassing slow disk reads for missing keys.
  * **Binary Search:** Seeks through alphabetized SSTables using $O(\log n)$ binary search instead of $O(n)$ linear scans.
* **Durability & Crash Recovery:** Implements a Write-Ahead Log (WAL) via an Append-Only File (AOF) to ensure no data loss occurs between MemTable flushes in the event of a power failure.
* **Background Compaction:** Automatically merges and compacts older SSTables, reclaiming disk space and permanently purging deleted keys.
* **Append-Only Deletions:** Uses Tombstone markers (`__TOMBSTONE__`) to safely handle data deletions in an immutable storage architecture.

## 🧠 Architecture

IronKV's data flow is designed for write-heavy workloads without sacrificing read speeds:

1. **Write Path (`SET` / `DEL`):**
    * The command is written to the Append-Only File (WAL) on disk for durability.
    * The key-value pair (or Tombstone) is inserted into the in-memory `BTreeMap` (MemTable).
    * If the MemTable exceeds its byte limit, it is flushed to disk as an `.sst` file, and the AOF is truncated.
2. **Read Path (`GET`):**
    * The engine first checks the MemTable.
    * If the key is not found, it searches the on-disk SSTables in reverse chronological order (newest first).
    * If a Tombstone is encountered, the engine immediately returns `(nil)`.
3. **Compaction:**
    * When the SSTable count exceeds the configured threshold, the engine merges all files into a single, contiguous SSTable and drops resolved Tombstones.

## 🛠️ Getting Started

### Prerequisites

* Rust (1.70+)
* Cargo

### Build and Run

Clone the repository and run the server:

```bash
git clone [https://github.com/yourusername/ironkv.git](https://github.com/yourusername/ironkv.git)
cd ironkv
cargo run --release
```

Note: The server binds to 127.0.0.1:6379 by default.

### Supported commands

```bash
SET lang Rust
GET lang
DEL lang
```

## Future Enhancements

* [x] **Binary Search**: Upgrade the SSTable read path to utilize binary search byte-offsets for O(logn) disk reads.
* [x] **Bloom Filters**: Implement in-memory probabilistic data structures to skip searching SSTables that do not contain the requested key.
* [x] **RESP Protocol**: Migrate the custom text protocol to the standard Redis Serialization Protocol (RESP) to enable compatibility with the official redis-cli.
