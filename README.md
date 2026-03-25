# trove

[![Package Version](https://img.shields.io/hexpm/v/trove)](https://hex.pm/packages/trove)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/trove/)

An embedded, crash-safe key-value store for Gleam, inspired by [CubDB](https://github.com/lucaong/cubdb). Built on an append-only copy-on-write B+ tree with type-safe codecs and explicit key ordering.

## Features

- Crash-safe - append-only file format with block-based headers; no corruption on unexpected shutdown
- Sorted keys - user-provided comparison function enables efficient range queries
- MVCC snapshots - readers never block writers
- Transactions - atomic read-write transactions with commit/cancel
- Compaction - manual or automatic; rebuilds the store file to reclaim dead entries

## Quick Start

```sh
gleam add trove
```

```gleam
import gleam/io
import gleam/string
import trove
import trove/codec

pub fn main() {
  let config = trove.Config(
    path: "./my_db",
    key_codec: codec.string(),
    value_codec: codec.string(),
    key_compare: string.compare,
    auto_compact: trove.NoAutoCompact,
    auto_file_sync: trove.AutoSync,
    call_timeout: 5000,
  )

  let assert Ok(db) = trove.open(config)
  trove.put(db, key: "language", value: "gleam")
  let assert Ok(value) = trove.get(db, key: "language")
  io.println(value)  // "gleam"
  trove.close(db)
}
```

## Usage

### CRUD Operations

```gleam
trove.put(db, key: "key", value: "value")
let assert Ok("value") = trove.get(db, key: "key")
let assert True = trove.has_key(db, key: "key")
trove.delete(db, key: "key")
let assert Error(Nil) = trove.get(db, key: "key")
```

### Batch Operations

All entries in a batch are written atomically — one header write covers
the entire batch.

```gleam
trove.put_multi(db, entries: [
  #("alice", "100"),
  #("bob", "200"),
  #("charlie", "300"),
])

trove.delete_multi(db, keys: ["alice", "charlie"])

// Or combine puts and deletes in one atomic call:
trove.put_and_delete_multi(
  db,
  puts: [#("dave", "400")],
  deletes: ["bob"],
)
```

### Range Queries

Keys are stored sorted, so range queries are efficient. Results are
returned as a `List`.

```gleam
import gleam/option.{None, Some}
import trove/range

// All entries, forward
let all = trove.range(db, min: None, max: None, direction: range.Forward)

// Bounded range: keys from "b" (inclusive) to "d" (exclusive)
let bounded = trove.range(
  db,
  min: Some(range.Inclusive("b")),
  max: Some(range.Exclusive("d")),
  direction: range.Forward,
)

// Reverse order
let reversed = trove.range(db, min: None, max: None, direction: range.Reverse)
```

For large result sets, use `with_snapshot` and `snapshot_range` to stream
entries lazily without reading them all into memory at once:

```gleam
import gleam/option.{None}
import gleam/yielder
import trove/range

trove.with_snapshot(db, fn(snap) {
  trove.snapshot_range(snapshot: snap, min: None, max: None, direction: range.Forward)
  |> yielder.each(fn(entry) { /* process entry */ })
})
```

### Transactions

Transactions provide exclusive write access. The callback receives a
`Tx` handle for reads and writes. Return `Commit` to apply or `Cancel`
to discard.

```gleam
let total = trove.transaction(db, timeout: 5000, callback: fn(tx) {
  let assert Ok(current) = trove.tx_get(tx, key: "alice")
  let tx = trove.tx_delete(tx, key: "old_key")
  let tx = trove.tx_put(tx, key: "alice", value: "150")
  let tx = trove.tx_put(tx, key: "bob", value: "250")
  trove.Commit(tx:, result: current)
})
```

To cancel a transaction (discards all writes within it):

```gleam
let result = trove.transaction(db, timeout: 5000, callback: fn(_tx) {
  trove.Cancel(result: "cancelled")
})
// result == "cancelled"
```

### Snapshots

Snapshots capture a point-in-time view of the database. Writes that
happen after the snapshot is taken are not visible to it.

```gleam
let value = trove.with_snapshot(db, fn(snap) {
  let assert Ok(v) = trove.snapshot_get(snapshot: snap, key: "key")
  v
})
```

### Custom Codecs

A `Codec(a)` pairs an encode function (`fn(a) -> BitArray`) with a
decode function (`fn(BitArray) -> Result(a, Nil)`). Built-in codecs
cover common types:

```gleam
import trove/codec

codec.string()      // UTF-8 strings
codec.int()         // 64-bit big-endian integers
codec.bit_array()   // raw bytes (identity)
```

For custom types, build your own codec:

```gleam
import gleam/bit_array
import trove/codec

pub type User {
  User(name: String, age: Int)
}

pub fn user_codec() -> codec.Codec(User) {
  codec.Codec(
    encode: fn(user) {
      let name_bytes = bit_array.from_string(user.name)
      let name_size = bit_array.byte_size(name_bytes)
      <<name_size:32, name_bytes:bits, user.age:32>>
    },
    decode: fn(bits) {
      case bits {
        <<name_size:32, name_bytes:bytes-size(name_size), age:32>> ->
          case bit_array.to_string(name_bytes) {
            Ok(name) -> Ok(User(name, age))
            Error(_) -> Error(Nil)
          }
        _ -> Error(Nil)
      }
    },
  )
}
```

### Compaction

Over time the store file accumulates dead entries from updates and
deletes. Compaction rebuilds the file, keeping only live data.

```gleam
// Manual compaction
let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

// Check dirt factor (ratio of dead to total entries)
let df = trove.dirt_factor(db)

// With ManualSync, call file_sync to flush writes to disk:
trove.file_sync(db)

// Enable auto-compaction: triggers when at least 100 dead entries
// and dirt factor exceeds 25%
trove.set_auto_compact(
  db,
  setting: trove.AutoCompact(min_dirt: 100, min_dirt_factor: 0.25),
)
```

Or configure auto-compaction at open time:

```gleam
let config = trove.Config(
  path: "./my_db",
  key_codec: codec.string(),
  value_codec: codec.string(),
  key_compare: string.compare,
  auto_compact: trove.AutoCompact(min_dirt: 100, min_dirt_factor: 0.25),
  auto_file_sync: trove.AutoSync,
  call_timeout: 5000,
)
```

## How It Works

trove stores data in an append-only B+ tree on disk. Every write appends new nodes to the file and creates a new root — old data is never overwritten. This gives you:

- Crash safety - a write is only committed when its header is fully written. Partial writes are detected and skipped on recovery.
- Zero-cost snapshots - a snapshot is just a pointer to an old root. The append-only structure guarantees those nodes remain valid.
- Single-writer / multiple-reader - one OTP actor serializes writes while any number of readers can traverse old tree snapshots concurrently.

The file format uses 1024-byte blocks with marker bytes for headers,
enabling backward-scanning recovery without a write-ahead log.
