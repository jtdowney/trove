import gleam/float
import gleam/int
import gleam/io
import gleam/string
import trove
import trove/codec

pub fn main() -> Nil {
  let config =
    trove.Config(
      path: "/tmp/trove_compaction_example",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
      auto_compact: trove.NoAutoCompact,
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  io.println("=== Building up dead entries ===")
  trove.put_multi(db, entries: [
    #("key1", "v1"),
    #("key2", "v2"),
    #("key3", "v3"),
    #("key4", "v4"),
    #("key5", "v5"),
  ])

  trove.put(db, key: "key1", value: "v1_updated")
  trove.put(db, key: "key2", value: "v2_updated")
  trove.put(db, key: "key2", value: "v2_updated_again")
  trove.delete(db, key: "key3")
  trove.delete(db, key: "key4")

  io.println(
    "size: "
    <> int.to_string(trove.size(db))
    <> ", dirt_factor: "
    <> float.to_string(trove.dirt_factor(db)),
  )

  io.println("\n=== Compacting ===")
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  io.println(
    "size: "
    <> int.to_string(trove.size(db))
    <> ", dirt_factor: "
    <> float.to_string(trove.dirt_factor(db)),
  )

  io.println("\n=== Data after compaction ===")
  let assert Ok(v1) = trove.get(db, key: "key1")
  io.println("  key1 = " <> v1)
  let assert Ok(v2) = trove.get(db, key: "key2")
  io.println("  key2 = " <> v2)
  let assert Error(Nil) = trove.get(db, key: "key3")
  io.println("  key3 = (deleted)")
  let assert Ok(v5) = trove.get(db, key: "key5")
  io.println("  key5 = " <> v5)

  trove.close(db)

  io.println("\n=== Auto-compaction demo ===")

  let auto_config =
    trove.Config(
      path: "/tmp/trove_auto_compact_example",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
      auto_compact: trove.AutoCompact(min_dirt: 3, min_dirt_factor: 0.1),
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db2) = trove.open(auto_config)

  trove.put(db2, key: "x", value: "1")
  io.println(
    "after 1 write: dirt_factor = " <> float.to_string(trove.dirt_factor(db2)),
  )

  trove.put(db2, key: "x", value: "2")
  trove.put(db2, key: "x", value: "3")
  trove.put(db2, key: "x", value: "4")
  io.println(
    "after 4 overwrites: dirt_factor = "
    <> float.to_string(trove.dirt_factor(db2))
    <> " (auto-compact triggered)",
  )

  let assert Ok("4") = trove.get(db2, key: "x")
  io.println("  x = 4 (data intact)")

  trove.close(db2)
  io.println("\ndone")
}
