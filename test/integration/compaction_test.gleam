import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleam/yielder
import non_empty_list
import simplifile
import trove
import trove/codec
import trove/internal/btree/node
import trove/internal/store
import trove/range
import trove/test_helpers

pub fn compact_empty_database_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  trove.put(db, key: 1, value: "after_compact")
  let assert Ok("after_compact") = trove.get(db, key: 1)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_blank_compaction_file_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "hello")
  trove.put(db, key: 2, value: "world")
  trove.close(db)

  let assert Ok(Nil) = simplifile.create_file(dir <> "/1.trv")

  let assert Ok(db2) = trove.open(config)
  let assert Ok("hello") = trove.get(db2, key: 1)
  let assert Ok("world") = trove.get(db2, key: 2)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_truncated_compaction_file_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "data")
  trove.close(db)

  let assert Ok(Nil) = simplifile.write_bits(dir <> "/1.trv", <<0, 0, 0>>)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("data") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// Compaction must not append onto a leftover partial .trv from a prior failure.
/// The compacted file should be the same size whether or not stale data existed.
pub fn compaction_cleans_stale_target_file_test() {
  // First: compact with no stale file to get the baseline size
  let dir1 = test_helpers.temp_dir()
  let config1 = test_helpers.int_string_config(dir1)
  let assert Ok(db1) = trove.open(config1)
  list.each(test_helpers.int_list(from: 1, to: 10), fn(i) {
    trove.put(db1, key: i, value: "val" <> int.to_string(i))
  })
  let assert Ok(Nil) = trove.compact(db1, timeout: 60_000)
  trove.close(db1)
  let assert Ok(baseline_info) = simplifile.file_info(dir1 <> "/1.trv")
  let baseline_size = baseline_info.size

  // Second: compact with a stale file planted beforehand
  let dir2 = test_helpers.temp_dir()
  let config2 = test_helpers.int_string_config(dir2)
  let assert Ok(db2) = trove.open(config2)
  list.each(test_helpers.int_list(from: 1, to: 10), fn(i) {
    trove.put(db2, key: i, value: "val" <> int.to_string(i))
  })
  // Must be larger than block_size (1024) so padding can't absorb it
  let garbage = list.repeat("x", 2048) |> list.fold("", fn(acc, c) { acc <> c })
  let assert Ok(Nil) = simplifile.write(dir2 <> "/1.trv", garbage)
  let assert Ok(Nil) = trove.compact(db2, timeout: 60_000)
  trove.close(db2)
  let assert Ok(actual_info) = simplifile.file_info(dir2 <> "/1.trv")

  // The compacted file must be the same size as baseline — no stale prefix
  assert actual_info.size == baseline_size

  let assert Ok(_) = simplifile.delete_all([dir1])
  let assert Ok(_) = simplifile.delete_all([dir2])
  Nil
}

pub fn recover_skips_file_with_corrupt_root_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "safe")
  trove.close(db)

  // Create 1.trv with a valid header pointing to non-existent root offset.
  // get_latest_header will succeed, but get_node at that offset will fail.
  let assert Ok(s) = store.open(path: dir <> "/1.trv")
  let header =
    store.Header(root: option.Some(9999), size: 1, dirt: 0, keyspaces: [])
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("safe") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_file_with_inconsistent_header_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: 1, value: "safe")
  trove.close(db)

  // Create 1.trv with header root: None, size: 5 (semantically invalid)
  let assert Ok(s) = store.open(path: dir <> "/1.trv")
  let header = store.Header(root: option.None, size: 5, dirt: 0, keyspaces: [])
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("safe") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_file_with_some_root_zero_size_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: 1, value: "safe")
  trove.close(db)

  // Build a structurally valid tree in 1.trv but with size: 0
  let key_codec = codec.int()
  let value_codec = codec.string()
  let data_bytes =
    node.encode_data_node(node: node.Value("bogus"), value_codec: value_codec)
  let assert Ok(s) = store.open(path: dir <> "/1.trv")
  let assert Ok(data_loc) = store.put_node(store: s, data: data_bytes)
  let leaf_bytes =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(1, data_loc), [])),
      key_codec: key_codec,
    )
  let assert Ok(root_offset) = store.put_node(store: s, data: leaf_bytes)
  // Header has valid root but size: 0 — semantically invalid
  let header =
    store.Header(
      root: option.Some(root_offset),
      size: 0,
      dirt: 0,
      keyspaces: [],
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("safe") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn writes_work_after_compaction_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  list.each(test_helpers.int_list(from: 0, to: 49), fn(i) {
    trove.put(db, key: i, value: "v")
  })
  list.each(test_helpers.int_list(from: 0, to: 49), fn(i) {
    trove.delete(db, key: i)
  })

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  trove.put(db, key: 100, value: "after")
  let assert Ok("after") = trove.get(db, key: 100)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn compact_failure_preserves_actor_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  list.each(test_helpers.int_list(from: 1, to: 10), fn(i) {
    trove.put(db, key: i, value: "v" <> int.to_string(i))
  })

  // Block compaction by creating a directory where the target file would go
  let assert Ok(Nil) = simplifile.create_directory_all(dir <> "/1.trv")

  let assert Error(_) = trove.compact(db, timeout: 60_000)

  list.each(test_helpers.int_list(from: 1, to: 10), fn(i) {
    let assert Ok(val) = trove.get(db, key: i)
    assert val == "v" <> int.to_string(i)
  })

  trove.put(db, key: 100, value: "after_failed_compact")
  let assert Ok("after_failed_compact") = trove.get(db, key: 100)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn snapshot_survives_compaction_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let entries =
    test_helpers.int_list(from: 0, to: 49)
    |> list.map(fn(i) { #(i, "v" <> int.to_string(i)) })
  trove.put_multi(db, entries: entries)

  // Acquire snapshot, compact (which deletes the old .trv file), then read
  let snap_data =
    trove.with_snapshot(db, fn(snap) {
      let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

      let results =
        trove.snapshot_range(
          snapshot: snap,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      yielder.to_list(results)
    })

  assert list.length(snap_data) == 50
  list.each(snap_data, fn(entry) {
    assert entry.1 == "v" <> int.to_string(entry.0)
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn old_files_preserved_until_compaction_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)

  let assert Ok(db) = trove.open(config)
  trove.put_multi(db, entries: test_helpers.make_entries(1, 10))
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
  trove.close(db)

  // Plant 0.trv back to simulate a crash that left the old file behind
  let assert Ok(data) = simplifile.read_bits(dir <> "/1.trv")
  let assert Ok(Nil) = simplifile.write_bits(dir <> "/0.trv", data)

  let assert Ok(db2) = trove.open(config)
  let assert Ok(files) = simplifile.read_directory(dir)
  let trv_files =
    list.filter(files, fn(f) { string.ends_with(f, ".trv") })
    |> list.sort(string.compare)
  assert trv_files == ["0.trv", "1.trv"]

  let assert Ok(Nil) = trove.compact(db2, timeout: 60_000)
  let assert Ok(files2) = simplifile.read_directory(dir)
  let trv_files2 = list.filter(files2, fn(f) { string.ends_with(f, ".trv") })
  assert trv_files2 == ["2.trv"]

  let assert Ok("val1") = trove.get(db2, key: 1)

  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn open_preserves_store_error_detail_test() {
  let dir = test_helpers.temp_dir()
  let assert Ok(Nil) = simplifile.create_directory_all(dir <> "/0.trv")

  let config = test_helpers.int_string_config(dir)
  let assert Error(trove.StoreError(_)) = trove.open(config)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_file_with_valid_root_but_corrupt_children_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "safe")
  trove.close(db)

  // Create 1.trv with a structurally valid leaf node whose entry locations
  // point to non-existent offsets. The root itself is readable and passes
  // validate_structure, but its children (data locations) are bogus.
  let key_codec = codec.int()
  let leaf = node.Leaf(non_empty_list.new(#(1, 99_999), [#(2, 99_998)]))
  let leaf_data = node.encode_tree_node(node: leaf, key_codec: key_codec)

  let assert Ok(s) = store.open(path: dir <> "/1.trv")
  let assert Ok(root_offset) = store.put_node(store: s, data: leaf_data)
  let header =
    store.Header(
      root: option.Some(root_offset),
      size: 2,
      dirt: 0,
      keyspaces: [],
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("safe") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recovery_cleans_up_older_store_files_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)

  let assert Ok(db) = trove.open(config)
  trove.put_multi(db, entries: test_helpers.make_entries(1, 5))
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
  trove.close(db)

  // Plant 0.trv and 1.trv back to simulate leftover files
  let assert Ok(data) = simplifile.read_bits(dir <> "/2.trv")
  let assert Ok(Nil) = simplifile.write_bits(dir <> "/0.trv", data)
  let assert Ok(Nil) = simplifile.write_bits(dir <> "/1.trv", data)

  let assert Ok(db2) = trove.open(config)
  let assert Ok(files) = simplifile.read_directory(dir)
  let trv_files =
    list.filter(files, fn(f) { string.ends_with(f, ".trv") })
    |> list.sort(string.compare)
  assert trv_files == ["1.trv", "2.trv"]

  let assert Ok("val1") = trove.get(db2, key: 1)
  let assert Ok("val5") = trove.get(db2, key: 5)

  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn recover_skips_file_with_corrupt_keyspace_root_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: 1, value: "safe")
  trove.close(db)

  // Plant 1.trv with a valid default root pointing nowhere dangerous but a
  // named-keyspace root pointing at a non-existent offset. Recovery must
  // skip this file and fall back to 0.trv.
  let assert Ok(s) = store.open(path: dir <> "/1.trv")
  let header =
    store.Header(root: option.None, size: 0, dirt: 0, keyspaces: [
      store.KeyspaceHeader(
        name: "users",
        root: option.Some(9999),
        size: 1,
        dirt: 0,
      ),
    ])
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("safe") = trove.get(db2, key: 1)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn auto_compact_fires_on_keyspace_dirt_test() {
  let dir = test_helpers.temp_dir()
  let config =
    trove.Config(
      ..test_helpers.int_string_config(dir),
      auto_compact: trove.AutoCompact(min_dirt: 1, min_dirt_factor: 0.01),
    )
  let assert Ok(db) = trove.open(config)
  let users =
    trove.keyspace(
      db,
      name: "users",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
    )

  // Only the named keyspace gets writes/deletes. The aggregate dirt must
  // cross the threshold and auto-compact must fire, leaving the live
  // entry intact and dropping dirt to zero.
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.put_in(db, keyspace: users, key: "alice", value: "admin2")
  trove.put_in(db, keyspace: users, key: "bob", value: "member")
  trove.delete_in(db, keyspace: users, key: "bob")

  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("admin2")
  assert trove.get_in(db, keyspace: users, key: "bob") == Error(Nil)
  assert trove.dirt_factor(db) == 0.0

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn compact_preserves_multiple_keyspaces_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let users =
    trove.keyspace(
      db,
      name: "users",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
    )
  let counters =
    trove.keyspace(
      db,
      name: "counters",
      key_codec: codec.string(),
      value_codec: codec.int(),
      key_compare: string.compare,
    )

  trove.put(db, key: 1, value: "default_one")
  trove.put(db, key: 2, value: "default_two")
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.put_in(db, keyspace: users, key: "bob", value: "member")
  trove.put_in(db, keyspace: counters, key: "visits", value: 42)
  trove.delete(db, key: 2)
  trove.delete_in(db, keyspace: users, key: "bob")

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  assert trove.get(db, key: 1) == Ok("default_one")
  assert trove.get(db, key: 2) == Error(Nil)
  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("admin")
  assert trove.get_in(db, keyspace: users, key: "bob") == Error(Nil)
  assert trove.get_in(db, keyspace: counters, key: "visits") == Ok(42)

  // Compaction collapses dirt to zero
  assert trove.dirt_factor(db) == 0.0

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}
