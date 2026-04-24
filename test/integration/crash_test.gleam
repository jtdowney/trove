import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import non_empty_list
import qcheck
import simplifile
import trove
import trove/codec
import trove/internal/btree
import trove/internal/btree/node
import trove/internal/store
import trove/range
import trove/test_helpers

const block_size = 1024

fn sample_offsets(from: Int, to: Int) -> List(Int) {
  // Test at: start, end-1, block boundaries, and every 50th byte
  let base =
    [from, to - 1]
    |> list.append(block_boundaries(from, to))
    |> list.append(every_nth(from, to, 50))
    |> list.unique
    |> list.filter(fn(x) { x >= from && x < to })
    |> list.sort(int.compare)
  base
}

fn block_boundaries(from: Int, to: Int) -> List(Int) {
  let first_block = { from / block_size + 1 } * block_size
  do_block_boundaries(first_block, to, [])
}

fn do_block_boundaries(current: Int, max: Int, acc: List(Int)) -> List(Int) {
  case current >= max {
    True -> list.reverse(acc)
    False ->
      do_block_boundaries(current + block_size, max, [
        current,
        // Also test 1 byte before and after block boundary
        current - 1,
        current + 1,
        // Test mid-header (header is 26 bytes from block boundary)
        current + 13,
        current + 25,
        current + 42,
        ..acc
      ])
  }
}

fn every_nth(from: Int, to: Int, step: Int) -> List(Int) {
  do_every_nth(from, to, step, [])
}

fn do_every_nth(
  current: Int,
  max: Int,
  step: Int,
  acc: List(Int),
) -> List(Int) {
  case current >= max {
    True -> list.reverse(acc)
    False -> do_every_nth(current + step, max, step, [current, ..acc])
  }
}

fn verify_header_recovery(
  path: String,
  full_bytes: BitArray,
  size: Int,
  expected_header: store.Header,
  expected_key: Int,
  expected_value: String,
) -> Nil {
  let assert Ok(truncated) = bit_array.slice(full_bytes, 0, size)
  let assert Ok(Nil) = simplifile.write_bits(path, truncated)

  let assert Ok(s2) = store.open(path: path)
  let assert Ok(recovered) = store.get_latest_header(store: s2)
  assert recovered.root == expected_header.root
  assert recovered.size == expected_header.size
  assert recovered.dirt == expected_header.dirt

  let assert Ok(recovered_tree) =
    btree.from_header(
      root: recovered.root,
      size: recovered.size,
      dirt: recovered.dirt,
      capacity: 32,
    )
  let assert Ok(option.Some(val)) =
    test_helpers.lookup(tree: recovered_tree, store: s2, key: expected_key)
  assert val == expected_value
  let assert Ok(Nil) = store.close(store: s2)
  Nil
}

pub fn crash_recovery_between_commits_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/crash.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header1 =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
      keyspaces: [],
    )
  let assert Ok(h1_offset) = store.put_header(store: s, header: header1)
  let committed_1 = h1_offset + store.encoded_size(header: header1)

  let assert Ok(tree2) =
    test_helpers.insert(tree: tree1, store: s, key: 2, value: "v2")
  let header2 =
    store.Header(
      root: btree.root(tree2),
      size: btree.size(tree2),
      dirt: btree.dirt(tree2),
      keyspaces: [],
    )
  let assert Ok(h2_offset) = store.put_header(store: s, header: header2)
  let committed_2 = h2_offset + store.encoded_size(header: header2)

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(full_bytes) = simplifile.read_bits(path)
  assert bit_array.byte_size(full_bytes) == committed_2

  // Truncate at sampled points between state 1 and state 2.
  // Recovery must return header 1 and its data must be readable.
  sample_offsets(committed_1, committed_2)
  |> list.each(fn(size) {
    verify_header_recovery(path, full_bytes, size, header1, 1, "v1")
  })

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn crash_recovery_before_first_commit_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/crash.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header1 =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
      keyspaces: [],
    )
  let assert Ok(h1_offset) = store.put_header(store: s, header: header1)
  let committed_1 = h1_offset + store.encoded_size(header: header1)

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(full_bytes) = simplifile.read_bits(path)

  // Truncate at sampled points before state 1.
  // Recovery must return Error (no valid header).
  sample_offsets(1, committed_1)
  |> list.each(fn(size) {
    let assert Ok(truncated) = bit_array.slice(full_bytes, 0, size)
    let assert Ok(Nil) = simplifile.write_bits(path, truncated)

    let assert Ok(s2) = store.open(path: path)
    let assert Error(_) = store.get_latest_header(store: s2)
    let assert Ok(Nil) = store.close(store: s2)
  })

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn crash_recovery_exact_commit_boundary_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/crash.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header1 =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
      keyspaces: [],
    )
  let assert Ok(h1_offset) = store.put_header(store: s, header: header1)
  let committed_1 = h1_offset + store.encoded_size(header: header1)

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(full_bytes) = simplifile.read_bits(path)

  // Truncate at exactly the first commit boundary. Recovery must succeed.
  verify_header_recovery(path, full_bytes, committed_1, header1, 1, "v1")

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn crash_recovery_empty_file_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/crash.db"
  let assert Ok(Nil) = simplifile.create_file(path)

  let assert Ok(s) = store.open(path: path)
  let assert Ok(True) = store.blank(store: s)
  let assert Error(_) = store.get_latest_header(store: s)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn crash_recovery_db_level_property_test() {
  let key_gen = qcheck.bounded_int(0, 49)
  let value_gen = qcheck.non_empty_string()
  let entry_gen = qcheck.tuple2(key_gen, value_gen)

  use #(entries1, entries2) <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(10),
    qcheck.tuple2(
      qcheck.generic_list(
        elements_from: entry_gen,
        length_from: qcheck.bounded_int(1, 20),
      ),
      qcheck.generic_list(
        elements_from: entry_gen,
        length_from: qcheck.bounded_int(1, 20),
      ),
    ),
  )

  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let file_path = dir <> "/0.trv"

  let assert Ok(db) = trove.open(config)
  let unique1 = dict.from_list(entries1) |> dict.to_list
  trove.put_multi(db, entries: unique1)
  trove.close(db)

  let assert Ok(committed_1_bytes) = simplifile.read_bits(file_path)
  let size_1 = bit_array.byte_size(committed_1_bytes)

  let assert Ok(db2) = trove.open(config)
  let unique2 = dict.from_list(entries2) |> dict.to_list
  trove.put_multi(db2, entries: unique2)
  trove.close(db2)

  let assert Ok(full_bytes) = simplifile.read_bits(file_path)
  let size_2 = bit_array.byte_size(full_bytes)

  let trunc_size = size_1 + { size_2 - size_1 } / 2
  let reference1 = dict.from_list(entries1)
  let reference2 = dict.merge(into: reference1, from: dict.from_list(unique2))
  case trunc_size > size_1 && trunc_size < size_2 {
    True -> {
      let assert Ok(truncated) = bit_array.slice(full_bytes, 0, trunc_size)
      let assert Ok(Nil) = simplifile.write_bits(file_path, truncated)

      let assert Ok(db3) = trove.open(config)

      dict.each(reference1, fn(key, value) {
        let assert Ok(val) = trove.get(db3, key: key)
        assert val == value
      })

      let expected =
        dict.to_list(reference1)
        |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
      let actual =
        trove.range(
          db3,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      assert actual == expected

      trove.close(db3)
      Nil
    }
    False -> {
      // Sizes too close for meaningful truncation — verify state 2
      // is intact so the test iteration exercises something
      let assert Ok(db3) = trove.open(config)
      dict.each(reference2, fn(key, value) {
        let assert Ok(val) = trove.get(db3, key: key)
        assert val == value
      })

      let expected =
        dict.to_list(reference2)
        |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
      let actual =
        trove.range(
          db3,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      assert actual == expected

      trove.close(db3)
      Nil
    }
  }

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn crash_recovery_three_states_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/crash.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "a")
  let h1 =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
      keyspaces: [],
    )
  let assert Ok(h1_off) = store.put_header(store: s, header: h1)
  let c1 = h1_off + store.encoded_size(header: h1)

  let assert Ok(tree2) =
    test_helpers.insert(tree: tree1, store: s, key: 2, value: "b")
  let h2 =
    store.Header(
      root: btree.root(tree2),
      size: btree.size(tree2),
      dirt: btree.dirt(tree2),
      keyspaces: [],
    )
  let assert Ok(h2_off) = store.put_header(store: s, header: h2)
  let c2 = h2_off + store.encoded_size(header: h2)

  let assert Ok(tree3) =
    test_helpers.insert(tree: tree2, store: s, key: 3, value: "c")
  let h3 =
    store.Header(
      root: btree.root(tree3),
      size: btree.size(tree3),
      dirt: btree.dirt(tree3),
      keyspaces: [],
    )
  let assert Ok(h3_off) = store.put_header(store: s, header: h3)
  let c3 = h3_off + store.encoded_size(header: h3)

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(full_bytes) = simplifile.read_bits(path)

  // Truncate between state 2 and state 3 → must recover state 2
  sample_offsets(c2, c3)
  |> list.each(fn(size) {
    verify_header_recovery(path, full_bytes, size, h2, 1, "a")
  })

  // Truncate between state 1 and state 2 → must recover state 1
  sample_offsets(c1, c2)
  |> list.each(fn(size) {
    verify_header_recovery(path, full_bytes, size, h1, 1, "a")
  })

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn deep_corruption_rejected_during_validation_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/0.trv"
  let assert Ok(s) = store.open(path: path)

  // capacity=2 forces splits, creating branch → leaf → data node structure
  let tree0 = btree.new_with_capacity(capacity: 2)
  let assert Ok(tree1) =
    btree.insert(
      tree: tree0,
      store: s,
      key: 1,
      value: "a",
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  let assert Ok(tree2) =
    btree.insert(
      tree: tree1,
      store: s,
      key: 2,
      value: "b",
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  let assert Ok(tree3) =
    btree.insert(
      tree: tree2,
      store: s,
      key: 3,
      value: "c",
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  let header =
    store.Header(
      root: btree.root(tree3),
      size: btree.size(tree3),
      dirt: btree.dirt(tree3),
      keyspaces: [],
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  // Corrupt byte 10 — deep in the data node region (written first)
  let assert Ok(bytes) = simplifile.read_bits(path)
  let assert Ok(before) = bit_array.slice(bytes, 0, 10)
  let assert Ok(<<target_byte>>) = bit_array.slice(bytes, 10, 1)
  let file_size = bit_array.byte_size(bytes)
  let assert Ok(after) = bit_array.slice(bytes, 11, file_size - 11)
  let flipped = int.bitwise_exclusive_or(target_byte, 0xFF)
  let corrupted = bit_array.concat([before, <<flipped>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let config = test_helpers.int_string_config(dir)
  let result = trove.open(config)
  assert case result {
    Error(trove.StoreError(_)) -> True
    _ -> False
  }

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn leaf_pointing_to_tree_node_rejected_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/0.trv"
  let assert Ok(s) = store.open(path: path)

  let data_bytes =
    node.encode_data_node(node: node.Value("x"), value_codec: codec.string())
  let assert Ok(data_loc) = store.put_node(store: s, data: data_bytes)

  let leaf_bytes =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(1, data_loc), [])),
      key_codec: codec.int(),
    )
  let assert Ok(leaf_loc) = store.put_node(store: s, data: leaf_bytes)

  // Leaf pointing to a tree node (leaf_loc) instead of a data node
  let bad_leaf_bytes =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(2, leaf_loc), [])),
      key_codec: codec.int(),
    )
  let assert Ok(bad_leaf_loc) = store.put_node(store: s, data: bad_leaf_bytes)

  let header =
    store.Header(
      root: option.Some(bad_leaf_loc),
      size: 1,
      dirt: 0,
      keyspaces: [],
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let config = test_helpers.int_string_config(dir)
  let result = trove.open(config)
  assert case result {
    Error(trove.StoreError(_)) -> True
    _ -> False
  }

  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn branch_pointing_to_data_node_rejected_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/0.trv"
  let assert Ok(s) = store.open(path: path)

  let data_bytes =
    node.encode_data_node(node: node.Value("x"), value_codec: codec.string())
  let assert Ok(data_loc) = store.put_node(store: s, data: data_bytes)

  // Branch pointing to a data node instead of a tree node
  let bad_branch_bytes =
    node.encode_tree_node(
      node: node.Branch(non_empty_list.new(#(1, data_loc), [])),
      key_codec: codec.int(),
    )
  let assert Ok(bad_branch_loc) =
    store.put_node(store: s, data: bad_branch_bytes)

  let header =
    store.Header(
      root: option.Some(bad_branch_loc),
      size: 1,
      dirt: 0,
      keyspaces: [],
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let config = test_helpers.int_string_config(dir)
  let result = trove.open(config)
  assert case result {
    Error(trove.StoreError(_)) -> True
    _ -> False
  }

  let assert Ok(_) = simplifile.delete_all([dir])
}
