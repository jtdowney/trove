import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option
import qcheck
import simplifile
import trove/internal/btree
import trove/internal/store.{ChecksumMismatch, NoHeaderFound, NodeExceedsBounds}
import trove/test_helpers

pub fn blank_new_store_test() {
  use s <- test_helpers.with_store()
  let assert Ok(True) = store.blank(store: s)
  Nil
}

pub fn blank_after_write_test() {
  use s <- test_helpers.with_store()
  let assert Ok(_) = store.put_node(store: s, data: <<"hello":utf8>>)
  let assert Ok(False) = store.blank(store: s)
  Nil
}

pub fn put_node_and_get_node_roundtrip_test() {
  use s <- test_helpers.with_store()
  let data = <<"hello world":utf8>>
  let assert Ok(loc) = store.put_node(store: s, data: data)
  let assert Ok(read_back) = store.get_node(store: s, location: loc)
  assert read_back == data
}

pub fn multiple_nodes_distinct_locations_test() {
  use s <- test_helpers.with_store()
  let d1 = <<"alpha":utf8>>
  let d2 = <<"bravo":utf8>>
  let d3 = <<"charlie":utf8>>

  let assert Ok(l1) = store.put_node(store: s, data: d1)
  let assert Ok(l2) = store.put_node(store: s, data: d2)
  let assert Ok(l3) = store.put_node(store: s, data: d3)

  assert l1 != l2
  assert l2 != l3
  assert l1 != l3

  let assert Ok(r1) = store.get_node(store: s, location: l1)
  let assert Ok(r2) = store.get_node(store: s, location: l2)
  let assert Ok(r3) = store.get_node(store: s, location: l3)

  assert r1 == d1
  assert r2 == d2
  assert r3 == d3
}

pub fn put_header_and_recover_test() {
  use s <- test_helpers.with_store()
  let header = store.Header(root: option.Some(42), size: 10, dirt: 3)
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == header
}

pub fn latest_header_returns_last_test() {
  use s <- test_helpers.with_store()
  let h1 = store.Header(root: option.Some(1), size: 10, dirt: 0)
  let h2 = store.Header(root: option.Some(2), size: 20, dirt: 5)

  let assert Ok(_) = store.put_header(store: s, header: h1)
  let assert Ok(_) = store.put_header(store: s, header: h2)

  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == h2
}

pub fn empty_store_no_header_test() {
  use s <- test_helpers.with_store()
  let assert Error(_) = store.get_latest_header(store: s)
  Nil
}

pub fn nodes_between_headers_dont_interfere_test() {
  use s <- test_helpers.with_store()
  let h1 = store.Header(root: option.Some(100), size: 5, dirt: 1)
  let assert Ok(_) = store.put_header(store: s, header: h1)

  let assert Ok(_) = store.put_node(store: s, data: <<"noise1":utf8>>)
  let assert Ok(_) = store.put_node(store: s, data: <<"noise2":utf8>>)

  let h2 = store.Header(root: option.Some(200), size: 15, dirt: 3)
  let assert Ok(_) = store.put_header(store: s, header: h2)

  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == h2
}

pub fn header_with_no_root_roundtrip_test() {
  use s <- test_helpers.with_store()
  let header = store.Header(root: option.None, size: 0, dirt: 0)
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == header
}

pub fn open_reader_can_read_existing_data_test() {
  use s <- test_helpers.with_store()
  let data = <<"test data":utf8>>
  let assert Ok(loc) = store.put_node(store: s, data: data)

  let assert Ok(reader) = store.open_reader(store: s)
  let assert Ok(read_back) = store.get_node(store: reader, location: loc)
  assert read_back == data
  let assert Ok(Nil) = store.close(store: reader)
  Nil
}

pub fn node_roundtrip_property_test() {
  let chunk_gen =
    qcheck.generic_byte_aligned_bit_array(
      values_from: qcheck.bounded_int(from: 0, to: 255),
      byte_size_from: qcheck.bounded_int(from: 1, to: 200),
    )

  use chunk <- qcheck.run(test_helpers.property_config(), chunk_gen)

  let dir = test_helpers.temp_dir()
  let path = dir <> "/prop.db"
  let assert Ok(s) = store.open(path: path)

  let assert Ok(loc) = store.put_node(store: s, data: chunk)
  let assert Ok(read_back) = store.get_node(store: s, location: loc)
  assert read_back == chunk

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn multiple_nodes_roundtrip_property_test() {
  let length_gen = qcheck.bounded_int(from: 1, to: 10)
  let chunk_gen =
    qcheck.generic_byte_aligned_bit_array(
      values_from: qcheck.bounded_int(from: 0, to: 255),
      byte_size_from: qcheck.bounded_int(from: 1, to: 100),
    )
  let chunks_gen =
    qcheck.generic_list(elements_from: chunk_gen, length_from: length_gen)

  use chunks <- qcheck.run(test_helpers.property_config(), chunks_gen)

  let dir = test_helpers.temp_dir()
  let path = dir <> "/prop_multi.db"
  let assert Ok(s) = store.open(path: path)

  let locations =
    list.map(chunks, fn(chunk) {
      let assert Ok(loc) = store.put_node(store: s, data: chunk)
      loc
    })
  let pairs = list.zip(locations, chunks)

  list.each(pairs, fn(pair) {
    let #(loc, expected) = pair
    let assert Ok(read_back) = store.get_node(store: s, location: loc)
    assert read_back == expected
  })

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn get_node_invalid_location_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Ok(_) = store.put_node(store: s, data: <<"hello":utf8>>)
  let header = store.Header(root: option.Some(42), size: 1, dirt: 0)
  let assert Ok(header_loc) = store.put_header(store: s, header: header)
  let assert Error(_) = store.get_node(store: s, location: header_loc)
  Nil
}

pub fn header_roundtrip_property_test() {
  let root_gen =
    qcheck.from_generators(qcheck.return(option.None), [
      qcheck.map(qcheck.bounded_int(from: 0, to: 100_000), option.Some),
    ])
  let header_gen =
    qcheck.map3(
      root_gen,
      qcheck.bounded_int(0, 10_000),
      qcheck.bounded_int(0, 10_000),
      fn(root, size, dirt) { store.Header(root: root, size: size, dirt: dirt) },
    )

  use header <- qcheck.run(test_helpers.property_config(), header_gen)

  let dir = test_helpers.temp_dir()
  let path = dir <> "/prop_header.db"
  let assert Ok(s) = store.open(path: path)

  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == header

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn truncated_header_recovery_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/test.db"
  let assert Ok(s) = store.open(path: path)

  let valid_header = store.Header(root: option.Some(42), size: 10, dirt: 3)
  let assert Ok(_) = store.put_header(store: s, header: valid_header)
  let assert Ok(Nil) = store.close(store: s)

  // The valid header is 42 bytes at offset 0. Pad to the next block boundary
  // (1024) and write a 0x2A marker followed by truncated garbage.
  let padding_size = 1024 - store.header_size
  let padding = <<0:size(padding_size)-unit(8)>>
  let corrupt = <<0x2A, 0xFF, 0xFF>>
  let assert Ok(Nil) =
    simplifile.append_bits(path, <<padding:bits, corrupt:bits>>)

  let assert Ok(s2) = store.open(path: path)
  let assert Ok(recovered) = store.get_latest_header(store: s2)
  assert recovered == valid_header

  let assert Ok(Nil) = store.close(store: s2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn corrupt_only_file_returns_no_header_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/test.db"

  // Write a file containing only a 0x2A marker and truncated garbage at
  // offset 0. No valid header exists. The scan should skip the corrupt
  // marker and report "no header found" rather than "corrupt header".
  let assert Ok(Nil) = simplifile.write_bits(path, <<0x2A, 0xDE, 0xAD>>)

  let assert Ok(s) = store.open(path: path)
  let assert Error(NoHeaderFound) = store.get_latest_header(store: s)

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn corrupted_header_marker_rejects_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/corrupt.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
    )
  let assert Ok(h_offset) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(bytes) = simplifile.read_bits(path)

  let assert Ok(before) = bit_array.slice(bytes, 0, h_offset)
  let assert Ok(after) =
    bit_array.slice(
      bytes,
      h_offset + 1,
      bit_array.byte_size(bytes) - h_offset - 1,
    )
  let corrupted = bit_array.concat([before, <<0x00>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let assert Ok(s2) = store.open(path: path)
  let assert Error(_) = store.get_latest_header(store: s2)
  let assert Ok(Nil) = store.close(store: s2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn corrupted_header_size_field_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/corrupt.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
    )
  let assert Ok(h_offset) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(bytes) = simplifile.read_bits(path)

  let size_offset = h_offset + 10
  let assert Ok(before) = bit_array.slice(bytes, 0, size_offset)
  let assert Ok(after) =
    bit_array.slice(
      bytes,
      size_offset + 8,
      bit_array.byte_size(bytes) - size_offset - 8,
    )
  let corrupted =
    bit_array.concat([before, <<999_999_999:int-big-size(64)>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let assert Ok(s2) = store.open(path: path)
  let assert Error(_) = store.get_latest_header(store: s2)
  let assert Ok(Nil) = store.close(store: s2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn corrupted_node_marker_rejects_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/corrupt.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let header =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
    )
  let assert Ok(_) = store.put_header(store: s, header: header)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(bytes) = simplifile.read_bits(path)

  let assert Ok(after) =
    bit_array.slice(bytes, 1, bit_array.byte_size(bytes) - 1)
  let corrupted = bit_array.concat([<<0xFF>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let assert Ok(s2) = store.open(path: path)
  let assert Ok(recovered) = store.get_latest_header(store: s2)

  let assert Ok(recovered_tree) =
    btree.from_header(
      root: recovered.root,
      size: recovered.size,
      dirt: recovered.dirt,
      capacity: 32,
    )

  let assert Error(_) =
    test_helpers.lookup(tree: recovered_tree, store: s2, key: 1)
  let assert Ok(Nil) = store.close(store: s2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn random_bit_flip_store_recovery_property_test() {
  use flip_offset <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(25),
    qcheck.bounded_int(0, 2000),
  )

  let dir = test_helpers.temp_dir()
  let path = dir <> "/flip.db"
  let assert Ok(s) = store.open(path: path)

  let tree0 = btree.new()
  let assert Ok(tree1) =
    test_helpers.insert(tree: tree0, store: s, key: 1, value: "v1")
  let h1 =
    store.Header(
      root: btree.root(tree1),
      size: btree.size(tree1),
      dirt: btree.dirt(tree1),
    )
  let assert Ok(_) = store.put_header(store: s, header: h1)

  let assert Ok(tree2) =
    test_helpers.insert(tree: tree1, store: s, key: 2, value: "v2")
  let h2 =
    store.Header(
      root: btree.root(tree2),
      size: btree.size(tree2),
      dirt: btree.dirt(tree2),
    )
  let assert Ok(_) = store.put_header(store: s, header: h2)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(bytes) = simplifile.read_bits(path)
  let file_size = bit_array.byte_size(bytes)
  let byte_pos = flip_offset % file_size

  let assert Ok(before) = bit_array.slice(bytes, 0, byte_pos)
  let assert Ok(<<target_byte>>) = bit_array.slice(bytes, byte_pos, 1)
  let assert Ok(after) =
    bit_array.slice(bytes, byte_pos + 1, file_size - byte_pos - 1)
  let flipped = int.bitwise_exclusive_or(target_byte, 1)
  let corrupted = bit_array.concat([before, <<flipped>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let assert Ok(s2) = store.open(path: path)
  case store.get_latest_header(store: s2) {
    Ok(recovered) -> {
      assert recovered == h1 || recovered == h2
      Nil
    }
    Error(_) -> Nil
  }
  let assert Ok(Nil) = store.close(store: s2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn interleaved_header_recovery_property_test() {
  let header_count_gen = qcheck.bounded_int(1, 5)
  let nodes_between_gen = qcheck.bounded_int(0, 5)
  let chunk_gen =
    qcheck.generic_byte_aligned_bit_array(
      values_from: qcheck.bounded_int(from: 0, to: 255),
      byte_size_from: qcheck.bounded_int(from: 1, to: 50),
    )

  use #(header_count, nodes_between, chunk) <- qcheck.run(
    test_helpers.property_config(),
    qcheck.tuple3(header_count_gen, nodes_between_gen, chunk_gen),
  )

  let dir = test_helpers.temp_dir()
  let path = dir <> "/interleaved.db"
  let assert Ok(s) = store.open(path: path)

  let last_header =
    list.fold(
      test_helpers.int_list(from: 1, to: header_count),
      option.None,
      fn(_, i) {
        list.each(test_helpers.int_list(from: 1, to: nodes_between), fn(_) {
          let assert Ok(_) = store.put_node(store: s, data: chunk)
          Nil
        })

        let header =
          store.Header(root: option.Some(i * 100), size: i, dirt: i - 1)
        let assert Ok(_) = store.put_header(store: s, header: header)
        option.Some(header)
      },
    )

  let assert option.Some(expected) = last_header
  let assert Ok(recovered) = store.get_latest_header(store: s)
  assert recovered == expected

  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn get_node_checksum_mismatch_returns_error_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/test.db"
  let assert Ok(s) = store.open(path: path)

  let data = <<"hello world":utf8>>
  let assert Ok(loc) = store.put_node(store: s, data: data)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(bytes) = simplifile.read_bits(path)
  let corrupt_offset = loc + 5
  let assert Ok(before) = bit_array.slice(bytes, 0, corrupt_offset)
  let assert Ok(<<target_byte>>) = bit_array.slice(bytes, corrupt_offset, 1)
  let assert Ok(after) =
    bit_array.slice(
      bytes,
      corrupt_offset + 1,
      bit_array.byte_size(bytes) - corrupt_offset - 1,
    )
  let flipped = int.bitwise_exclusive_or(target_byte, 0xFF)
  let corrupted = bit_array.concat([before, <<flipped>>, after])
  let assert Ok(Nil) = simplifile.write_bits(path, corrupted)

  let assert Ok(s2) = store.open(path: path)
  let assert Error(ChecksumMismatch) = store.get_node(store: s2, location: loc)
  let assert Ok(Nil) = store.close(store: s2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn get_node_truncated_node_returns_error_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/test.db"

  let fake_size = 1000
  let payload = <<0x00, fake_size:32, 0xDE, 0xAD>>
  let assert Ok(Nil) = simplifile.write_bits(path, payload)

  let assert Ok(s) = store.open(path: path)
  let assert Error(NodeExceedsBounds) = store.get_node(store: s, location: 0)
  let assert Ok(Nil) = store.close(store: s)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn sync_returns_ok_test() {
  use s <- test_helpers.with_store()
  let assert Ok(_) = store.put_node(store: s, data: <<"hello":utf8>>)
  let assert Ok(Nil) = store.sync(store: s)
  Nil
}

pub fn current_offset_increases_after_write_test() {
  use s <- test_helpers.with_store()
  let assert Ok(offset_before) = store.current_offset(store: s)
  let assert Ok(_) = store.put_node(store: s, data: <<"hello":utf8>>)
  let assert Ok(offset_after) = store.current_offset(store: s)
  assert offset_after > offset_before
}
