import gleam/bit_array
import gleam/int
import gleam/list
import non_empty_list
import qcheck
import trove/codec
import trove/internal/btree/node
import trove/test_helpers

fn tree_node_generator() -> qcheck.Generator(node.TreeNode(String)) {
  let entry = qcheck.tuple2(qcheck.string(), qcheck.bounded_int(0, 999_999_999))
  let entry_gen =
    qcheck.tuple2(entry, qcheck.list_from(entry))
    |> qcheck.map(fn(pair) { non_empty_list.new(pair.0, pair.1) })

  qcheck.from_generators(qcheck.map(entry_gen, node.Leaf), [
    qcheck.map(entry_gen, node.Branch),
  ])
}

fn data_node_generator() -> qcheck.Generator(node.DataNode(String)) {
  qcheck.from_generators(qcheck.map(qcheck.string(), node.Value), [
    qcheck.return(node.Deleted),
  ])
}

pub fn tree_node_roundtrip_test() {
  let key_codec = codec.string()
  use n <- qcheck.run(test_helpers.property_config(), tree_node_generator())
  let encoded = node.encode_tree_node(node: n, key_codec: key_codec)
  let assert Ok(decoded) =
    node.decode_tree_node(data: encoded, key_codec: key_codec)
  assert decoded == n
}

pub fn data_node_roundtrip_test() {
  let value_codec = codec.string()
  use n <- qcheck.run(test_helpers.property_config(), data_node_generator())
  let encoded = node.encode_data_node(node: n, value_codec: value_codec)
  let assert Ok(decoded) =
    node.decode_data_node(data: encoded, value_codec: value_codec)
  assert decoded == n
}

pub fn any_node_roundtrip_test() {
  let key_codec = codec.string()
  let value_codec = codec.string()
  let gen =
    qcheck.from_generators(
      qcheck.map(tree_node_generator(), fn(n) {
        let encoded = node.encode_tree_node(node: n, key_codec: key_codec)
        #(encoded, node.Tree(n))
      }),
      [
        qcheck.map(data_node_generator(), fn(n) {
          let encoded = node.encode_data_node(node: n, value_codec: value_codec)
          #(encoded, node.Data(n))
        }),
      ],
    )
  use #(encoded, expected) <- qcheck.run(test_helpers.property_config(), gen)
  let assert Ok(decoded) =
    node.decode_any_node(
      data: encoded,
      key_codec: key_codec,
      value_codec: value_codec,
    )
  assert decoded == expected
}

pub fn decode_tree_node_invalid_tag_returns_error_test() {
  let key_codec = codec.string()
  let assert Error(Nil) =
    node.decode_tree_node(data: <<0xFF>>, key_codec: key_codec)
  Nil
}

pub fn decode_data_node_invalid_tag_returns_error_test() {
  let value_codec = codec.string()
  let assert Error(Nil) =
    node.decode_data_node(data: <<0xFF>>, value_codec: value_codec)
  Nil
}

pub fn decode_tree_node_truncated_leaf_returns_error_test() {
  let key_codec = codec.string()
  let assert Error(Nil) =
    node.decode_tree_node(data: <<0x01, 0, 0, 0, 1>>, key_codec: key_codec)
  Nil
}

pub fn decode_data_node_on_tree_data_returns_error_test() {
  let value_codec = codec.string()
  let key_codec = codec.string()
  let tree_data =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#("a", 1), [])),
      key_codec: key_codec,
    )
  let assert Error(Nil) =
    node.decode_data_node(data: tree_data, value_codec: value_codec)
  Nil
}

pub fn decode_tree_node_on_data_data_returns_error_test() {
  let value_codec = codec.string()
  let key_codec = codec.string()
  let data_data =
    node.encode_data_node(node: node.Deleted, value_codec: value_codec)
  let assert Error(Nil) =
    node.decode_tree_node(data: data_data, key_codec: key_codec)
  Nil
}

pub fn random_bit_flip_in_node_property_test() {
  let key_codec = codec.int()
  let value_codec = codec.string()

  use #(value, flip_pos) <- qcheck.run(
    qcheck.default_config() |> qcheck.with_test_count(25),
    qcheck.tuple2(qcheck.non_empty_string(), qcheck.bounded_int(0, 100)),
  )

  let encoded =
    node.encode_data_node(node: node.Value(value), value_codec: value_codec)
  let size = bit_array.byte_size(encoded)

  let byte_pos = flip_pos % size
  let assert Ok(before) = bit_array.slice(encoded, 0, byte_pos)
  let assert Ok(<<target_byte>>) = bit_array.slice(encoded, byte_pos, 1)
  let assert Ok(after) =
    bit_array.slice(encoded, byte_pos + 1, size - byte_pos - 1)

  let bit_index = flip_pos % 8
  let flipped =
    int.bitwise_exclusive_or(target_byte, int.bitwise_shift_left(1, bit_index))
  let corrupted = bit_array.concat([before, <<flipped>>, after])

  case
    node.decode_any_node(
      data: corrupted,
      key_codec: key_codec,
      value_codec: value_codec,
    )
  {
    Ok(node.Data(node.Value(decoded))) -> {
      let _ = decoded
      Nil
    }
    Ok(node.Data(node.Deleted)) -> Nil
    Ok(node.Tree(_)) -> Nil
    Error(Nil) -> Nil
  }
}

pub fn decode_tree_node_trailing_bytes_returns_error_test() {
  let key_codec = codec.int()
  let valid =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(1, 100), [])),
      key_codec: key_codec,
    )
  let corrupted = bit_array.concat([valid, <<0xFF>>])
  let assert Error(Nil) =
    node.decode_tree_node(data: corrupted, key_codec: key_codec)
  Nil
}

pub fn validate_structure_valid_leaf_test() {
  let key_codec = codec.int()
  let data =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(1, 100), [#(2, 200)])),
      key_codec: key_codec,
    )
  let assert True = node.validate_structure(data:)
}

pub fn validate_structure_invalid_tag_test() {
  let assert False = node.validate_structure(data: <<0xFF, 0, 0, 0, 0>>)
}

pub fn validate_structure_truncated_test() {
  let assert False = node.validate_structure(data: <<0x01, 0, 0, 0, 1>>)
}

pub fn validate_structure_trailing_bytes_test() {
  let key_codec = codec.int()
  let valid =
    node.encode_tree_node(
      node: node.Leaf(non_empty_list.new(#(1, 100), [])),
      key_codec: key_codec,
    )
  let corrupted = bit_array.concat([valid, <<0xFF>>])
  let assert False = node.validate_structure(data: corrupted)
}

pub fn validate_structure_valid_branch_test() {
  let key_codec = codec.int()
  let data =
    node.encode_tree_node(
      node: node.Branch(non_empty_list.new(#(1, 100), [#(2, 200)])),
      key_codec: key_codec,
    )
  let assert True = node.validate_structure(data:)
}

pub fn extract_locations_roundtrip_property_test() {
  let key_codec = codec.string()
  use n <- qcheck.run(test_helpers.property_config(), tree_node_generator())
  let expected_locations = case n {
    node.Leaf(children) ->
      non_empty_list.to_list(children) |> list.map(fn(e) { e.1 })
    node.Branch(children) ->
      non_empty_list.to_list(children) |> list.map(fn(e) { e.1 })
  }
  let encoded = node.encode_tree_node(node: n, key_codec: key_codec)
  let assert Ok(locations) = node.extract_locations(data: encoded)
  assert locations == expected_locations
}

pub fn extract_locations_data_node_returns_error_test() {
  let value_codec = codec.string()
  let encoded =
    node.encode_data_node(node: node.Value("hello"), value_codec: value_codec)
  let assert Error(Nil) = node.extract_locations(data: encoded)

  let assert Error(Nil) = node.extract_locations(data: <<0x04>>)
  Nil
}

pub fn validate_structure_rejects_zero_children_test() {
  let assert False = node.validate_structure(data: <<0x01, 0:32>>)
  let assert False = node.validate_structure(data: <<0x02, 0:32>>)
}

pub fn extract_locations_rejects_zero_children_test() {
  let assert Error(Nil) = node.extract_locations(data: <<0x01, 0:32>>)
  let assert Error(Nil) = node.extract_locations(data: <<0x02, 0:32>>)
}

pub fn node_kind_classifies_each_tag_test() {
  assert node.node_kind(data: <<0x01>>) == Ok(node.TreeKind)
  assert node.node_kind(data: <<0x02>>) == Ok(node.TreeKind)
  assert node.node_kind(data: <<0x03>>) == Ok(node.DataKind)
  assert node.node_kind(data: <<0x04>>) == Ok(node.DataKind)
  assert node.node_kind(data: <<0xFF>>) == Error(Nil)
  assert node.node_kind(data: <<>>) == Error(Nil)
}

pub fn is_leaf_only_true_for_leaf_tag_test() {
  assert node.is_leaf(data: <<0x01, 0:32>>) == True
  assert node.is_leaf(data: <<0x02, 0:32>>) == False
  assert node.is_leaf(data: <<0x03>>) == False
  assert node.is_leaf(data: <<0x04>>) == False
  assert node.is_leaf(data: <<>>) == False
}

pub fn is_tombstone_only_true_for_encoded_tombstone_test() {
  assert node.is_tombstone(data: node.encode_tombstone()) == True
  assert node.is_tombstone(data: <<0x04>>) == True
  assert node.is_tombstone(data: <<0x03, "x":utf8>>) == False
  assert node.is_tombstone(data: <<0x01>>) == False
}
