//// Binary encoding and decoding of B+ tree nodes.
////
//// Nodes are split into two types: `TreeNode` for the tree structure
//// (leaves and branches) and `DataNode` for stored values (live or deleted).

import gleam/bit_array
import gleam/list
import gleam/result
import non_empty_list.{type NonEmptyList}
import trove/codec

/// A structural node in the B+ tree: either a leaf holding key-location
/// pairs, or a branch holding child pointers.
pub type TreeNode(k) {
  /// A leaf node containing sorted key-location pairs.
  Leaf(children: NonEmptyList(#(k, Int)))
  /// A branch node containing sorted key-child pairs.
  Branch(children: NonEmptyList(#(k, Int)))
}

/// A data node referenced by a leaf entry.
pub type DataNode(v) {
  /// A live value.
  Value(value: v)
  /// A tombstone marking a deleted entry.
  Deleted
}

/// A decoded node of any kind — used to dispatch on the discriminator byte
/// in a single pass rather than trying tree-node then data-node decode
/// separately.
pub type AnyNode(k, v) {
  Tree(TreeNode(k))
  Data(DataNode(v))
}

pub type Kind {
  TreeKind
  DataKind
}

pub fn node_kind(data data: BitArray) -> Result(Kind, Nil) {
  case data {
    <<0x01, _:bits>> | <<0x02, _:bits>> -> Ok(TreeKind)
    <<0x03, _:bits>> | <<0x04, _:bits>> -> Ok(DataKind)
    _ -> Error(Nil)
  }
}

pub fn is_leaf(data data: BitArray) -> Bool {
  case data {
    <<0x01, _:bits>> -> True
    _ -> False
  }
}

/// Encode a tree node to bytes.
pub fn encode_tree_node(
  node node: TreeNode(k),
  key_codec key_codec: codec.Codec(k),
) -> BitArray {
  let #(tag, children) = case node {
    Leaf(children) -> #(<<0x01>>, non_empty_list.to_list(children))
    Branch(children) -> #(<<0x02>>, non_empty_list.to_list(children))
  }
  let payload = encode_entries(children, key_codec)
  bit_array.concat([tag, payload])
}

/// Encode a data node to bytes.
pub fn encode_data_node(
  node node: DataNode(v),
  value_codec value_codec: codec.Codec(v),
) -> BitArray {
  case node {
    Value(value) -> {
      let value_bytes = value_codec.encode(value)
      bit_array.concat([<<0x03>>, value_bytes])
    }
    Deleted -> <<0x04>>
  }
}

/// Encode a tombstone marker without requiring a value codec.
pub fn encode_tombstone() -> BitArray {
  <<0x04>>
}

/// Decode bytes into a tree node. Returns `Error(Nil)` for data nodes
/// or corrupt data (including nodes with zero children).
pub fn decode_tree_node(
  data data: BitArray,
  key_codec key_codec: codec.Codec(k),
) -> Result(TreeNode(k), Nil) {
  case data {
    <<0x01, rest:bits>> -> {
      use entries <- result.try(decode_entries(rest, key_codec))
      use nel <- result.map(
        non_empty_list.from_list(entries) |> result.replace_error(Nil),
      )
      Leaf(nel)
    }
    <<0x02, rest:bits>> -> {
      use entries <- result.try(decode_entries(rest, key_codec))
      use nel <- result.map(
        non_empty_list.from_list(entries) |> result.replace_error(Nil),
      )
      Branch(nel)
    }
    _ -> Error(Nil)
  }
}

/// Decode bytes into a data node. Returns `Error(Nil)` for tree nodes
/// or corrupt data.
pub fn decode_data_node(
  data data: BitArray,
  value_codec value_codec: codec.Codec(v),
) -> Result(DataNode(v), Nil) {
  case data {
    <<0x03, rest:bits>> -> {
      use value <- result.map(value_codec.decode(rest))
      Value(value)
    }
    <<0x04>> -> Ok(Deleted)
    _ -> Error(Nil)
  }
}

/// Check if raw node data represents a tombstone (deleted entry).
pub fn is_tombstone(data data: BitArray) -> Bool {
  data == <<0x04>>
}

/// Validate that raw node data has valid tree node structure without decoding
/// keys. Checks the tag byte, count field, and that entries parse with exact
/// byte consumption (4-byte key_size + key_bytes + 8-byte location each).
pub fn validate_structure(data data: BitArray) -> Bool {
  case data {
    <<0x01, rest:bits>> | <<0x02, rest:bits>> ->
      validate_entries_structure(rest)
    _ -> False
  }
}

/// Extract the list of child/data locations from raw tree node bytes without
/// decoding keys. Returns `Error(Nil)` if the data is not a valid tree node.
pub fn extract_locations(data data: BitArray) -> Result(List(Int), Nil) {
  case data {
    <<0x01, rest:bits>> | <<0x02, rest:bits>> ->
      extract_locations_from_entries(rest)
    _ -> Error(Nil)
  }
}

fn extract_locations_from_entries(data: BitArray) -> Result(List(Int), Nil) {
  case data {
    <<count:32, rest:bits>> if count > 0 -> collect_n_locations(rest, count, [])
    _ -> Error(Nil)
  }
}

fn collect_n_locations(
  data: BitArray,
  remaining: Int,
  acc: List(Int),
) -> Result(List(Int), Nil) {
  case remaining, data {
    0, <<>> -> Ok(list.reverse(acc))
    _,
      <<
        key_size:32,
        _key_bytes:bytes-size(key_size),
        location:big-size(64),
        tail:bits,
      >>
    -> collect_n_locations(tail, remaining - 1, [location, ..acc])
    _, _ -> Error(Nil)
  }
}

fn validate_entries_structure(data: BitArray) -> Bool {
  case data {
    <<count:32, rest:bits>> if count > 0 ->
      validate_n_entries_structure(rest, count)
    _ -> False
  }
}

fn validate_n_entries_structure(data: BitArray, remaining: Int) -> Bool {
  case remaining, data {
    0, <<>> -> True
    _,
      <<
        key_size:32,
        _key_bytes:bytes-size(key_size),
        _location:big-size(64),
        tail:bits,
      >>
    -> validate_n_entries_structure(tail, remaining - 1)
    _, _ -> False
  }
}

/// Decode bytes into any node type in a single pass.
pub fn decode_any_node(
  data data: BitArray,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
) -> Result(AnyNode(k, v), Nil) {
  case decode_tree_node(data: data, key_codec: key_codec) {
    Ok(tree_node) -> Ok(Tree(tree_node))
    Error(Nil) ->
      case decode_data_node(data: data, value_codec: value_codec) {
        Ok(data_node) -> Ok(Data(data_node))
        Error(Nil) -> Error(Nil)
      }
  }
}

fn encode_entries(
  entries: List(#(k, Int)),
  key_codec: codec.Codec(k),
) -> BitArray {
  let count = list.length(entries)
  let entry_bits =
    list.map(entries, fn(entry) {
      let #(key, location) = entry
      let key_bytes = key_codec.encode(key)
      let key_size = bit_array.byte_size(key_bytes)
      bit_array.concat([
        <<key_size:32>>,
        key_bytes,
        <<location:int-big-size(64)>>,
      ])
    })
  bit_array.concat([<<count:32>>, ..entry_bits])
}

fn decode_entries(
  data: BitArray,
  key_codec: codec.Codec(k),
) -> Result(List(#(k, Int)), Nil) {
  case data {
    <<count:32, rest:bits>> -> decode_n_entries(rest, count, key_codec, [])
    _ -> Error(Nil)
  }
}

fn decode_n_entries(
  data: BitArray,
  remaining: Int,
  key_codec: codec.Codec(k),
  acc: List(#(k, Int)),
) -> Result(List(#(k, Int)), Nil) {
  case remaining, data {
    0, <<>> -> Ok(list.reverse(acc))
    _,
      <<
        key_size:32,
        key_bytes:bytes-size(key_size),
        location:big-size(64),
        tail:bits,
      >>
    -> {
      use key <- result.try(key_codec.decode(key_bytes))
      decode_n_entries(tail, remaining - 1, key_codec, [#(key, location), ..acc])
    }
    _, _ -> Error(Nil)
  }
}
