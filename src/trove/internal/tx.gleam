//// Transaction handle for reading and writing within an atomic transaction.

import gleam/dict
import gleam/erlang/process
import gleam/erlang/reference.{type Reference}
import gleam/option
import gleam/order
import trove/codec
import trove/internal/btree
import trove/internal/store

/// A named keyspace tracked inside a transaction. Carries the current tree
/// and the byte-level compare adapter captured when the keyspace was
/// registered.
pub type KeyspaceEntry {
  KeyspaceEntry(
    tree: btree.Btree(BitArray, BitArray),
    byte_compare: fn(BitArray, BitArray) -> order.Order,
  )
}

/// An opaque transaction handle. Accumulates writes against a snapshot of
/// the tree. Writes become visible to later reads within the same transaction.
pub opaque type Tx(k, v) {
  Tx(
    tree: btree.Btree(k, v),
    store: store.Store,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
    token: Reference,
    nonce: Reference,
    nonce_tracker: option.Option(process.Subject(Reference)),
    other_trees: dict.Dict(String, KeyspaceEntry),
  )
}

/// Create a new transaction from the current tree state and keyspace map.
pub fn new(
  tree tree: btree.Btree(k, v),
  store store: store.Store,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
  other_trees other_trees: dict.Dict(String, KeyspaceEntry),
) -> Tx(k, v) {
  Tx(
    tree:,
    store:,
    key_codec:,
    value_codec:,
    key_compare:,
    token: reference.new(),
    nonce: reference.new(),
    nonce_tracker: option.None,
    other_trees:,
  )
}

pub fn set_token(tx tx: Tx(k, v), token token: Reference) -> Tx(k, v) {
  Tx(..tx, token: token)
}

pub fn token(tx tx: Tx(k, v)) -> Reference {
  tx.token
}

pub fn nonce(tx tx: Tx(k, v)) -> Reference {
  tx.nonce
}

pub fn set_nonce_tracker(
  tx tx: Tx(k, v),
  tracker tracker: option.Option(process.Subject(Reference)),
) -> Tx(k, v) {
  Tx(..tx, nonce_tracker: tracker)
}

/// Extract the default-keyspace tree from a committed transaction.
pub fn get_tree(tx tx: Tx(k, v)) -> btree.Btree(k, v) {
  tx.tree
}

/// Extract the full map of named-keyspace states from a committed transaction.
pub fn get_other_trees(tx tx: Tx(k, v)) -> dict.Dict(String, KeyspaceEntry) {
  tx.other_trees
}

/// Look up a key within the transaction, seeing any writes made so far.
pub fn get(tx tx: Tx(k, v), key key: k) -> option.Option(v) {
  let assert Ok(result) =
    btree.lookup(
      tree: tx.tree,
      store: tx.store,
      key: key,
      key_codec: tx.key_codec,
      value_codec: tx.value_codec,
      compare: tx.key_compare,
    )
  result
}

/// Write a key-value pair within the transaction. Returns the updated `Tx`.
pub fn put(tx tx: Tx(k, v), key key: k, value value: v) -> Tx(k, v) {
  let assert Ok(new_tree) =
    btree.insert(
      tree: tx.tree,
      store: tx.store,
      key: key,
      value: value,
      key_codec: tx.key_codec,
      value_codec: tx.value_codec,
      compare: tx.key_compare,
    )
  Tx(..tx, tree: new_tree, nonce: bump_nonce(tx))
}

/// Delete a key within the transaction. Returns the updated `Tx`.
pub fn delete(tx tx: Tx(k, v), key key: k) -> Tx(k, v) {
  let assert Ok(new_tree) =
    btree.delete(
      tree: tx.tree,
      store: tx.store,
      key: key,
      key_codec: tx.key_codec,
      compare: tx.key_compare,
    )
  Tx(..tx, tree: new_tree, nonce: bump_nonce(tx))
}

/// Look up a key in a named keyspace within the transaction.
pub fn get_in(
  tx tx: Tx(k, v),
  name name: String,
  key_bytes key_bytes: BitArray,
) -> option.Option(BitArray) {
  let assert Ok(entry) = dict.get(tx.other_trees, name)
  let assert Ok(result) =
    btree.lookup(
      tree: entry.tree,
      store: tx.store,
      key: key_bytes,
      key_codec: codec.bit_array(),
      value_codec: codec.bit_array(),
      compare: entry.byte_compare,
    )
  result
}

/// Insert or update a key-value pair in a named keyspace within the
/// transaction.
pub fn put_in(
  tx tx: Tx(k, v),
  name name: String,
  key_bytes key_bytes: BitArray,
  value_bytes value_bytes: BitArray,
) -> Tx(k, v) {
  let assert Ok(entry) = dict.get(tx.other_trees, name)
  let assert Ok(new_tree) =
    btree.insert(
      tree: entry.tree,
      store: tx.store,
      key: key_bytes,
      value: value_bytes,
      key_codec: codec.bit_array(),
      value_codec: codec.bit_array(),
      compare: entry.byte_compare,
    )
  let new_entry = KeyspaceEntry(..entry, tree: new_tree)
  let new_other = dict.insert(tx.other_trees, name, new_entry)
  Tx(..tx, other_trees: new_other, nonce: bump_nonce(tx))
}

/// Delete a key from a named keyspace within the transaction.
pub fn delete_in(
  tx tx: Tx(k, v),
  name name: String,
  key_bytes key_bytes: BitArray,
) -> Tx(k, v) {
  let assert Ok(entry) = dict.get(tx.other_trees, name)
  let assert Ok(new_tree) =
    btree.delete(
      tree: entry.tree,
      store: tx.store,
      key: key_bytes,
      key_codec: codec.bit_array(),
      compare: entry.byte_compare,
    )
  let new_entry = KeyspaceEntry(..entry, tree: new_tree)
  let new_other = dict.insert(tx.other_trees, name, new_entry)
  Tx(..tx, other_trees: new_other, nonce: bump_nonce(tx))
}

fn bump_nonce(tx: Tx(k, v)) -> Reference {
  let new_nonce = reference.new()
  case tx.nonce_tracker {
    option.Some(subject) -> process.send(subject, new_nonce)
    option.None -> Nil
  }
  new_nonce
}
