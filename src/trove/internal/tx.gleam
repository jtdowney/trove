//// Transaction handle for reading and writing within an atomic transaction.

import gleam/erlang/process
import gleam/erlang/reference.{type Reference}
import gleam/option
import gleam/order
import trove/codec
import trove/internal/btree
import trove/internal/store

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
  )
}

/// Create a new transaction from the current tree state.
pub fn new(
  tree tree: btree.Btree(k, v),
  store store: store.Store,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
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

/// Extract the tree from a committed transaction.
pub fn get_tree(tx tx: Tx(k, v)) -> btree.Btree(k, v) {
  tx.tree
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
  let new_nonce = reference.new()
  case tx.nonce_tracker {
    option.Some(subject) -> process.send(subject, new_nonce)
    option.None -> Nil
  }
  Tx(..tx, tree: new_tree, nonce: new_nonce)
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
  let new_nonce = reference.new()
  case tx.nonce_tracker {
    option.Some(subject) -> process.send(subject, new_nonce)
    option.None -> Nil
  }
  Tx(..tx, tree: new_tree, nonce: new_nonce)
}
