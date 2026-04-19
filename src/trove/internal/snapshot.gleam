//// Point-in-time snapshot for consistent reads.

import gleam/dict
import gleam/io
import gleam/option
import gleam/order
import gleam/yielder
import trove/codec
import trove/internal/btree
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

/// A named keyspace captured in a snapshot. Carries the keyspace's frozen
/// tree plus its byte-level compare adapter so the snapshot can serve reads
/// without re-deriving either from the caller's codecs.
pub type KeyspaceView {
  KeyspaceView(
    tree: btree.Btree(BitArray, BitArray),
    byte_compare: fn(BitArray, BitArray) -> order.Order,
  )
}

/// An opaque snapshot of the database at a point in time. Subsequent writes
/// to the database are invisible to a snapshot.
pub opaque type Snapshot(k, v) {
  Snapshot(
    tree: btree.Btree(k, v),
    store: store.Store,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
    keyspaces: dict.Dict(String, KeyspaceView),
  )
}

/// Create a new snapshot from the current tree, a read-only store handle,
/// and the current keyspace map.
pub fn new(
  tree tree: btree.Btree(k, v),
  store store: store.Store,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
  keyspaces keyspaces: dict.Dict(String, KeyspaceView),
) -> Snapshot(k, v) {
  Snapshot(tree:, store:, key_codec:, value_codec:, key_compare:, keyspaces:)
}

/// Close the snapshot's read-only store handle, releasing the file descriptor.
pub fn close(snapshot snapshot: Snapshot(k, v)) -> Nil {
  case store.close(store: snapshot.store) {
    Ok(Nil) -> Nil
    Error(reason) ->
      io.println_error(
        "[trove] failed to close snapshot store: "
        <> store.error_to_string(reason),
      )
  }
}

/// Look up a key in the snapshot.
pub fn get(snapshot snapshot: Snapshot(k, v), key key: k) -> option.Option(v) {
  let assert Ok(result) =
    btree.lookup(
      tree: snapshot.tree,
      store: snapshot.store,
      key: key,
      key_codec: snapshot.key_codec,
      value_codec: snapshot.value_codec,
      compare: snapshot.key_compare,
    )
  result
}

/// Iterate over entries in the snapshot within optional key bounds.
pub fn range(
  snapshot snapshot: Snapshot(k, v),
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  btree_range.query(
    tree: snapshot.tree,
    store: snapshot.store,
    min: min,
    max: max,
    direction: direction,
    key_codec: snapshot.key_codec,
    value_codec: snapshot.value_codec,
    compare: snapshot.key_compare,
  )
}

/// Look up a key in a named keyspace captured by this snapshot.
pub fn get_in(
  snapshot snapshot: Snapshot(k, v),
  name name: String,
  key_bytes key_bytes: BitArray,
) -> option.Option(BitArray) {
  let assert Ok(view) = dict.get(snapshot.keyspaces, name)
  let assert Ok(result) =
    btree.lookup(
      tree: view.tree,
      store: snapshot.store,
      key: key_bytes,
      key_codec: codec.bit_array(),
      value_codec: codec.bit_array(),
      compare: view.byte_compare,
    )
  result
}

/// Iterate over entries in a named keyspace within optional byte bounds.
pub fn range_in(
  snapshot snapshot: Snapshot(k, v),
  name name: String,
  min min: option.Option(range.Bound(BitArray)),
  max max: option.Option(range.Bound(BitArray)),
  direction direction: range.Direction,
) -> yielder.Yielder(#(BitArray, BitArray)) {
  let assert Ok(view) = dict.get(snapshot.keyspaces, name)
  btree_range.query(
    tree: view.tree,
    store: snapshot.store,
    min: min,
    max: max,
    direction: direction,
    key_codec: codec.bit_array(),
    value_codec: codec.bit_array(),
    compare: view.byte_compare,
  )
}
