//// Point-in-time snapshot for consistent reads.

import gleam/io
import gleam/option
import gleam/order
import gleam/yielder
import trove/codec
import trove/internal/btree
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

/// An opaque snapshot of the database at a point in time. Subsequent writes
/// to the database are invisible to a snapshot.
pub opaque type Snapshot(k, v) {
  Snapshot(
    tree: btree.Btree(k, v),
    store: store.Store,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
  )
}

/// Create a new snapshot from the current tree and a read-only store handle.
pub fn new(
  tree tree: btree.Btree(k, v),
  store store: store.Store,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
) -> Snapshot(k, v) {
  Snapshot(tree:, store:, key_codec:, value_codec:, key_compare:)
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
