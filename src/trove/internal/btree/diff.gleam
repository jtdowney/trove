//// Computes the diff between two B+ tree snapshots.

import gleam/bool
import gleam/option
import gleam/order
import gleam/yielder
import trove/codec
import trove/internal/btree
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

/// A single entry in a tree diff.
pub type DiffEntry(v) {
  /// The key was inserted or its value changed.
  Upserted(v)
  /// The key was removed.
  Removed
}

/// Compute the diff between two trees sharing the same store, returning
/// a yielder of changed entries in key order.
pub fn diff(
  old old: btree.Btree(k, v),
  new new: btree.Btree(k, v),
  store store: store.Store,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> yielder.Yielder(#(k, DiffEntry(v))) {
  use <- bool.guard(
    when: btree.root(tree: old) == btree.root(tree: new),
    return: yielder.empty(),
  )
  let old_yielder =
    btree_range.query(
      tree: old,
      store: store,
      min: option.None,
      max: option.None,
      direction: range.Forward,
      key_codec: key_codec,
      value_codec: value_codec,
      compare: compare,
    )

  let new_yielder =
    btree_range.query(
      tree: new,
      store: store,
      min: option.None,
      max: option.None,
      direction: range.Forward,
      key_codec: key_codec,
      value_codec: value_codec,
      compare: compare,
    )

  lazy_merge_diff(old_yielder, new_yielder, compare)
}

fn lazy_merge_diff(
  old: yielder.Yielder(#(k, v)),
  new: yielder.Yielder(#(k, v)),
  compare: fn(k, k) -> order.Order,
) -> yielder.Yielder(#(k, DiffEntry(v))) {
  yielder.unfold(from: #(yielder.step(old), yielder.step(new)), with: fn(state) {
    let #(old_step, new_step) = state
    do_merge_step(old_step, new_step, compare)
  })
}

fn do_merge_step(
  old_step: yielder.Step(#(k, v), yielder.Yielder(#(k, v))),
  new_step: yielder.Step(#(k, v), yielder.Yielder(#(k, v))),
  compare: fn(k, k) -> order.Order,
) -> yielder.Step(
  #(k, DiffEntry(v)),
  #(
    yielder.Step(#(k, v), yielder.Yielder(#(k, v))),
    yielder.Step(#(k, v), yielder.Yielder(#(k, v))),
  ),
) {
  case old_step, new_step {
    yielder.Done, yielder.Done -> yielder.Done
    yielder.Done, yielder.Next(#(new_key, new_value), new_rest) ->
      yielder.Next(#(new_key, Upserted(new_value)), #(
        yielder.Done,
        yielder.step(new_rest),
      ))
    yielder.Next(#(old_key, _), old_rest), yielder.Done ->
      yielder.Next(#(old_key, Removed), #(yielder.step(old_rest), yielder.Done))
    yielder.Next(#(old_key, old_value), old_rest),
      yielder.Next(#(new_key, new_value), new_rest)
    ->
      case compare(old_key, new_key) {
        order.Lt ->
          yielder.Next(#(old_key, Removed), #(yielder.step(old_rest), new_step))
        order.Gt ->
          yielder.Next(#(new_key, Upserted(new_value)), #(
            old_step,
            yielder.step(new_rest),
          ))
        order.Eq ->
          case old_value == new_value {
            True ->
              do_merge_step(
                yielder.step(old_rest),
                yielder.step(new_rest),
                compare,
              )
            False ->
              yielder.Next(#(new_key, Upserted(new_value)), #(
                yielder.step(old_rest),
                yielder.step(new_rest),
              ))
          }
      }
  }
}
