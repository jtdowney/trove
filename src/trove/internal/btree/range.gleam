//// Range query execution over B+ trees.

import gleam/bool
import gleam/int
import gleam/list
import gleam/option
import gleam/order
import gleam/yielder
import non_empty_list
import trove/codec
import trove/internal/btree
import trove/internal/btree/node
import trove/internal/store
import trove/range

/// Run a range query over the tree, returning a yielder of key-value pairs
/// within the given bounds. Entries are streamed lazily from disk; only one
/// leaf node's worth of data is held in memory at a time.
///
/// **Panics** on store read or decode errors during traversal.
pub fn query(
  tree tree: btree.Btree(k, v),
  store store: store.Store,
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> yielder.Yielder(#(k, v)) {
  use <- bool.guard(
    when: bounds_are_empty(min, max, compare),
    return: yielder.empty(),
  )
  case btree.root(tree) {
    option.None -> yielder.empty()
    option.Some(location) ->
      traverse_node(
        store,
        location,
        key_codec,
        value_codec,
        compare,
        min,
        max,
        direction,
      )
  }
}

fn fetch_or_panic(store: store.Store, location: Int, label: String) -> BitArray {
  case store.get_node(store: store, location: location) {
    Ok(d) -> d
    Error(reason) ->
      panic as {
        "range query: "
        <> label
        <> " read failed at offset "
        <> int.to_string(location)
        <> ": "
        <> store.error_to_string(reason)
      }
  }
}

fn traverse_node(
  store: store.Store,
  location: Int,
  key_codec: codec.Codec(k),
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
  min: option.Option(range.Bound(k)),
  max: option.Option(range.Bound(k)),
  direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  let data = fetch_or_panic(store, location, "tree node")
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Ok(node.Leaf(children)) ->
      traverse_leaf(store, children, value_codec, compare, min, max, direction)
    Ok(node.Branch(children)) ->
      traverse_branch(
        store,
        children,
        key_codec,
        value_codec,
        compare,
        min,
        max,
        direction,
      )
    Error(Nil) ->
      panic as {
        "failed to decode tree node during range query at offset "
        <> int.to_string(location)
      }
  }
}

fn traverse_leaf(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
  min: option.Option(range.Bound(k)),
  max: option.Option(range.Bound(k)),
  direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  let ordered = order_children(non_empty_list.to_list(children), direction)
  yielder.from_list(ordered)
  |> yielder.filter(fn(entry) { in_range(entry.0, min, max, compare) })
  |> yielder.filter_map(fn(entry) {
    let #(key, data_loc) = entry
    let data = fetch_or_panic(store, data_loc, "data node")
    case node.decode_data_node(data: data, value_codec: value_codec) {
      Ok(node.Value(value)) -> Ok(#(key, value))
      Ok(node.Deleted) -> Error(Nil)
      Error(Nil) ->
        panic as {
          "failed to decode data node during range query at offset "
          <> int.to_string(data_loc)
        }
    }
  })
}

fn traverse_branch(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key_codec: codec.Codec(k),
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
  min: option.Option(range.Bound(k)),
  max: option.Option(range.Bound(k)),
  direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  let pruned =
    non_empty_list.to_list(children)
    |> prune_below_min(min, compare)
    |> list.take_while(fn(entry) { !beyond_max(entry.0, max, compare) })
  let ordered = order_children(pruned, direction)
  yielder.from_list(ordered)
  |> yielder.flat_map(fn(entry) {
    traverse_node(
      store,
      entry.1,
      key_codec,
      value_codec,
      compare,
      min,
      max,
      direction,
    )
  })
}

fn order_children(
  children: List(#(k, Int)),
  direction: range.Direction,
) -> List(#(k, Int)) {
  case direction {
    range.Forward -> children
    range.Reverse -> list.reverse(children)
  }
}

fn prune_below_min(
  children: List(#(k, Int)),
  min: option.Option(range.Bound(k)),
  compare: fn(k, k) -> order.Order,
) -> List(#(k, Int)) {
  case min {
    option.None -> children
    option.Some(bound) -> do_prune_below_min(children, bound, compare)
  }
}

fn do_prune_below_min(
  children: List(#(k, Int)),
  min: range.Bound(k),
  compare: fn(k, k) -> order.Order,
) -> List(#(k, Int)) {
  let bound_key = case min {
    range.Inclusive(k) -> k
    range.Exclusive(k) -> k
  }
  case children {
    [] -> []
    [_] -> children
    [_, next, ..rest] ->
      case compare(next.0, bound_key) {
        order.Gt -> children
        order.Lt | order.Eq -> do_prune_below_min([next, ..rest], min, compare)
      }
  }
}

fn beyond_max(
  key: k,
  max: option.Option(range.Bound(k)),
  compare: fn(k, k) -> order.Order,
) -> Bool {
  case max {
    option.None -> False
    option.Some(range.Inclusive(bound)) -> compare(key, bound) == order.Gt
    option.Some(range.Exclusive(bound)) -> compare(key, bound) != order.Lt
  }
}

fn in_range(
  key: k,
  min: option.Option(range.Bound(k)),
  max: option.Option(range.Bound(k)),
  compare: fn(k, k) -> order.Order,
) -> Bool {
  let above_min = case min {
    option.None -> True
    option.Some(range.Inclusive(bound_key)) ->
      compare(key, bound_key) != order.Lt
    option.Some(range.Exclusive(bound_key)) ->
      compare(key, bound_key) == order.Gt
  }

  let below_max = case max {
    option.None -> True
    option.Some(range.Inclusive(bound_key)) ->
      compare(key, bound_key) != order.Gt
    option.Some(range.Exclusive(bound_key)) ->
      compare(key, bound_key) == order.Lt
  }

  above_min && below_max
}

fn bounds_are_empty(
  min: option.Option(range.Bound(k)),
  max: option.Option(range.Bound(k)),
  compare: fn(k, k) -> order.Order,
) -> Bool {
  case min, max {
    option.Some(min_bound), option.Some(max_bound) -> {
      let lo = case min_bound {
        range.Inclusive(k) | range.Exclusive(k) -> k
      }
      let hi = case max_bound {
        range.Inclusive(k) | range.Exclusive(k) -> k
      }
      case min_bound, max_bound {
        range.Inclusive(_), range.Inclusive(_) -> compare(lo, hi) == order.Gt
        _, _ -> compare(lo, hi) != order.Lt
      }
    }
    option.None, _ -> False
    _, option.None -> False
  }
}
