//// Append-only B+ tree implementation backed by a file store.

import gleam/bool
import gleam/int
import gleam/list
import gleam/option
import gleam/order
import gleam/result
import gleam/yielder
import non_empty_list.{type NonEmptyList}
import trove/codec
import trove/internal/btree/node
import trove/internal/store

pub type Error {
  StoreError(store.Error)
  DecodeError(detail: String)
  ValidationError(detail: String)
}

pub fn error_to_string(error: Error) -> String {
  case error {
    StoreError(e) -> store.error_to_string(e)
    DecodeError(detail) -> "decode error: " <> detail
    ValidationError(detail) -> "validation error: " <> detail
  }
}

fn read_node(store: store.Store, location: Int) -> Result(BitArray, Error) {
  store.get_node(store: store, location: location)
  |> result.map_error(StoreError)
}

fn write_node(store: store.Store, data: BitArray) -> Result(Int, Error) {
  store.put_node(store: store, data: data)
  |> result.map_error(StoreError)
}

type InsertResult(k) {
  InsertResult(outcome: InsertOutcome(k), is_new: Bool)
}

type InsertOutcome(k) {
  Single(location: Int, min_key: k)
  Split(left: Int, left_min: k, right_min: k, right: Int)
}

type DeleteResult(k) {
  DeletedNode(location: Int, min_key: k)
  DeleteNotFound
  DeleteEmpty
}

type MarkDeletedResult(k) {
  Marked(location: Int, min_key: k)
  MarkNotFound
}

/// The in-memory representation of a B+ tree. Tracks the root offset, entry
/// count, dead-entry count (dirt), and the maximum children per node.
///
/// `Empty` means no entries have been written (or all have been deleted).
/// `NonEmpty` guarantees a valid root offset and positive size.
pub opaque type Btree(k, v) {
  Empty(dirt: Int, capacity: Int)
  NonEmpty(root: Int, size: Int, dirt: Int, capacity: Int)
}

/// Create an empty tree with the default capacity of 32.
pub fn new() -> Btree(k, v) {
  Empty(dirt: 0, capacity: 32)
}

/// Create an empty tree with the given node capacity.
pub fn new_with_capacity(capacity capacity: Int) -> Btree(k, v) {
  let assert True = capacity >= 2
  Empty(dirt: 0, capacity: capacity)
}

/// Construct a tree from persisted header data. Validates that root and size
/// are consistent: `None` root requires size 0, `Some` root requires size > 0.
pub fn from_header(
  root root: option.Option(Int),
  size size: Int,
  dirt dirt: Int,
  capacity capacity: Int,
) -> Result(Btree(k, v), Error) {
  use <- bool.guard(
    when: capacity < 2,
    return: Error(ValidationError("capacity must be at least 2")),
  )
  use <- bool.guard(
    when: dirt < 0,
    return: Error(ValidationError("inconsistent header: negative dirt")),
  )
  case root, size {
    option.None, 0 -> Ok(Empty(dirt: dirt, capacity: capacity))
    option.Some(r), s if s > 0 ->
      Ok(NonEmpty(root: r, size: s, dirt: dirt, capacity: capacity))
    option.None, _ ->
      Error(ValidationError("inconsistent header: None root with non-zero size"))
    option.Some(_), _ ->
      Error(ValidationError(
        "inconsistent header: Some root with zero or negative size",
      ))
  }
}

/// Get the root offset, or `None` if the tree is empty.
pub fn root(tree tree: Btree(k, v)) -> option.Option(Int) {
  case tree {
    Empty(..) -> option.None
    NonEmpty(root:, ..) -> option.Some(root)
  }
}

/// Get the number of live entries.
pub fn size(tree tree: Btree(k, v)) -> Int {
  case tree {
    Empty(..) -> 0
    NonEmpty(size:, ..) -> size
  }
}

/// Get the number of dead entries.
pub fn dirt(tree tree: Btree(k, v)) -> Int {
  case tree {
    Empty(dirt:, ..) -> dirt
    NonEmpty(dirt:, ..) -> dirt
  }
}

/// Get the maximum children per node.
pub fn capacity(tree tree: Btree(k, v)) -> Int {
  case tree {
    Empty(capacity:, ..) -> capacity
    NonEmpty(capacity:, ..) -> capacity
  }
}

/// Look up a key in the tree. Returns `Ok(option.Some(value))` if found,
/// `Ok(None)` if the key does not exist or was deleted.
pub fn lookup(
  tree tree: Btree(k, v),
  store store: store.Store,
  key key: k,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> Result(option.Option(v), Error) {
  case tree {
    Empty(..) -> Ok(option.None)
    NonEmpty(root: location, ..) ->
      lookup_tree_at(store, location, key, key_codec, value_codec, compare)
  }
}

/// Check whether a key exists in the tree without decoding the value.
/// More efficient than `lookup` when the value is not needed.
pub fn contains(
  tree tree: Btree(k, v),
  store store: store.Store,
  key key: k,
  key_codec key_codec: codec.Codec(k),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Bool, Error) {
  case tree {
    Empty(..) -> Ok(False)
    NonEmpty(root: location, ..) ->
      contains_at(store, location, key, key_codec, compare)
  }
}

fn contains_at(
  store: store.Store,
  location: Int,
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(Bool, Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Ok(node.Leaf(children)) -> {
      case
        non_empty_list.find(in: children, one_that: fn(entry) {
          compare(entry.0, key) == order.Eq
        })
      {
        Ok(#(_, data_loc)) -> {
          use data <- result.try(read_node(store, data_loc))
          Ok(!node.is_tombstone(data: data))
        }
        Error(Nil) -> Ok(False)
      }
    }
    Ok(node.Branch(children)) -> {
      let #(_, child_loc) = find_branch_child_index(children, key, compare)
      contains_at(store, child_loc, key, key_codec, compare)
    }
    Error(Nil) ->
      Error(DecodeError("tree node at offset " <> int.to_string(location)))
  }
}

fn lookup_tree_at(
  store: store.Store,
  location: Int,
  key: k,
  key_codec: codec.Codec(k),
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
) -> Result(option.Option(v), Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Ok(node.Leaf(children)) ->
      lookup_in_leaf(store, children, key, value_codec, compare)
    Ok(node.Branch(children)) ->
      lookup_in_branch(store, children, key, key_codec, value_codec, compare)
    Error(Nil) ->
      Error(DecodeError("tree node at offset " <> int.to_string(location)))
  }
}

fn resolve_data(
  store: store.Store,
  location: Int,
  value_codec: codec.Codec(v),
) -> Result(option.Option(v), Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_data_node(data: data, value_codec: value_codec) {
    Ok(node.Value(value)) -> Ok(option.Some(value))
    Ok(node.Deleted) -> Ok(option.None)
    Error(Nil) ->
      Error(DecodeError("data node at offset " <> int.to_string(location)))
  }
}

fn lookup_in_leaf(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
) -> Result(option.Option(v), Error) {
  case
    non_empty_list.find(in: children, one_that: fn(entry) {
      compare(entry.0, key) == order.Eq
    })
  {
    Ok(#(_, value_loc)) -> resolve_data(store, value_loc, value_codec)
    Error(Nil) -> Ok(option.None)
  }
}

fn lookup_in_branch(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  key_codec: codec.Codec(k),
  value_codec: codec.Codec(v),
  compare: fn(k, k) -> order.Order,
) -> Result(option.Option(v), Error) {
  let #(_, child_loc) = find_branch_child_index(children, key, compare)
  lookup_tree_at(store, child_loc, key, key_codec, value_codec, compare)
}

/// Insert a key-value pair, returning the updated tree. Overwrites the
/// value if the key already exists.
pub fn insert(
  tree tree: Btree(k, v),
  store store: store.Store,
  key key: k,
  value value: v,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Btree(k, v), Error) {
  let tree_dirt = dirt(tree)
  let tree_capacity = capacity(tree)
  use value_loc <- result.try(write_data_node(
    store,
    node.Value(value),
    value_codec,
  ))
  case tree {
    Empty(..) -> {
      use leaf_loc <- result.try(write_tree_node(
        store,
        node.Leaf(non_empty_list.single(#(key, value_loc))),
        key_codec,
      ))
      Ok(NonEmpty(
        root: leaf_loc,
        size: 1,
        dirt: tree_dirt,
        capacity: tree_capacity,
      ))
    }
    NonEmpty(root: root_loc, size: tree_size, ..) -> {
      use insert_result <- result.try(do_insert(
        store,
        root_loc,
        key,
        value_loc,
        tree_capacity,
        key_codec,
        compare,
      ))
      let InsertResult(outcome:, is_new:) = insert_result
      let new_root_loc = case outcome {
        Single(loc, _) -> Ok(loc)
        Split(left_loc, left_min, right_min, right_loc) ->
          write_tree_node(
            store,
            node.Branch(
              non_empty_list.new(#(left_min, left_loc), [
                #(right_min, right_loc),
              ]),
            ),
            key_codec,
          )
      }
      let new_size = case is_new {
        True -> tree_size + 1
        False -> tree_size
      }
      use root <- result.try(new_root_loc)
      Ok(NonEmpty(
        root: root,
        size: new_size,
        dirt: case is_new {
          True -> tree_dirt
          False -> tree_dirt + 1
        },
        capacity: tree_capacity,
      ))
    }
  }
}

fn do_insert(
  store: store.Store,
  location: Int,
  key: k,
  value_loc: Int,
  capacity: Int,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(InsertResult(k), Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Error(Nil) ->
      Error(DecodeError("tree node at offset " <> int.to_string(location)))
    Ok(node.Leaf(children)) ->
      insert_into_leaf(
        store,
        children,
        key,
        value_loc,
        capacity,
        key_codec,
        compare,
      )
    Ok(node.Branch(children)) ->
      insert_into_branch(
        store,
        children,
        key,
        value_loc,
        capacity,
        key_codec,
        compare,
      )
  }
}

fn insert_into_leaf(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  value_loc: Int,
  capacity: Int,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(InsertResult(k), Error) {
  let #(new_children, is_new) =
    insert_into_sorted(children, key, value_loc, compare)
  let #(min_key, _) = non_empty_list.first(new_children)
  let count = non_empty_list.length(of: new_children)
  use <- bool.lazy_guard(when: count <= capacity, return: fn() {
    use loc <- result.try(write_tree_node(
      store,
      node.Leaf(new_children),
      key_codec,
    ))
    Ok(InsertResult(Single(loc, min_key), is_new))
  })
  let #(left_children, left_min, right_min, right_children) =
    split_children(new_children)
  use left_loc <- result.try(write_tree_node(
    store,
    node.Leaf(left_children),
    key_codec,
  ))
  use right_loc <- result.try(write_tree_node(
    store,
    node.Leaf(right_children),
    key_codec,
  ))
  Ok(InsertResult(Split(left_loc, left_min, right_min, right_loc), is_new))
}

fn insert_into_branch(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  value_loc: Int,
  capacity: Int,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(InsertResult(k), Error) {
  let #(child_index, child_loc) =
    find_branch_child_index(children, key, compare)
  use child_result <- result.try(do_insert(
    store,
    child_loc,
    key,
    value_loc,
    capacity,
    key_codec,
    compare,
  ))
  let InsertResult(outcome: child_outcome, is_new:) = child_result
  case child_outcome {
    Single(new_child_loc, new_min) -> {
      let new_children =
        replace_child(children, child_index, new_min, new_child_loc)
      let #(branch_min, _) = non_empty_list.first(new_children)
      use loc <- result.try(write_tree_node(
        store,
        node.Branch(new_children),
        key_codec,
      ))
      Ok(InsertResult(Single(loc, branch_min), is_new))
    }
    Split(left_loc, left_min, right_min, right_loc) -> {
      let new_children =
        splice_split(
          children,
          child_index,
          left_min,
          left_loc,
          right_min,
          right_loc,
        )
      let #(branch_min, _) = non_empty_list.first(new_children)
      let count = non_empty_list.length(of: new_children)
      use <- bool.lazy_guard(when: count <= capacity, return: fn() {
        use loc <- result.try(write_tree_node(
          store,
          node.Branch(new_children),
          key_codec,
        ))
        Ok(InsertResult(Single(loc, branch_min), is_new))
      })
      let #(left_children, branch_left_min, branch_right_min, right_children) =
        split_children(new_children)
      use new_left <- result.try(write_tree_node(
        store,
        node.Branch(left_children),
        key_codec,
      ))
      use new_right <- result.try(write_tree_node(
        store,
        node.Branch(right_children),
        key_codec,
      ))
      Ok(InsertResult(
        Split(new_left, branch_left_min, branch_right_min, new_right),
        is_new,
      ))
    }
  }
}

fn insert_into_sorted(
  children: NonEmptyList(#(k, Int)),
  key: k,
  location: Int,
  compare: fn(k, k) -> order.Order,
) -> #(NonEmptyList(#(k, Int)), Bool) {
  let #(child_key, child_loc) = non_empty_list.first(children)
  let rest = non_empty_list.rest(children)
  case compare(key, child_key) {
    order.Lt -> #(
      non_empty_list.new(#(key, location), [#(child_key, child_loc), ..rest]),
      True,
    )
    order.Eq -> #(non_empty_list.new(#(key, location), rest), False)
    order.Gt -> {
      let #(new_rest, is_new) =
        do_insert_into_sorted(rest, key, location, compare)
      #(non_empty_list.new(#(child_key, child_loc), new_rest), is_new)
    }
  }
}

fn do_insert_into_sorted(
  children: List(#(k, Int)),
  key: k,
  location: Int,
  compare: fn(k, k) -> order.Order,
) -> #(List(#(k, Int)), Bool) {
  case children {
    [] -> #([#(key, location)], True)
    [#(child_key, child_loc), ..rest] ->
      case compare(key, child_key) {
        order.Lt -> #([#(key, location), #(child_key, child_loc), ..rest], True)
        order.Eq -> #([#(key, location), ..rest], False)
        order.Gt -> {
          let #(new_rest, is_new) =
            do_insert_into_sorted(rest, key, location, compare)
          #([#(child_key, child_loc), ..new_rest], is_new)
        }
      }
  }
}

fn split_children(
  children: NonEmptyList(#(k, Int)),
) -> #(NonEmptyList(#(k, Int)), k, k, NonEmptyList(#(k, Int))) {
  // Caller invariant: length > capacity >= 2, so length >= 3 and both halves
  // end up with at least one element.
  let count = non_empty_list.length(of: children)
  let mid = count / 2
  let #(left, right) = list.split(non_empty_list.to_list(children), mid)
  case left, right {
    [lh, ..lt], [rh, ..rt] -> {
      let #(left_min, _) = lh
      let #(right_min, _) = rh
      #(
        non_empty_list.new(lh, lt),
        left_min,
        right_min,
        non_empty_list.new(rh, rt),
      )
    }
    _, _ ->
      panic as "split_children: caller invariant (length > capacity >= 2) violated"
  }
}

fn find_branch_child_index(
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  compare: fn(k, k) -> order.Order,
) -> #(Int, Int) {
  let #(_, first_loc) = non_empty_list.first(children)
  do_find_branch_child_index(
    non_empty_list.to_list(children),
    key,
    compare,
    0,
    #(0, first_loc),
  )
}

fn do_find_branch_child_index(
  children: List(#(k, Int)),
  key: k,
  compare: fn(k, k) -> order.Order,
  index: Int,
  best: #(Int, Int),
) -> #(Int, Int) {
  case children {
    [] -> best
    [#(child_key, child_loc), ..rest] ->
      case compare(child_key, key) {
        order.Lt | order.Eq ->
          do_find_branch_child_index(rest, key, compare, index + 1, #(
            index,
            child_loc,
          ))
        order.Gt -> best
      }
  }
}

fn replace_child(
  children: NonEmptyList(#(k, Int)),
  index: Int,
  new_key: k,
  new_loc: Int,
) -> NonEmptyList(#(k, Int)) {
  let children_list = non_empty_list.to_list(children)
  let before = list.take(children_list, index)
  let after = list.drop(children_list, index + 1)
  nel_splice_one(before, #(new_key, new_loc), after)
}

fn splice_split(
  children: NonEmptyList(#(k, Int)),
  index: Int,
  left_key: k,
  left_loc: Int,
  right_key: k,
  right_loc: Int,
) -> NonEmptyList(#(k, Int)) {
  let children_list = non_empty_list.to_list(children)
  let before = list.take(children_list, index)
  let after = list.drop(children_list, index + 1)
  nel_splice_two(before, #(left_key, left_loc), #(right_key, right_loc), after)
}

fn nel_splice_one(
  before: List(a),
  middle: a,
  after: List(a),
) -> NonEmptyList(a) {
  case before {
    [] -> non_empty_list.new(middle, after)
    [h, ..t] -> non_empty_list.new(h, list.append(t, [middle, ..after]))
  }
}

fn nel_splice_two(
  before: List(a),
  first: a,
  second: a,
  after: List(a),
) -> NonEmptyList(a) {
  case before {
    [] -> non_empty_list.new(first, [second, ..after])
    [h, ..t] -> non_empty_list.new(h, list.append(t, [first, second, ..after]))
  }
}

/// Bulk-load a sorted list of unique entries into a new tree.
pub fn load(
  entries entries: List(#(k, v)),
  store store: store.Store,
  capacity capacity: Int,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Btree(k, v), Error) {
  use <- bool.guard(
    when: capacity < 2,
    return: Error(ValidationError("capacity must be at least 2")),
  )
  case entries {
    [] -> Ok(Empty(dirt: 0, capacity: capacity))
    [first, ..rest] -> {
      use Nil <- result.try(validate_sorted_unique(entries, compare))
      let entry_count = list.length(entries)
      let entries_nel = non_empty_list.new(first, rest)
      use value_pairs <- result.try(write_values(
        entries_nel,
        store,
        value_codec,
      ))
      use leaf_pairs <- result.try(write_level(
        value_pairs,
        capacity,
        store,
        key_codec,
        node.Leaf,
      ))
      use root_loc <- result.try(build_branches(
        leaf_pairs,
        capacity,
        store,
        key_codec,
      ))
      Ok(NonEmpty(
        root: root_loc,
        size: entry_count,
        dirt: 0,
        capacity: capacity,
      ))
    }
  }
}

/// Bulk-load unique entries from a yielder into a new tree. Unlike `load`, this
/// streams entries lazily — only one leaf chunk worth of data is held in
/// memory at a time, plus the accumulated leaf references.
pub fn load_from_yielder(
  entries entries: yielder.Yielder(#(k, v)),
  store store: store.Store,
  capacity capacity: Int,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Btree(k, v), Error) {
  use <- bool.guard(
    when: capacity < 2,
    return: Error(ValidationError("capacity must be at least 2")),
  )
  // State: (current_chunk_reversed, leaf_pairs_reversed, chunk_size, total_count, prev_key)
  // current_chunk_reversed is Option(NonEmptyList) with entries newest-first
  let initial = #(option.None, [], 0, 0, option.None)

  use #(remaining, leaf_pairs_rev, _, total_count, _) <- result.try(
    yielder.try_fold(entries, initial, fn(state, entry) {
      let #(chunk_opt, leaves_rev, chunk_size, count, prev_key) = state
      let #(key, value) = entry
      use Nil <- result.try(case prev_key {
        option.None -> Ok(Nil)
        option.Some(prev) ->
          case compare(prev, key) {
            order.Lt -> Ok(Nil)
            order.Eq ->
              Error(ValidationError("duplicate key in bulk load input"))
            order.Gt ->
              Error(ValidationError("unsorted key in bulk load input"))
          }
      })
      use data_loc <- result.try(write_data_node(
        store,
        node.Value(value),
        value_codec,
      ))
      let new_entry = #(key, data_loc)
      let chunk_with_entry = case chunk_opt {
        option.None -> non_empty_list.single(new_entry)
        option.Some(nel) -> non_empty_list.prepend(to: nel, this: new_entry)
      }
      let chunk_size = chunk_size + 1
      let count = count + 1
      case chunk_size >= capacity {
        True -> {
          let leaf_children = non_empty_list.reverse(chunk_with_entry)
          use leaf_pair <- result.try(flush_chunk(
            leaf_children,
            store,
            key_codec,
            node.Leaf,
          ))
          Ok(#(
            option.None,
            [leaf_pair, ..leaves_rev],
            0,
            count,
            option.Some(key),
          ))
        }
        False ->
          Ok(#(
            option.Some(chunk_with_entry),
            leaves_rev,
            chunk_size,
            count,
            option.Some(key),
          ))
      }
    }),
  )

  // Write any remaining partial chunk as a final leaf
  use leaf_pairs <- result.try(case remaining {
    option.None -> Ok(list.reverse(leaf_pairs_rev))
    option.Some(chunk) -> {
      let leaf_children = non_empty_list.reverse(chunk)
      use leaf_pair <- result.try(flush_chunk(
        leaf_children,
        store,
        key_codec,
        node.Leaf,
      ))
      Ok(list.reverse([leaf_pair, ..leaf_pairs_rev]))
    }
  })

  case leaf_pairs {
    [] -> Ok(Empty(dirt: 0, capacity: capacity))
    [first_leaf, ..rest_leaves] -> {
      let leaf_nel = non_empty_list.new(first_leaf, rest_leaves)
      use root_loc <- result.try(build_branches(
        leaf_nel,
        capacity,
        store,
        key_codec,
      ))
      Ok(NonEmpty(
        root: root_loc,
        size: total_count,
        dirt: 0,
        capacity: capacity,
      ))
    }
  }
}

fn write_values(
  entries: NonEmptyList(#(k, v)),
  store: store.Store,
  value_codec: codec.Codec(v),
) -> Result(NonEmptyList(#(k, Int)), Error) {
  non_empty_list.map(entries, fn(entry) {
    let #(key, value) = entry
    write_data_node(store, node.Value(value), value_codec)
    |> result.map(fn(loc) { #(key, loc) })
  })
  |> non_empty_list.all
}

fn write_level(
  pairs: NonEmptyList(#(k, Int)),
  capacity: Int,
  store: store.Store,
  key_codec: codec.Codec(k),
  make_node: fn(NonEmptyList(#(k, Int))) -> node.TreeNode(k),
) -> Result(NonEmptyList(#(k, Int)), Error) {
  chunk_non_empty(pairs, capacity)
  |> non_empty_list.map(flush_chunk(_, store, key_codec, make_node))
  |> non_empty_list.all
}

fn chunk_non_empty(
  items: NonEmptyList(a),
  capacity: Int,
) -> NonEmptyList(NonEmptyList(a)) {
  let first = non_empty_list.first(items)
  let rest = non_empty_list.rest(items)
  let head_rest = list.take(rest, capacity - 1)
  let tail = list.drop(rest, capacity - 1)
  let head_chunk = non_empty_list.new(first, head_rest)
  case tail {
    [] -> non_empty_list.single(head_chunk)
    [h, ..t] ->
      non_empty_list.prepend(
        to: chunk_non_empty(non_empty_list.new(h, t), capacity),
        this: head_chunk,
      )
  }
}

fn flush_chunk(
  children: NonEmptyList(#(k, Int)),
  store: store.Store,
  key_codec: codec.Codec(k),
  make_node: fn(NonEmptyList(#(k, Int))) -> node.TreeNode(k),
) -> Result(#(k, Int), Error) {
  let #(first_key, _) = non_empty_list.first(children)
  use loc <- result.try(write_tree_node(store, make_node(children), key_codec))
  Ok(#(first_key, loc))
}

fn build_branches(
  pairs: NonEmptyList(#(k, Int)),
  capacity: Int,
  store: store.Store,
  key_codec: codec.Codec(k),
) -> Result(Int, Error) {
  case non_empty_list.rest(pairs) {
    [] -> {
      let #(_, loc) = non_empty_list.first(pairs)
      Ok(loc)
    }
    _ -> {
      use branch_pairs <- result.try(write_level(
        pairs,
        capacity,
        store,
        key_codec,
        node.Branch,
      ))
      build_branches(branch_pairs, capacity, store, key_codec)
    }
  }
}

fn write_tree_node(
  store: store.Store,
  tree_node: node.TreeNode(k),
  key_codec: codec.Codec(k),
) -> Result(Int, Error) {
  write_node(
    store,
    node.encode_tree_node(node: tree_node, key_codec: key_codec),
  )
}

fn write_data_node(
  store: store.Store,
  data_node: node.DataNode(v),
  value_codec: codec.Codec(v),
) -> Result(Int, Error) {
  write_node(
    store,
    node.encode_data_node(node: data_node, value_codec: value_codec),
  )
}

fn write_tombstone(store: store.Store) -> Result(Int, Error) {
  write_node(store, node.encode_tombstone())
}

/// Delete a key from the tree, removing the leaf entry entirely.
pub fn delete(
  tree tree: Btree(k, v),
  store store: store.Store,
  key key: k,
  key_codec key_codec: codec.Codec(k),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Btree(k, v), Error) {
  case tree {
    Empty(..) -> Ok(tree)
    NonEmpty(
      root: root_loc,
      size: tree_size,
      dirt: tree_dirt,
      capacity: tree_capacity,
    ) -> {
      use delete_result <- result.try(do_delete(
        store,
        root_loc,
        key,
        key_codec,
        compare,
      ))
      case delete_result {
        DeleteNotFound -> Ok(tree)
        DeleteEmpty -> Ok(Empty(dirt: tree_dirt + 1, capacity: tree_capacity))
        DeletedNode(new_loc, _) -> {
          use final_root <- result.try(collapse_root(store, new_loc, key_codec))
          Ok(NonEmpty(
            root: final_root,
            size: tree_size - 1,
            dirt: tree_dirt + 1,
            capacity: tree_capacity,
          ))
        }
      }
    }
  }
}

fn collapse_root(
  store: store.Store,
  location: Int,
  key_codec: codec.Codec(k),
) -> Result(Int, Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Ok(node.Branch(children)) ->
      case non_empty_list.rest(children) {
        [] -> {
          let #(_, child_loc) = non_empty_list.first(children)
          collapse_root(store, child_loc, key_codec)
        }
        _ -> Ok(location)
      }
    Ok(node.Leaf(_)) -> Ok(location)
    Error(Nil) ->
      Error(DecodeError(
        "node during root collapse at offset " <> int.to_string(location),
      ))
  }
}

fn do_delete(
  store: store.Store,
  location: Int,
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(DeleteResult(k), Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Error(Nil) ->
      Error(DecodeError("tree node at offset " <> int.to_string(location)))
    Ok(node.Leaf(children)) ->
      delete_from_leaf(store, children, key, key_codec, compare)
    Ok(node.Branch(children)) ->
      delete_from_branch(store, children, key, key_codec, compare)
  }
}

fn delete_from_leaf(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(DeleteResult(k), Error) {
  let children_list = non_empty_list.to_list(children)
  let #(rev_children, found) =
    list.fold(children_list, #([], False), fn(acc, entry) {
      case compare(entry.0, key) == order.Eq {
        True -> #(acc.0, True)
        False -> #([entry, ..acc.0], acc.1)
      }
    })
  case found {
    False -> Ok(DeleteNotFound)
    True ->
      case list.reverse(rev_children) {
        [] -> Ok(DeleteEmpty)
        [#(min_key, _) as head, ..tail] -> {
          let nel = non_empty_list.new(head, tail)
          use loc <- result.try(write_tree_node(
            store,
            node.Leaf(nel),
            key_codec,
          ))
          Ok(DeletedNode(loc, min_key))
        }
      }
  }
}

fn delete_from_branch(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(DeleteResult(k), Error) {
  let #(child_index, child_loc) =
    find_branch_child_index(children, key, compare)
  use child_result <- result.try(do_delete(
    store,
    child_loc,
    key,
    key_codec,
    compare,
  ))
  case child_result {
    DeleteNotFound -> Ok(DeleteNotFound)
    DeleteEmpty -> {
      let new_children = remove_child(children, child_index)
      case new_children {
        [] -> Ok(DeleteEmpty)
        [#(min_key, _) as head, ..tail] -> {
          let nel = non_empty_list.new(head, tail)
          use loc <- result.try(write_tree_node(
            store,
            node.Branch(nel),
            key_codec,
          ))
          Ok(DeletedNode(loc, min_key))
        }
      }
    }
    DeletedNode(new_child_loc, new_min) -> {
      let new_children =
        replace_child(children, child_index, new_min, new_child_loc)
      let #(branch_min, _) = non_empty_list.first(new_children)
      use loc <- result.try(write_tree_node(
        store,
        node.Branch(new_children),
        key_codec,
      ))
      Ok(DeletedNode(loc, branch_min))
    }
  }
}

fn remove_child(
  children: non_empty_list.NonEmptyList(#(k, Int)),
  index: Int,
) -> List(#(k, Int)) {
  let children_list = non_empty_list.to_list(children)
  list.append(
    list.take(children_list, index),
    list.drop(children_list, index + 1),
  )
}

/// Mark a key as deleted by replacing its value node with a tombstone.
/// Unlike `delete`, this preserves the leaf entry so the tree structure
/// is unchanged, which is useful for diff-based compaction.
///
/// **Experimental:** Not yet wired into any production code path.
/// Reserved for a planned diff-based compaction strategy.
pub fn mark_deleted(
  tree tree: Btree(k, v),
  store store: store.Store,
  key key: k,
  key_codec key_codec: codec.Codec(k),
  compare compare: fn(k, k) -> order.Order,
) -> Result(Btree(k, v), Error) {
  case tree {
    Empty(..) -> Ok(tree)
    NonEmpty(
      root: root_loc,
      size: tree_size,
      dirt: tree_dirt,
      capacity: tree_capacity,
    ) -> {
      use mark_result <- result.try(do_mark_deleted(
        store,
        root_loc,
        key,
        key_codec,
        compare,
      ))
      case mark_result {
        MarkNotFound -> Ok(tree)
        Marked(new_loc, _) ->
          Ok(NonEmpty(
            root: new_loc,
            size: tree_size - 1,
            dirt: tree_dirt + 1,
            capacity: tree_capacity,
          ))
      }
    }
  }
}

fn do_mark_deleted(
  store: store.Store,
  location: Int,
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(MarkDeletedResult(k), Error) {
  use data <- result.try(read_node(store, location))
  case node.decode_tree_node(data: data, key_codec: key_codec) {
    Error(Nil) ->
      Error(DecodeError("tree node at offset " <> int.to_string(location)))
    Ok(node.Leaf(children)) ->
      mark_deleted_in_leaf(store, children, key, key_codec, compare)
    Ok(node.Branch(children)) ->
      mark_deleted_in_branch(store, children, key, key_codec, compare)
  }
}

fn mark_deleted_in_leaf(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(MarkDeletedResult(k), Error) {
  case
    non_empty_list.any(in: children, satisfying: fn(entry) {
      compare(entry.0, key) == order.Eq
    })
  {
    False -> Ok(MarkNotFound)
    True -> {
      use tombstone_loc <- result.try(write_tombstone(store))
      let #(_, new_children) =
        replace_child_value(children, key, tombstone_loc, compare)
      let #(min_key, _) = non_empty_list.first(new_children)
      use loc <- result.try(write_tree_node(
        store,
        node.Leaf(new_children),
        key_codec,
      ))
      Ok(Marked(loc, min_key))
    }
  }
}

fn replace_child_value(
  children: NonEmptyList(#(k, Int)),
  key: k,
  new_loc: Int,
  compare: fn(k, k) -> order.Order,
) -> #(Bool, NonEmptyList(#(k, Int))) {
  non_empty_list.map_fold(over: children, from: False, with: fn(found, entry) {
    case compare(entry.0, key) == order.Eq {
      True -> #(True, #(entry.0, new_loc))
      False -> #(found, entry)
    }
  })
}

fn mark_deleted_in_branch(
  store: store.Store,
  children: non_empty_list.NonEmptyList(#(k, Int)),
  key: k,
  key_codec: codec.Codec(k),
  compare: fn(k, k) -> order.Order,
) -> Result(MarkDeletedResult(k), Error) {
  let #(child_index, child_loc) =
    find_branch_child_index(children, key, compare)
  use child_result <- result.try(do_mark_deleted(
    store,
    child_loc,
    key,
    key_codec,
    compare,
  ))
  case child_result {
    MarkNotFound -> Ok(MarkNotFound)
    Marked(new_child_loc, new_min) -> {
      let new_children =
        replace_child(children, child_index, new_min, new_child_loc)
      let #(branch_min, _) = non_empty_list.first(new_children)
      use loc <- result.try(write_tree_node(
        store,
        node.Branch(new_children),
        key_codec,
      ))
      Ok(Marked(loc, branch_min))
    }
  }
}

/// Return a new tree with the dirt count incremented by `amount`.
pub fn add_dirt(tree tree: Btree(k, v), amount amount: Int) -> Btree(k, v) {
  case tree {
    Empty(dirt:, capacity:) -> Empty(dirt: dirt + amount, capacity: capacity)
    NonEmpty(root:, size:, dirt:, capacity:) ->
      NonEmpty(root:, size:, dirt: dirt + amount, capacity:)
  }
}

fn validate_sorted_unique(
  entries: List(#(k, v)),
  compare: fn(k, k) -> order.Order,
) -> Result(Nil, Error) {
  case entries {
    [] | [_] -> Ok(Nil)
    [#(k1, _), #(k2, _) as next, ..rest] ->
      case compare(k1, k2) {
        order.Lt -> validate_sorted_unique([next, ..rest], compare)
        order.Eq -> Error(ValidationError("duplicate key in bulk load input"))
        order.Gt -> Error(ValidationError("unsorted key in bulk load input"))
      }
  }
}
