import exception
import gleam/int
import gleam/list
import gleam/option
import gleam/yielder
import qcheck
import trove/codec
import trove/internal/btree
import trove/test_helpers

pub fn new_creates_empty_tree_test() {
  let tree = btree.new()
  assert btree.root(tree) == option.None
  assert btree.size(tree) == 0
  assert btree.dirt(tree) == 0
  assert btree.capacity(tree) == 32
}

pub fn new_with_capacity_test() {
  let tree = btree.new_with_capacity(capacity: 64)
  assert btree.root(tree) == option.None
  assert btree.capacity(tree) == 64
}

pub fn from_header_valid_empty_test() {
  let assert Ok(tree) =
    btree.from_header(root: option.None, size: 0, dirt: 0, capacity: 32)
  assert btree.root(tree) == option.None
  assert btree.size(tree) == 0
  assert btree.dirt(tree) == 0
  assert btree.capacity(tree) == 32
}

pub fn from_header_valid_nonempty_test() {
  let assert Ok(tree) =
    btree.from_header(root: option.Some(100), size: 5, dirt: 2, capacity: 16)
  assert btree.root(tree) == option.Some(100)
  assert btree.size(tree) == 5
  assert btree.dirt(tree) == 2
  assert btree.capacity(tree) == 16
}

pub fn from_header_none_root_nonzero_size_returns_error_test() {
  let assert Error(btree.ValidationError(
    "inconsistent header: None root with non-zero size",
  )) = btree.from_header(root: option.None, size: 5, dirt: 0, capacity: 32)
  Nil
}

pub fn from_header_some_root_zero_size_returns_error_test() {
  let assert Error(btree.ValidationError(
    "inconsistent header: Some root with zero or negative size",
  )) = btree.from_header(root: option.Some(42), size: 0, dirt: 0, capacity: 32)
  Nil
}

pub fn from_header_some_root_negative_size_returns_error_test() {
  let assert Error(btree.ValidationError(
    "inconsistent header: Some root with zero or negative size",
  )) = btree.from_header(root: option.Some(42), size: -1, dirt: 0, capacity: 32)
  Nil
}

pub fn from_header_negative_dirt_returns_error_test() {
  let assert Error(btree.ValidationError("inconsistent header: negative dirt")) =
    btree.from_header(root: option.None, size: 0, dirt: -1, capacity: 32)
  Nil
}

pub fn from_header_negative_dirt_with_root_returns_error_test() {
  let assert Error(btree.ValidationError("inconsistent header: negative dirt")) =
    btree.from_header(root: option.Some(42), size: 5, dirt: -1, capacity: 32)
  Nil
}

pub fn insert_into_empty_tree_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  assert btree.size(tree) == 1
  assert btree.dirt(tree) == 0
  assert btree.root(tree) != option.None
  let assert Ok(option.Some("hello")) = test_helpers.lookup(tree, s, 1)
  Nil
}

pub fn insert_overwrites_existing_key_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "first")
  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "second")
  assert btree.size(tree) == 1
  let assert Ok(option.Some("second")) = test_helpers.lookup(tree, s, 1)
  Nil
}

pub fn insert_size_tracks_unique_keys_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "a")
  assert btree.size(tree) == 1
  let assert Ok(tree) = test_helpers.insert(tree, s, 2, "b")
  assert btree.size(tree) == 2
  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "c")
  assert btree.size(tree) == 2
}

pub fn insert_dirt_increments_on_overwrite_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "a")
  assert btree.dirt(tree) == 0
  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "b")
  assert btree.dirt(tree) == 1
}

pub fn insert_new_keys_do_not_increment_dirt_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "a")
  let assert Ok(tree) = test_helpers.insert(tree, s, 2, "b")
  let assert Ok(tree) = test_helpers.insert(tree, s, 3, "c")
  let assert Ok(tree) = test_helpers.insert(tree, s, 4, "d")
  let assert Ok(tree) = test_helpers.insert(tree, s, 5, "e")
  assert btree.dirt(tree) == 0

  let assert Ok(tree) = test_helpers.insert(tree, s, 3, "overwritten")
  assert btree.dirt(tree) == 1
}

pub fn insert_property_all_distinct_keys_findable_test() {
  use keys <- qcheck.run(
    test_helpers.property_config(),
    qcheck.list_from(qcheck.bounded_int(-1000, 1000)),
  )
  use s <- test_helpers.with_store()

  let unique_keys = list.unique(keys)
  let entries = list.map(unique_keys, fn(k) { #(k, "v" <> int.to_string(k)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  assert btree.size(tree) == list.length(unique_keys)
  list.each(entries, fn(entry) {
    let #(k, v) = entry
    let assert Ok(option.Some(found)) = test_helpers.lookup(tree, s, k)
    assert found == v
  })
}

pub fn delete_from_empty_tree_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree2) = test_helpers.delete(tree, s, 1)
  assert btree.size(tree2) == 0
  assert btree.root(tree2) == option.None
}

pub fn delete_nonexistent_key_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let tree = test_helpers.insert_all(tree, s, [#(1, "a"), #(2, "b"), #(3, "c")])
  let assert Ok(tree2) = test_helpers.delete(tree, s, 99)
  assert btree.size(tree2) == btree.size(tree)
  assert btree.dirt(tree2) == btree.dirt(tree)
  let assert Ok(option.Some("a")) = test_helpers.lookup(tree2, s, 1)
  let assert Ok(option.Some("b")) = test_helpers.lookup(tree2, s, 2)
  let assert Ok(option.Some("c")) = test_helpers.lookup(tree2, s, 3)
  Nil
}

pub fn delete_single_entry_makes_empty_tree_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  let assert Ok(tree) = test_helpers.delete(tree, s, 1)
  assert btree.size(tree) == 0
  assert btree.root(tree) == option.None
  let assert Ok(option.None) = test_helpers.lookup(tree, s, 1)
  Nil
}

pub fn delete_all_entries_makes_empty_tree_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)
  let keys = list.map(entries, fn(e) { e.0 })
  let tree = test_helpers.delete_all(tree, s, keys)

  assert btree.size(tree) == 0
  assert btree.root(tree) == option.None
  list.each(keys, fn(k) {
    let assert Ok(option.None) = test_helpers.lookup(tree, s, k)
    Nil
  })
}

pub fn delete_property_insert_then_delete_subset_test() {
  use keys <- qcheck.run(
    test_helpers.property_config(),
    qcheck.list_from(qcheck.bounded_int(-500, 500)),
  )
  use s <- test_helpers.with_store()

  let unique_keys = list.unique(keys)
  let entries = list.map(unique_keys, fn(k) { #(k, "v" <> int.to_string(k)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  let #(to_delete, to_keep) =
    list.split(unique_keys, list.length(unique_keys) / 2)
  let tree = test_helpers.delete_all(tree, s, to_delete)

  assert btree.size(tree) == list.length(to_keep)
  list.each(to_delete, fn(k) {
    let assert Ok(option.None) = test_helpers.lookup(tree, s, k)
    Nil
  })
  list.each(to_keep, fn(k) {
    let assert Ok(option.Some(_)) = test_helpers.lookup(tree, s, k)
    Nil
  })
}

pub fn mark_deleted_key_returns_none_on_lookup_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  let assert Ok(tree) = test_helpers.mark_deleted(tree, s, 1)
  let assert Ok(option.None) = test_helpers.lookup(tree, s, 1)
  Nil
}

pub fn mark_deleted_decrements_size_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  let assert Ok(tree) = test_helpers.insert(tree, s, 2, "world")
  let assert Ok(tree) = test_helpers.mark_deleted(tree, s, 1)
  assert btree.size(tree) == 1
}

pub fn mark_deleted_increments_dirt_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  let dirt_before = btree.dirt(tree)
  let assert Ok(tree) = test_helpers.mark_deleted(tree, s, 1)
  assert btree.dirt(tree) > dirt_before
}

pub fn mark_deleted_nonexistent_key_unchanged_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let assert Ok(tree) = test_helpers.insert(tree, s, 1, "hello")
  let assert Ok(tree2) = test_helpers.mark_deleted(tree, s, 99)
  assert btree.size(tree2) == btree.size(tree)
  assert btree.dirt(tree2) == btree.dirt(tree)
}

pub fn mark_deleted_in_multi_level_tree_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let entries = test_helpers.make_entries(1, 20)
  let tree = test_helpers.insert_all(tree, s, entries)

  let assert Ok(tree) = test_helpers.mark_deleted(tree, s, 15)
  let assert Ok(option.None) = test_helpers.lookup(tree, s, 15)
  let assert Ok(option.Some("val10")) = test_helpers.lookup(tree, s, 10)
  assert btree.size(tree) == 19
}

pub fn load_empty_list_returns_empty_tree_test() {
  use s <- test_helpers.with_store()

  let assert Ok(tree) =
    btree.load(
      entries: [],
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  assert btree.root(tree) == option.None
  assert btree.size(tree) == 0
  assert btree.dirt(tree) == 0
  assert btree.capacity(tree) == 4
  let assert Ok(option.None) = test_helpers.lookup(tree, s, 1)
  Nil
}

pub fn load_property_all_findable_and_correct_size_test() {
  use n <- qcheck.run(test_helpers.property_config(), qcheck.bounded_int(0, 50))
  use s <- test_helpers.with_store()

  let entries = test_helpers.make_entries(1, n)
  let assert Ok(tree) =
    btree.load(
      entries: entries,
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  assert btree.size(tree) == list.length(entries)
  assert btree.dirt(tree) == 0
  list.each(entries, fn(entry) {
    let #(k, v) = entry
    let assert Ok(option.Some(found)) = test_helpers.lookup(tree, s, k)
    assert found == v
  })
}

pub fn load_matches_insert_one_by_one_test() {
  use n <- qcheck.run(test_helpers.property_config(), qcheck.bounded_int(1, 30))
  use s <- test_helpers.with_store()

  let entries = test_helpers.make_entries(1, n)
  let assert Ok(loaded_tree) =
    btree.load(
      entries: entries,
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  let inserted_tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  assert btree.size(loaded_tree) == btree.size(inserted_tree)
  list.each(entries, fn(entry) {
    let #(k, _) = entry
    let assert Ok(loaded_val) = test_helpers.lookup(loaded_tree, s, k)
    let assert Ok(inserted_val) = test_helpers.lookup(inserted_tree, s, k)
    assert loaded_val == inserted_val
  })
}

pub fn insert_property_last_value_wins_test() {
  use pairs <- qcheck.run(
    test_helpers.property_config(),
    qcheck.generic_list(
      elements_from: qcheck.map2(
        qcheck.bounded_int(0, 20),
        qcheck.bounded_int(0, 100),
        fn(k, v) { #(k, v) },
      ),
      length_from: qcheck.bounded_int(1, 30),
    ),
  )
  use s <- test_helpers.with_store()

  let entries = list.map(pairs, fn(p) { #(p.0, "v" <> int.to_string(p.1)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  let last_values =
    list.fold(entries, [], fn(acc, entry) {
      list.key_set(acc, entry.0, entry.1)
    })

  list.each(last_values, fn(entry) {
    let #(k, v) = entry
    let assert Ok(option.Some(found)) = test_helpers.lookup(tree, s, k)
    assert found == v
  })
}

pub fn load_from_yielder_matches_load_property_test() {
  use n <- qcheck.run(test_helpers.property_config(), qcheck.bounded_int(0, 50))
  use s <- test_helpers.with_store()

  let entries = test_helpers.make_entries(1, n)

  let assert Ok(loaded_tree) =
    btree.load(
      entries: entries,
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )

  let assert Ok(yielder_tree) =
    btree.load_from_yielder(
      entries: yielder.from_list(entries),
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )

  assert btree.size(loaded_tree) == btree.size(yielder_tree)

  list.each(entries, fn(entry) {
    let #(k, _) = entry
    let assert Ok(loaded_val) = test_helpers.lookup(loaded_tree, s, k)
    let assert Ok(yielder_val) = test_helpers.lookup(yielder_tree, s, k)
    assert loaded_val == yielder_val
  })
}

pub fn contains_matches_lookup_property_test() {
  use keys <- qcheck.run(
    test_helpers.property_config(),
    qcheck.list_from(qcheck.bounded_int(-100, 100)),
  )
  use s <- test_helpers.with_store()

  let unique_keys = list.unique(keys)
  let entries = list.map(unique_keys, fn(k) { #(k, "v" <> int.to_string(k)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  let #(to_delete, _) = list.split(unique_keys, list.length(unique_keys) / 2)
  let tree = test_helpers.delete_all(tree, s, to_delete)

  list.each(test_helpers.int_list(from: -100, to: 100), fn(k) {
    let assert Ok(has) =
      btree.contains(
        tree: tree,
        store: s,
        key: k,
        key_codec: codec.int(),
        compare: int.compare,
      )
    let assert Ok(val) = test_helpers.lookup(tree, s, k)
    assert has == option.is_some(val)
  })
}

pub fn from_header_capacity_1_returns_error_test() {
  let assert Error(btree.ValidationError("capacity must be at least 2")) =
    btree.from_header(root: option.None, size: 0, dirt: 0, capacity: 1)
  Nil
}

pub fn load_capacity_1_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("capacity must be at least 2")) =
    btree.load(
      entries: [#(1, "a")],
      store: s,
      capacity: 1,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn load_from_yielder_capacity_1_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("capacity must be at least 2")) =
    btree.load_from_yielder(
      entries: yielder.from_list([#(1, "a")]),
      store: s,
      capacity: 1,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn load_duplicate_keys_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("duplicate key in bulk load input")) =
    btree.load(
      entries: [#(1, "a"), #(1, "b")],
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn load_unsorted_keys_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("unsorted key in bulk load input")) =
    btree.load(
      entries: [#(2, "a"), #(1, "b")],
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn load_from_yielder_duplicate_keys_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("duplicate key in bulk load input")) =
    btree.load_from_yielder(
      entries: yielder.from_list([#(1, "a"), #(1, "b")]),
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn load_from_yielder_unsorted_keys_returns_error_test() {
  use s <- test_helpers.with_store()
  let assert Error(btree.ValidationError("unsorted key in bulk load input")) =
    btree.load_from_yielder(
      entries: yielder.from_list([#(2, "a"), #(1, "b")]),
      store: s,
      capacity: 4,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  Nil
}

pub fn new_with_capacity_1_panics_test() {
  let assert Error(_) =
    exception.rescue(fn() { btree.new_with_capacity(capacity: 1) })
  Nil
}

pub fn load_capacity_2_small_dataset_test() {
  use s <- test_helpers.with_store()
  let assert Ok(tree) =
    btree.load(
      entries: [#(1, "a"), #(2, "b"), #(3, "c")],
      store: s,
      capacity: 2,
      key_codec: codec.int(),
      value_codec: codec.string(),
      compare: int.compare,
    )
  assert btree.size(tree) == 3
  let assert Ok(option.Some("a")) = test_helpers.lookup(tree, s, 1)
  let assert Ok(option.Some("b")) = test_helpers.lookup(tree, s, 2)
  let assert Ok(option.Some("c")) = test_helpers.lookup(tree, s, 3)
  Nil
}
