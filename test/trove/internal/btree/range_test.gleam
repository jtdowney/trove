import gleam/int
import gleam/list
import gleam/option
import gleam/yielder
import qcheck
import trove/internal/btree
import trove/range
import trove/test_helpers

pub fn query_empty_tree_returns_empty_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()
  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Forward)
  assert yielder.to_list(result) == []
}

pub fn query_empty_tree_reverse_returns_empty_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new()
  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Reverse)
  assert yielder.to_list(result) == []
}

pub fn query_full_range_returns_all_sorted_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Forward)
  assert yielder.to_list(result) == entries
}

pub fn query_inclusive_bounds_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(3)),
      option.Some(range.Inclusive(7)),
      range.Forward,
    )

  let expected = test_helpers.make_entries(3, 7)
  assert yielder.to_list(result) == expected
}

pub fn query_exclusive_bounds_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Exclusive(3)),
      option.Some(range.Exclusive(7)),
      range.Forward,
    )

  let expected = test_helpers.make_entries(4, 6)
  assert yielder.to_list(result) == expected
}

pub fn query_reverse_order_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Reverse)
  assert yielder.to_list(result) == list.reverse(entries)
}

pub fn query_empty_range_min_greater_than_max_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(8)),
      option.Some(range.Inclusive(3)),
      range.Forward,
    )

  assert yielder.to_list(result) == []
}

pub fn query_skips_deleted_entries_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)

  let entries = test_helpers.make_entries(1, 5)
  let tree = test_helpers.insert_all(tree, s, entries)
  let assert Ok(tree) = test_helpers.mark_deleted(tree, s, 3)

  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Forward)

  let expected = [#(1, "val1"), #(2, "val2"), #(4, "val4"), #(5, "val5")]
  assert yielder.to_list(result) == expected
}

pub fn query_min_only_bound_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(5)),
      option.None,
      range.Forward,
    )

  let expected = test_helpers.make_entries(5, 10)
  assert yielder.to_list(result) == expected
}

pub fn query_max_only_bound_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.None,
      option.Some(range.Exclusive(6)),
      range.Forward,
    )

  let expected = test_helpers.make_entries(1, 5)
  assert yielder.to_list(result) == expected
}

pub fn query_reverse_with_bounds_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(3)),
      option.Some(range.Inclusive(7)),
      range.Reverse,
    )

  let expected = list.reverse(test_helpers.make_entries(3, 7))
  assert yielder.to_list(result) == expected
}

pub fn query_mixed_bounds_inclusive_min_exclusive_max_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(3)),
      option.Some(range.Exclusive(7)),
      range.Forward,
    )

  let expected = test_helpers.make_entries(3, 6)
  assert yielder.to_list(result) == expected
}

pub fn query_property_all_entries_returned_sorted_test() {
  use keys <- qcheck.run(
    test_helpers.property_config(),
    qcheck.list_from(qcheck.bounded_int(-500, 500)),
  )
  use s <- test_helpers.with_store()

  let unique_keys = list.unique(keys)
  let entries = list.map(unique_keys, fn(k) { #(k, "v" <> int.to_string(k)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  let result =
    test_helpers.query(tree, s, option.None, option.None, range.Forward)

  let expected = list.sort(entries, fn(a, b) { int.compare(a.0, b.0) })
  assert yielder.to_list(result) == expected
}

pub fn query_property_bounds_filter_correctly_test() {
  use input <- qcheck.run(
    test_helpers.property_config(),
    qcheck.tuple2(
      qcheck.list_from(qcheck.bounded_int(0, 100)),
      qcheck.tuple2(qcheck.bounded_int(0, 100), qcheck.bounded_int(0, 100)),
    ),
  )
  let #(keys, #(bound_a, bound_b)) = input
  let lo = int.min(bound_a, bound_b)
  let hi = int.max(bound_a, bound_b)

  use s <- test_helpers.with_store()

  let unique_keys = list.unique(keys)
  let entries = list.map(unique_keys, fn(k) { #(k, "v" <> int.to_string(k)) })
  let tree =
    test_helpers.insert_all(btree.new_with_capacity(capacity: 4), s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Inclusive(lo)),
      option.Some(range.Inclusive(hi)),
      range.Forward,
    )

  let result_list = yielder.to_list(result)
  let expected =
    list.sort(entries, fn(a, b) { int.compare(a.0, b.0) })
    |> list.filter(fn(e) { e.0 >= lo && e.0 <= hi })

  assert result_list == expected
}

pub fn query_exclusive_same_key_returns_empty_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Exclusive(5)),
      option.Some(range.Exclusive(5)),
      range.Forward,
    )

  assert yielder.to_list(result) == []
}

pub fn query_exclusive_min_inclusive_max_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 10)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result =
    test_helpers.query(
      tree,
      s,
      option.Some(range.Exclusive(3)),
      option.Some(range.Inclusive(7)),
      range.Forward,
    )

  let expected = test_helpers.make_entries(4, 7)
  assert yielder.to_list(result) == expected
}
