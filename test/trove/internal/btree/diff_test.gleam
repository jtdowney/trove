import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/yielder
import qcheck
import trove/internal/btree
import trove/internal/btree/diff
import trove/range
import trove/test_helpers

pub fn diff_identical_trees_is_empty_test() {
  use s <- test_helpers.with_store()
  let tree = btree.new_with_capacity(capacity: 4)
  let entries = test_helpers.make_entries(1, 5)
  let tree = test_helpers.insert_all(tree, s, entries)

  let result = test_helpers.compute_diff(tree, tree, s)
  assert yielder.to_list(result) == []
}

pub fn diff_new_entries_show_as_upserted_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let old_tree = test_helpers.insert_all(old_tree, s, [#(1, "a"), #(3, "c")])

  let assert Ok(new_tree) = test_helpers.insert(old_tree, s, 2, "b")

  let result = test_helpers.compute_diff(old_tree, new_tree, s)
  assert yielder.to_list(result) == [#(2, diff.Upserted("b"))]
}

pub fn diff_mark_deleted_shows_as_removed_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let old_tree =
    test_helpers.insert_all(old_tree, s, test_helpers.make_entries(1, 5))

  let assert Ok(new_tree) = test_helpers.mark_deleted(old_tree, s, 3)

  let result = test_helpers.compute_diff(old_tree, new_tree, s)
  assert yielder.to_list(result) == [#(3, diff.Removed)]
}

pub fn diff_modified_value_shows_as_upserted_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let old_tree =
    test_helpers.insert_all(old_tree, s, [#(1, "a"), #(2, "b"), #(3, "c")])

  let assert Ok(new_tree) = test_helpers.insert(old_tree, s, 2, "updated")

  let result = test_helpers.compute_diff(old_tree, new_tree, s)
  assert yielder.to_list(result) == [#(2, diff.Upserted("updated"))]
}

pub fn diff_empty_to_populated_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let new_tree =
    test_helpers.insert_all(
      btree.new_with_capacity(capacity: 4),
      s,
      test_helpers.make_entries(1, 3),
    )

  let result = test_helpers.compute_diff(old_tree, new_tree, s)

  let expected = [
    #(1, diff.Upserted("val1")),
    #(2, diff.Upserted("val2")),
    #(3, diff.Upserted("val3")),
  ]
  assert yielder.to_list(result) == expected
}

pub fn diff_populated_to_empty_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let old_tree =
    test_helpers.insert_all(old_tree, s, test_helpers.make_entries(1, 3))

  let assert Ok(new_tree) = test_helpers.mark_deleted(old_tree, s, 1)
  let assert Ok(new_tree) = test_helpers.mark_deleted(new_tree, s, 2)
  let assert Ok(new_tree) = test_helpers.mark_deleted(new_tree, s, 3)

  let result = test_helpers.compute_diff(old_tree, new_tree, s)

  let expected = [
    #(1, diff.Removed),
    #(2, diff.Removed),
    #(3, diff.Removed),
  ]
  assert yielder.to_list(result) == expected
}

pub fn diff_property_captures_all_mutations_test() {
  use ops <- qcheck.run(
    test_helpers.property_config(),
    qcheck.generic_list(
      elements_from: qcheck.tuple2(qcheck.bounded_int(1, 20), qcheck.bool()),
      length_from: qcheck.bounded_int(1, 20),
    ),
  )
  use s <- test_helpers.with_store()

  let old_tree =
    test_helpers.insert_all(
      btree.new_with_capacity(capacity: 4),
      s,
      test_helpers.make_entries(1, 10),
    )

  let new_tree =
    list.fold(ops, old_tree, fn(tree, op) {
      let #(key, is_insert) = op
      case is_insert {
        True -> {
          let assert Ok(t) =
            test_helpers.insert(tree, s, key, "new" <> int.to_string(key))
          t
        }
        False -> {
          let assert Ok(t) = test_helpers.mark_deleted(tree, s, key)
          t
        }
      }
    })

  let diff_yielder = test_helpers.compute_diff(old_tree, new_tree, s)
  let diff_entries = yielder.to_list(diff_yielder)

  let diff_keys = list.map(diff_entries, fn(e) { e.0 })
  assert diff_keys == list.unique(diff_keys)

  let sorted_keys = list.sort(diff_keys, int.compare)
  assert diff_keys == sorted_keys

  list.each(diff_entries, fn(entry) {
    let #(key, change) = entry
    let assert Ok(old_val) = test_helpers.lookup(old_tree, s, key)
    let assert Ok(new_val) = test_helpers.lookup(new_tree, s, key)
    case change {
      diff.Upserted(value) -> {
        assert new_val == option.Some(value)
        assert old_val != new_val
      }
      diff.Removed -> {
        assert new_val == option.None
        assert old_val != option.None
      }
    }
  })
}

pub fn diff_delete_shows_as_removed_test() {
  use s <- test_helpers.with_store()

  let old_tree = btree.new_with_capacity(capacity: 4)
  let old_tree =
    test_helpers.insert_all(old_tree, s, test_helpers.make_entries(1, 3))

  let assert Ok(new_tree) = test_helpers.delete(old_tree, s, 2)

  let result = test_helpers.compute_diff(old_tree, new_tree, s)
  assert yielder.to_list(result) == [#(2, diff.Removed)]
}

pub fn diff_applied_to_old_produces_new_property_test() {
  use ops <- qcheck.run(
    test_helpers.property_config(),
    qcheck.generic_list(
      elements_from: qcheck.tuple2(qcheck.bounded_int(1, 20), qcheck.bool()),
      length_from: qcheck.bounded_int(1, 20),
    ),
  )
  use s <- test_helpers.with_store()

  let old_tree =
    test_helpers.insert_all(
      btree.new_with_capacity(capacity: 4),
      s,
      test_helpers.make_entries(1, 10),
    )

  let new_tree =
    list.fold(ops, old_tree, fn(tree, op) {
      let #(key, is_insert) = op
      case is_insert {
        True -> {
          let assert Ok(t) =
            test_helpers.insert(tree, s, key, "new" <> int.to_string(key))
          t
        }
        False -> {
          let assert Ok(t) = test_helpers.mark_deleted(tree, s, key)
          t
        }
      }
    })

  let old_entries =
    test_helpers.query(old_tree, s, option.None, option.None, range.Forward)
    |> yielder.to_list()
  let old_dict = dict.from_list(old_entries)

  let diff_entries =
    test_helpers.compute_diff(old_tree, new_tree, s) |> yielder.to_list()
  let reconstructed =
    list.fold(diff_entries, old_dict, fn(d, entry) {
      let #(key, change) = entry
      case change {
        diff.Upserted(value) -> dict.insert(d, key, value)
        diff.Removed -> dict.delete(d, key)
      }
    })

  let new_entries =
    test_helpers.query(new_tree, s, option.None, option.None, range.Forward)
    |> yielder.to_list()
  let new_dict = dict.from_list(new_entries)

  assert reconstructed == new_dict
}
