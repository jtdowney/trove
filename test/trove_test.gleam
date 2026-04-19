import exception
import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleam/yielder
import qcheck
import simplifile
import trove
import trove/codec
import trove/range
import trove/test_helpers
import unitest

@external(erlang, "trove_test_ffi", "suppress_crash_reports")
fn suppress_crash_reports() -> Nil

pub fn main() -> Nil {
  suppress_crash_reports()
  unitest.main()
}

fn test_config(path: String) -> trove.Config(String, String) {
  trove.Config(
    path: path,
    key_codec: codec.string(),
    value_codec: codec.string(),
    key_compare: string.compare,
    auto_compact: trove.NoAutoCompact,
    auto_file_sync: trove.ManualSync,
    call_timeout: 5000,
  )
}

fn with_db(f: fn(trove.Db(String, String)) -> Nil) -> Nil {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)
  use <- exception.defer(fn() {
    trove.close(db)
    let _ = simplifile.delete_all([dir])
    Nil
  })
  f(db)
}

pub fn open_and_close_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn put_then_get_test() {
  with_db(fn(db) {
    trove.put(db, key: "hello", value: "world")
    let assert Ok("world") = trove.get(db, key: "hello")
    Nil
  })
}

pub fn get_nonexistent_key_returns_none_test() {
  with_db(fn(db) {
    let assert Error(Nil) = trove.get(db, key: "missing")
    Nil
  })
}

pub fn delete_then_get_returns_none_test() {
  with_db(fn(db) {
    trove.put(db, key: "key", value: "value")
    let assert Ok("value") = trove.get(db, key: "key")
    trove.delete(db, key: "key")
    let assert Error(Nil) = trove.get(db, key: "key")
    Nil
  })
}

pub fn has_key_test() {
  with_db(fn(db) {
    let assert False = trove.has_key(db, key: "key")
    trove.put(db, key: "key", value: "value")
    let assert True = trove.has_key(db, key: "key")
    trove.delete(db, key: "key")
    let assert False = trove.has_key(db, key: "key")
    Nil
  })
}

pub fn persistence_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)

  let assert Ok(db) = trove.open(config)
  trove.put(db, key: "persist", value: "me")
  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("me") = trove.get(db2, key: "persist")
  trove.close(db2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn put_multi_test() {
  with_db(fn(db) {
    let entries = [#("a", "1"), #("b", "2"), #("c", "3")]
    trove.put_multi(db, entries: entries)
    let assert Ok("1") = trove.get(db, key: "a")
    let assert Ok("2") = trove.get(db, key: "b")
    let assert Ok("3") = trove.get(db, key: "c")
    Nil
  })
}

pub fn delete_multi_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [#("a", "1"), #("b", "2"), #("c", "3")])
    trove.delete_multi(db, keys: ["a", "c"])
    let assert Error(Nil) = trove.get(db, key: "a")
    let assert Ok("2") = trove.get(db, key: "b")
    let assert Error(Nil) = trove.get(db, key: "c")
    Nil
  })
}

pub fn put_and_delete_multi_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [#("a", "1"), #("b", "2")])
    trove.put_and_delete_multi(db, puts: [#("c", "3"), #("d", "4")], deletes: [
      "a",
    ])
    let assert Error(Nil) = trove.get(db, key: "a")
    let assert Ok("2") = trove.get(db, key: "b")
    let assert Ok("3") = trove.get(db, key: "c")
    let assert Ok("4") = trove.get(db, key: "d")
    Nil
  })
}

pub fn transaction_commit_test() {
  with_db(fn(db) {
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let tx = trove.tx_put(tx, key: "a", value: "1")
      let tx = trove.tx_put(tx, key: "b", value: "2")
      trove.Commit(tx:, result: Nil)
    })
    let assert Ok("1") = trove.get(db, key: "a")
    let assert Ok("2") = trove.get(db, key: "b")
    Nil
  })
}

pub fn transaction_cancel_test() {
  with_db(fn(db) {
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let tx = trove.tx_put(tx, key: "x", value: "should_not_exist")
      let _ = tx
      trove.Cancel(result: Nil)
    })
    let assert Error(Nil) = trove.get(db, key: "x")
    Nil
  })
}

pub fn transaction_read_write_test() {
  with_db(fn(db) {
    trove.put(db, key: "key", value: "old")
    let assert "new" =
      trove.transaction(db, timeout: 5000, callback: fn(tx) {
        let assert Ok(old_val) = trove.tx_get(tx, key: "key")
        let tx = trove.tx_put(tx, key: "key", value: old_val <> "_new")
        trove.Commit(tx:, result: "new")
      })
    let assert Ok("old_new") = trove.get(db, key: "key")
    Nil
  })
}

pub fn transaction_read_after_write_test() {
  with_db(fn(db) {
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let tx = trove.tx_put(tx, key: "k", value: "v")
      let assert Ok(val) = trove.tx_get(tx, key: "k")
      let assert "v" = val
      trove.Commit(tx:, result: Nil)
    })
    Nil
  })
}

pub fn transaction_delete_test() {
  with_db(fn(db) {
    trove.put(db, key: "key", value: "value")
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let tx = trove.tx_delete(tx, key: "key")
      trove.Commit(tx:, result: Nil)
    })
    let assert Error(Nil) = trove.get(db, key: "key")
    Nil
  })
}

pub fn snapshot_isolation_test() {
  with_db(fn(db) {
    trove.put(db, key: "key", value: "before")
    let assert "before" =
      trove.with_snapshot(db, callback: fn(snap) {
        trove.put(db, key: "key", value: "after")
        let assert Ok("after") = trove.get(db, key: "key")
        let assert Ok("before") = trove.snapshot_get(snapshot: snap, key: "key")
        "before"
      })
    Nil
  })
}

pub fn snapshot_get_missing_key_test() {
  with_db(fn(db) {
    let result =
      trove.with_snapshot(db, callback: fn(snap) {
        trove.snapshot_get(snapshot: snap, key: "missing")
      })
    let assert Error(Nil) = result
    Nil
  })
}

pub fn double_compact_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put_multi(db, entries: [#("a", "1"), #("b", "2"), #("c", "3")])
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
  trove.put(db, key: "d", value: "4")
  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  let assert Ok("1") = trove.get(db, key: "a")
  let assert Ok("2") = trove.get(db, key: "b")
  let assert Ok("3") = trove.get(db, key: "c")
  let assert Ok("4") = trove.get(db, key: "d")

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn auto_compact_triggers_test() {
  let dir = test_helpers.temp_dir()
  let config =
    trove.Config(
      ..test_config(dir),
      auto_compact: trove.AutoCompact(min_dirt: 3, min_dirt_factor: 0.1),
    )
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "a", value: "1")
  trove.put(db, key: "a", value: "2")
  trove.put(db, key: "a", value: "3")
  trove.put(db, key: "a", value: "4")

  assert trove.dirt_factor(db) == 0.0

  let assert Ok("4") = trove.get(db, key: "a")

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn range_forward_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [#("a", "1"), #("b", "2"), #("c", "3")])
    let entries =
      trove.range(
        db,
        min: option.None,
        max: option.None,
        direction: range.Forward,
      )
    assert entries == [#("a", "1"), #("b", "2"), #("c", "3")]
  })
}

pub fn range_reverse_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [#("a", "1"), #("b", "2"), #("c", "3")])
    let entries =
      trove.range(
        db,
        min: option.None,
        max: option.None,
        direction: range.Reverse,
      )
    assert entries == [#("c", "3"), #("b", "2"), #("a", "1")]
  })
}

pub fn range_bounded_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [
      #("a", "1"),
      #("b", "2"),
      #("c", "3"),
      #("d", "4"),
    ])
    let entries =
      trove.range(
        db,
        min: option.Some(range.Inclusive("b")),
        max: option.Some(range.Exclusive("d")),
        direction: range.Forward,
      )
    assert entries == [#("b", "2"), #("c", "3")]
  })
}

pub fn dirt_factor_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)

  let df = trove.dirt_factor(db)
  assert df == 0.0

  trove.put(db, key: "a", value: "1")
  trove.put(db, key: "b", value: "2")
  trove.put(db, key: "a", value: "3")

  let df2 = trove.dirt_factor(db)
  assert df2 >. 0.0

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  let df3 = trove.dirt_factor(db)
  assert df3 == 0.0

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn file_sync_test() {
  let dir = test_helpers.temp_dir()
  let config =
    trove.Config(..test_config(dir), auto_file_sync: trove.ManualSync)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "k", value: "v")
  trove.file_sync(db)
  let assert Ok("v") = trove.get(db, key: "k")

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn set_auto_compact_at_runtime_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "a", value: "1")
  trove.put(db, key: "a", value: "2")
  trove.put(db, key: "a", value: "3")

  assert trove.dirt_factor(db) >. 0.0

  trove.set_auto_compact(
    db,
    setting: trove.AutoCompact(min_dirt: 1, min_dirt_factor: 0.0),
  )
  trove.put(db, key: "a", value: "4")

  assert trove.dirt_factor(db) == 0.0
  let assert Ok("4") = trove.get(db, key: "a")

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn auto_sync_persistence_test() {
  let dir = test_helpers.temp_dir()
  let config = trove.Config(..test_config(dir), auto_file_sync: trove.AutoSync)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "x", value: "1")
  trove.put(db, key: "y", value: "2")
  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  let assert Ok("1") = trove.get(db2, key: "x")
  let assert Ok("2") = trove.get(db2, key: "y")
  trove.close(db2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn empty_batch_ops_test() {
  with_db(fn(db) {
    trove.put(db, key: "a", value: "1")
    trove.put_multi(db, entries: [])
    trove.delete_multi(db, keys: [])
    trove.put_and_delete_multi(db, puts: [], deletes: [])
    let assert Ok("1") = trove.get(db, key: "a")
    Nil
  })
}

pub fn empty_batch_does_not_grow_file_test() {
  let dir = test_helpers.temp_dir()
  let config = test_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "a", value: "1")
  trove.file_sync(db)

  let assert Ok(info_before) = simplifile.file_info(dir <> "/0.trv")
  let size_before = info_before.size

  trove.put_multi(db, entries: [])
  trove.delete_multi(db, keys: [])
  trove.put_and_delete_multi(db, puts: [], deletes: [])
  trove.delete(db, key: "zzz_nonexistent")
  trove.file_sync(db)

  let assert Ok(info_after) = simplifile.file_info(dir <> "/0.trv")
  assert info_after.size == size_before

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

fn with_int_db(f: fn(trove.Db(Int, String)) -> Nil) -> Nil {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  use <- exception.defer(fn() {
    trove.close(db)
    let _ = simplifile.delete_all([dir])
    Nil
  })
  f(db)
}

type Op {
  OpPut(key: Int, value: String)
  OpDelete(key: Int)
}

fn small_property_config() -> qcheck.Config {
  qcheck.default_config() |> qcheck.with_test_count(25)
}

fn op_generator() -> qcheck.Generator(Op) {
  let key_gen = qcheck.bounded_int(0, 99)
  let value_gen = qcheck.non_empty_string()

  qcheck.from_generators(
    qcheck.map2(key_gen, value_gen, fn(k, v) { OpPut(k, v) }),
    [qcheck.map(key_gen, fn(k) { OpDelete(k) })],
  )
}

fn apply_ops(db: trove.Db(Int, String), ops: List(Op)) -> dict.Dict(Int, String) {
  list.fold(ops, dict.new(), fn(reference, op) {
    case op {
      OpPut(key, value) -> {
        trove.put(db, key: key, value: value)
        dict.insert(reference, key, value)
      }
      OpDelete(key) -> {
        trove.delete(db, key: key)
        dict.delete(reference, key)
      }
    }
  })
}

fn reference_sorted(reference: dict.Dict(Int, String)) -> List(#(Int, String)) {
  dict.to_list(reference)
  |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
}

fn assert_range_matches_reference(
  db: trove.Db(Int, String),
  reference: dict.Dict(Int, String),
) -> Nil {
  let expected = reference_sorted(reference)

  let forward_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert forward_list == expected

  let reverse_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Reverse,
    )
  assert reverse_list == list.reverse(expected)
}

pub fn ops_with_range_verification_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(1, 100),
    ),
  )

  use db <- with_int_db()

  let reference = apply_ops(db, ops)

  dict.each(reference, fn(key, value) {
    let assert Ok(val) = trove.get(db, key: key)
    assert val == value
  })

  assert_range_matches_reference(db, reference)
}

pub fn range_bounded_property_test() {
  use entries <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: qcheck.tuple2(
        qcheck.bounded_int(0, 199),
        qcheck.non_empty_string(),
      ),
      length_from: qcheck.bounded_int(5, 50),
    ),
  )

  use db <- with_int_db()

  let unique = dict.from_list(entries)
  let unique_list = dict.to_list(unique)
  trove.put_multi(db, entries: unique_list)

  let sorted = reference_sorted(unique)
  let mid = list.length(sorted) / 2
  let lo = case list.first(list.drop(sorted, mid / 2)) {
    Ok(#(k, _)) -> k
    Error(Nil) -> 0
  }
  let hi = case list.first(list.drop(sorted, mid + mid / 2)) {
    Ok(#(k, _)) -> k
    Error(Nil) -> 199
  }

  let expected =
    list.filter(sorted, fn(entry) { entry.0 >= lo && entry.0 <= hi })

  let result_list =
    trove.range(
      db,
      min: option.Some(range.Inclusive(lo)),
      max: option.Some(range.Inclusive(hi)),
      direction: range.Forward,
    )
  assert result_list == expected
}

pub fn snapshot_isolation_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.tuple2(
      qcheck.generic_list(
        elements_from: op_generator(),
        length_from: qcheck.bounded_int(1, 30),
      ),
      qcheck.generic_list(
        elements_from: op_generator(),
        length_from: qcheck.bounded_int(1, 30),
      ),
    ),
  )

  let #(initial_ops, mutation_ops) = ops
  use db <- with_int_db()

  let reference_before = apply_ops(db, initial_ops)
  let expected_before = reference_sorted(reference_before)

  let snapshot_entries =
    trove.with_snapshot(db, fn(snap) {
      let _reference_after = apply_ops(db, mutation_ops)

      let entries =
        trove.snapshot_range(
          snapshot: snap,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      yielder.to_list(entries)
    })

  assert snapshot_entries == expected_before
}

pub fn compaction_equivalence_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(10, 100),
    ),
  )

  use db <- with_int_db()

  let reference = apply_ops(db, ops)
  let expected = reference_sorted(reference)

  let before_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert before_list == expected

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)

  let after_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert after_list == expected

  assert trove.size(db) == dict.size(reference)

  dict.each(reference, fn(key, value) {
    let assert Ok(val) = trove.get(db, key: key)
    assert val == value
  })
}

pub fn transaction_matches_reference_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(1, 30),
    ),
  )

  use db <- with_int_db()

  let reference =
    list.fold(ops, dict.new(), fn(reference, op) {
      case op {
        OpPut(key, value) -> dict.insert(reference, key, value)
        OpDelete(key) -> dict.delete(reference, key)
      }
    })

  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    let final_tx =
      list.fold(ops, tx, fn(tx_acc, op) {
        case op {
          OpPut(key, value) -> {
            trove.tx_put(tx_acc, key: key, value: value)
          }
          OpDelete(key) -> {
            trove.tx_delete(tx_acc, key: key)
          }
        }
      })
    trove.Commit(tx: final_tx, result: Nil)
  })

  dict.each(reference, fn(key, value) {
    let assert Ok(val) = trove.get(db, key: key)
    assert val == value
  })

  assert_range_matches_reference(db, reference)
}

pub fn batch_ops_match_reference_property_test() {
  use input <- qcheck.run(
    small_property_config(),
    qcheck.tuple3(
      qcheck.generic_list(
        elements_from: qcheck.tuple2(
          qcheck.bounded_int(0, 99),
          qcheck.non_empty_string(),
        ),
        length_from: qcheck.bounded_int(0, 30),
      ),
      qcheck.generic_list(
        elements_from: qcheck.bounded_int(0, 99),
        length_from: qcheck.bounded_int(0, 10),
      ),
      qcheck.generic_list(
        elements_from: qcheck.tuple2(
          qcheck.bounded_int(0, 99),
          qcheck.non_empty_string(),
        ),
        length_from: qcheck.bounded_int(0, 20),
      ),
    ),
  )

  let #(initial_puts, delete_keys, second_puts) = input
  use db <- with_int_db()

  trove.put_multi(db, entries: initial_puts)
  trove.put_and_delete_multi(db, puts: second_puts, deletes: delete_keys)

  let reference = dict.from_list(initial_puts)
  let reference =
    list.fold(second_puts, reference, fn(d, e) { dict.insert(d, e.0, e.1) })
  let reference = list.fold(delete_keys, reference, dict.delete)

  dict.each(reference, fn(key, value) {
    let assert Ok(val) = trove.get(db, key: key)
    assert val == value
  })

  assert_range_matches_reference(db, reference)
}

pub fn has_key_matches_reference_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(1, 50),
    ),
  )

  use db <- with_int_db()

  let reference = apply_ops(db, ops)

  assert trove.size(db) == dict.size(reference)

  list.each(test_helpers.int_list(from: 0, to: 99), fn(key) {
    let has = trove.has_key(db, key: key)
    assert has == dict.has_key(reference, key)
  })
}

pub fn persistence_roundtrip_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(1, 100),
    ),
  )

  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let reference = apply_ops(db, ops)
  let expected = reference_sorted(reference)

  trove.close(db)
  let assert Ok(db2) = trove.open(config)

  use <- exception.defer(fn() {
    trove.close(db2)
    let _ = simplifile.delete_all([dir])
    Nil
  })

  dict.each(reference, fn(key, value) {
    let assert Ok(val) = trove.get(db2, key: key)
    assert val == value
  })

  let entries_list =
    trove.range(
      db2,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert entries_list == expected
}

pub fn transaction_cancel_tracks_dirt_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  assert trove.dirt_factor(db) == 0.0

  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    let tx = trove.tx_put(tx, key: 1, value: "a")
    let tx = trove.tx_put(tx, key: 2, value: "b")
    let tx = trove.tx_put(tx, key: 3, value: "c")
    let _ = tx
    trove.Cancel(result: Nil)
  })

  assert trove.dirt_factor(db) >. 0.0

  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  assert trove.dirt_factor(db2) >. 0.0
  trove.close(db2)

  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn transaction_delete_then_get_returns_none_test() {
  with_db(fn(db) {
    let result =
      trove.transaction(db, timeout: 5000, callback: fn(tx) {
        let tx = trove.tx_put(tx, key: "key", value: "value")
        let tx = trove.tx_delete(tx, key: "key")
        let got = trove.tx_get(tx, key: "key")
        trove.Commit(tx:, result: got)
      })
    assert result == Error(Nil)
    Nil
  })
}

pub fn tx_has_key_test() {
  with_db(fn(db) {
    trove.put(db, key: "existing", value: "yes")
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      assert trove.tx_has_key(tx, key: "existing") == True
      assert trove.tx_has_key(tx, key: "missing") == False

      let tx = trove.tx_put(tx, key: "new", value: "added")
      assert trove.tx_has_key(tx, key: "new") == True

      let tx = trove.tx_delete(tx, key: "existing")
      assert trove.tx_has_key(tx, key: "existing") == False

      trove.Commit(tx:, result: Nil)
    })
    Nil
  })
}

pub fn open_invalid_directory_returns_directory_error_test() {
  let config = test_config("/dev/null/impossible")
  let assert Error(trove.DirectoryError(_)) = trove.open(config)
  Nil
}

pub fn auto_compact_failure_continues_normally_test() {
  let dir = test_helpers.temp_dir()
  let config =
    trove.Config(
      ..test_config(dir),
      auto_compact: trove.AutoCompact(min_dirt: 1, min_dirt_factor: 0.0),
    )
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: "a", value: "1")
  trove.put(db, key: "a", value: "2")
  trove.put(db, key: "a", value: "3")
  let assert Ok("3") = trove.get(db, key: "a")
  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn snapshot_range_bounded_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [
      #("a", "1"),
      #("b", "2"),
      #("c", "3"),
      #("d", "4"),
      #("e", "5"),
    ])
    let entries =
      trove.with_snapshot(db, fn(snap) {
        trove.snapshot_range(
          snapshot: snap,
          min: option.Some(range.Inclusive("b")),
          max: option.Some(range.Exclusive("d")),
          direction: range.Forward,
        )
        |> yielder.to_list()
      })
    assert entries == [#("b", "2"), #("c", "3")]
  })
}

pub fn read_only_transaction_commit_is_noop_test() {
  with_db(fn(db) {
    trove.put(db, key: "key", value: "value")
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let assert Ok("value") = trove.tx_get(tx, key: "key")
      trove.Commit(tx:, result: Nil)
    })
    let assert Ok("value") = trove.get(db, key: "key")
    assert trove.size(db) == 1
    Nil
  })
}

pub fn range_empty_database_test() {
  with_db(fn(db) {
    let entries =
      trove.range(
        db,
        min: option.None,
        max: option.None,
        direction: range.Forward,
      )
    assert entries == []
  })
}

pub fn put_multi_duplicate_keys_last_wins_test() {
  with_db(fn(db) {
    trove.put_multi(db, entries: [
      #("key", "first"),
      #("key", "second"),
      #("key", "third"),
    ])
    let assert Ok("third") = trove.get(db, key: "key")
    assert trove.size(db) == 1
    Nil
  })
}

pub fn delete_nonexistent_key_succeeds_test() {
  with_db(fn(db) {
    trove.delete(db, key: "nonexistent")
    assert trove.size(db) == 0
    Nil
  })
}

pub fn is_empty_test() {
  with_db(fn(db) {
    assert trove.is_empty(db) == True
    trove.put(db, key: "a", value: "1")
    assert trove.is_empty(db) == False
    trove.delete(db, key: "a")
    assert trove.is_empty(db) == True
    Nil
  })
}

pub fn compaction_roundtrip_property_test() {
  use ops <- qcheck.run(
    small_property_config(),
    qcheck.generic_list(
      elements_from: op_generator(),
      length_from: qcheck.bounded_int(10, 100),
    ),
  )

  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let reference = apply_ops(db, ops)
  let expected = reference_sorted(reference)

  let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
  trove.close(db)
  let assert Ok(db2) = trove.open(config)

  use <- exception.defer(fn() {
    trove.close(db2)
    let _ = simplifile.delete_all([dir])
    Nil
  })

  let entries =
    trove.range(
      db2,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert entries == expected
}

fn users_keyspace(db: trove.Db(a, b)) -> trove.Keyspace(String, String) {
  trove.keyspace(
    db,
    name: "users",
    key_codec: codec.string(),
    value_codec: codec.string(),
    key_compare: string.compare,
  )
}

pub fn list_keyspaces_is_sorted_test() {
  use db <- test_helpers.with_open_db()
  let _ =
    trove.keyspace(
      db,
      name: "zulu",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
    )
  let _ =
    trove.keyspace(
      db,
      name: "alpha",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
    )
  assert trove.list_keyspaces(db) == ["alpha", "zulu"]
}

pub fn reserved_default_name_panics_test() {
  use db <- test_helpers.with_open_db()
  let result =
    exception.rescue(fn() {
      trove.keyspace(
        db,
        name: "__trove_default__",
        key_codec: codec.string(),
        value_codec: codec.string(),
        key_compare: string.compare,
      )
    })
  let assert Error(_) = result
  Nil
}

pub fn put_in_get_in_roundtrip_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("admin")
  assert trove.get_in(db, keyspace: users, key: "bob") == Error(Nil)
}

pub fn put_in_survives_close_reopen_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)

  let assert Ok(db) = trove.open(config)
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  let users2 = users_keyspace(db2)
  assert trove.get_in(db2, keyspace: users2, key: "alice") == Ok("admin")
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
}

pub fn delete_in_removes_key_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.put_in(db, keyspace: users, key: "bob", value: "member")

  trove.delete_in(db, keyspace: users, key: "alice")

  assert trove.get_in(db, keyspace: users, key: "alice") == Error(Nil)
  assert trove.get_in(db, keyspace: users, key: "bob") == Ok("member")
}

pub fn has_key_in_reflects_state_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")

  assert trove.has_key_in(db, keyspace: users, key: "alice") == True
  assert trove.has_key_in(db, keyspace: users, key: "bob") == False
}

pub fn size_in_counts_live_entries_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.put_in(db, keyspace: users, key: "bob", value: "member")
  trove.put_in(db, keyspace: users, key: "carol", value: "member")
  trove.delete_in(db, keyspace: users, key: "bob")

  assert trove.size_in(db, keyspace: users) == 2
}

pub fn range_in_respects_bounds_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "a")
  trove.put_in(db, keyspace: users, key: "bob", value: "b")
  trove.put_in(db, keyspace: users, key: "carol", value: "c")
  trove.put_in(db, keyspace: users, key: "dave", value: "d")

  let results =
    trove.range_in(
      db,
      keyspace: users,
      min: option.Some(range.Inclusive("bob")),
      max: option.Some(range.Exclusive("dave")),
      direction: range.Forward,
    )
  assert results == [#("bob", "b"), #("carol", "c")]
}

pub fn snapshot_isolates_keyspace_writes_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "v1")

  trove.with_snapshot(db, fn(snap) {
    trove.put_in(db, keyspace: users, key: "alice", value: "v2")
    assert trove.snapshot_get_in(snap, keyspace: users, key: "alice")
      == Ok("v1")
  })

  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("v2")
}

pub fn tx_writes_to_multiple_keyspaces_commit_atomically_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)

  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    let tx = trove.tx_put(tx, key: 42, value: "default")
    let tx = trove.tx_put_in(tx, keyspace: users, key: "alice", value: "admin")
    trove.Commit(tx:, result: Nil)
  })

  assert trove.get(db, key: 42) == Ok("default")
  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("admin")
}

pub fn tx_cancel_leaves_keyspace_untouched_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "original")

  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    let _tx =
      trove.tx_put_in(tx, keyspace: users, key: "alice", value: "replaced")
    trove.Cancel(result: Nil)
  })

  assert trove.get_in(db, keyspace: users, key: "alice") == Ok("original")
}

pub fn put_and_delete_multi_in_is_atomic_test() {
  use db <- test_helpers.with_open_db()
  let users = users_keyspace(db)
  trove.put_in(db, keyspace: users, key: "alice", value: "admin")
  trove.put_in(db, keyspace: users, key: "carol", value: "member")

  trove.put_and_delete_multi_in(
    db,
    keyspace: users,
    puts: [#("bob", "admin"), #("dave", "member")],
    deletes: ["alice"],
  )

  assert trove.get_in(db, keyspace: users, key: "alice") == Error(Nil)
  assert trove.get_in(db, keyspace: users, key: "bob") == Ok("admin")
  assert trove.get_in(db, keyspace: users, key: "carol") == Ok("member")
  assert trove.get_in(db, keyspace: users, key: "dave") == Ok("member")
}
