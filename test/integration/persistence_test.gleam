import gleam/int
import gleam/list
import simplifile
import trove
import trove/test_helpers

pub fn large_dataset_with_persistence_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let entries =
    test_helpers.int_list(from: 0, to: 999)
    |> list.map(fn(i) { #(i, "value_" <> int.to_string(i)) })
  trove.put_multi(db, entries: entries)

  let samples = [0, 42, 123, 456, 789, 999]
  list.each(samples, fn(i) {
    let assert Ok(val) = trove.get(db, key: i)
    assert val == "value_" <> int.to_string(i)
  })

  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  list.each(samples, fn(i) {
    let assert Ok(val) = trove.get(db2, key: i)
    assert val == "value_" <> int.to_string(i)
  })

  let assert Ok(Nil) = trove.compact(db2, timeout: 60_000)
  list.each(samples, fn(i) {
    let assert Ok(val) = trove.get(db2, key: i)
    assert val == "value_" <> int.to_string(i)
  })

  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}
