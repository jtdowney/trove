import gleam/erlang/process
import gleam/list
import gleam/option
import simplifile
import trove
import trove/range
import trove/test_helpers

pub fn snapshot_closes_reader_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "hello")

  list.each(test_helpers.int_list(from: 1, to: 500), fn(_) {
    let result =
      trove.with_snapshot(db, fn(snap) {
        trove.snapshot_get(snapshot: snap, key: 1)
      })
    let assert Ok("hello") = result
    Nil
  })

  list.each(test_helpers.int_list(from: 1, to: 500), fn(_) {
    let entries =
      trove.range(
        db,
        min: option.None,
        max: option.None,
        direction: range.Forward,
      )
    let assert [#(1, "hello")] = entries
    Nil
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// A panicking snapshot callback must still close the reader handle.
pub fn snapshot_callback_panic_closes_reader_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "hello")

  let pid =
    process.spawn_unlinked(fn() {
      trove.with_snapshot(db, fn(_snap) { panic as "snap boom" })
    })

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  let assert Ok(Nil) = process.selector_receive(selector, 5000)

  let assert Ok("hello") = trove.get(db, key: 1)
  trove.put(db, key: 2, value: "world")
  let assert Ok("world") = trove.get(db, key: 2)

  // Opening many more snapshots must not leak handles
  list.each(test_helpers.int_list(from: 1, to: 100), fn(_) {
    trove.with_snapshot(db, fn(snap) {
      let assert Ok("hello") = trove.snapshot_get(snapshot: snap, key: 1)
      Nil
    })
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}
