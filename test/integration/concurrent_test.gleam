import gleam/bool
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleam/yielder
import simplifile
import trove
import trove/range
import trove/test_helpers

/// Concurrent readers take snapshots, writer mutates, readers verify
/// they still see their original point-in-time view.
///
/// Coordination: each spawned reader creates its own `go` subject
/// (so it can receive on it) and sends it back to the main process
/// through a shared `ready` subject.
pub fn concurrent_snapshot_isolation_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let initial_entries =
    test_helpers.int_list(from: 0, to: 49)
    |> list.map(fn(i) { #(i, "v" <> int.to_string(i)) })
  trove.put_multi(db, entries: initial_entries)

  let initial_sorted =
    list.sort(initial_entries, fn(a, b) { int.compare(a.0, b.0) })

  let ready_subject: process.Subject(process.Subject(Nil)) =
    process.new_subject()
  let done_subject: process.Subject(Bool) = process.new_subject()

  let reader_count = 10
  list.each(test_helpers.int_list(from: 1, to: reader_count), fn(_) {
    process.spawn(fn() {
      // Each reader creates its own go subject (so it can receive on it)
      let go: process.Subject(Nil) = process.new_subject()

      let snapshot_entries =
        trove.with_snapshot(db, fn(snap) {
          // Signal ready and hand back our go subject
          process.send(ready_subject, go)

          // Wait for the go signal (on our own subject — allowed)
          let assert Ok(Nil) = process.receive(go, 10_000)

          let entries =
            trove.snapshot_range(
              snapshot: snap,
              min: option.None,
              max: option.None,
              direction: range.Forward,
            )
          yielder.to_list(entries)
        })

      process.send(done_subject, snapshot_entries == initial_sorted)
    })
  })

  // Collect all go subjects (blocks until every reader is ready)
  let go_subjects =
    test_helpers.int_list(from: 1, to: reader_count)
    |> list.map(fn(_) {
      let assert Ok(go) = process.receive(ready_subject, 10_000)
      go
    })

  list.each(test_helpers.int_list(from: 0, to: 49), fn(i) {
    trove.put(db, key: i, value: "mutated_" <> int.to_string(i))
  })
  trove.put_multi(
    db,
    entries: test_helpers.int_list(from: 50, to: 99)
      |> list.map(fn(i) { #(i, "new_" <> int.to_string(i)) }),
  )
  list.each(test_helpers.int_list(from: 0, to: 9), fn(i) {
    trove.delete(db, key: i)
  })

  list.each(go_subjects, fn(go) { process.send(go, Nil) })

  list.each(test_helpers.int_list(from: 1, to: reader_count), fn(_) {
    let assert Ok(True) = process.receive(done_subject, 10_000)
  })

  list.each(test_helpers.int_list(from: 0, to: 9), fn(i) {
    let assert Error(Nil) = trove.get(db, key: i)
  })
  list.each(test_helpers.int_list(from: 10, to: 49), fn(i) {
    let assert Ok(val) = trove.get(db, key: i)
    assert val == "mutated_" <> int.to_string(i)
  })
  list.each(test_helpers.int_list(from: 50, to: 99), fn(i) {
    let assert Ok(val) = trove.get(db, key: i)
    assert val == "new_" <> int.to_string(i)
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// Multiple sequential snapshots: each reflects the state at
/// the time it was taken, even as the database continues to change.
pub fn sequential_snapshot_consistency_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "a")
  let snap1_data =
    trove.with_snapshot(db, fn(snap) {
      trove.put(db, key: 2, value: "b")
      let entries =
        trove.snapshot_range(
          snapshot: snap,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      yielder.to_list(entries)
    })
  assert snap1_data == [#(1, "a")]

  let snap2_data =
    trove.with_snapshot(db, fn(snap) {
      trove.put(db, key: 3, value: "c")
      let entries =
        trove.snapshot_range(
          snapshot: snap,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      yielder.to_list(entries)
    })
  assert snap2_data == [#(1, "a"), #(2, "b")]

  let snap3_data =
    trove.with_snapshot(db, fn(snap) {
      let entries =
        trove.snapshot_range(
          snapshot: snap,
          min: option.None,
          max: option.None,
          direction: range.Forward,
        )
      yielder.to_list(entries)
    })
  assert snap3_data == [#(1, "a"), #(2, "b"), #(3, "c")]

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// Concurrent writes through transactions are serialized: no lost updates.
pub fn concurrent_transaction_serialization_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 0, value: "0")

  let num_writers = 20
  let done_subject: process.Subject(Nil) = process.new_subject()

  list.each(test_helpers.int_list(from: 1, to: num_writers), fn(i) {
    process.spawn(fn() {
      trove.transaction(db, timeout: 10_000, callback: fn(tx) {
        let tx = trove.tx_put(tx, key: i, value: "writer_" <> int.to_string(i))
        trove.Commit(tx:, result: Nil)
      })
      process.send(done_subject, Nil)
    })
  })

  list.each(test_helpers.int_list(from: 1, to: num_writers), fn(_) {
    let assert Ok(Nil) = process.receive(done_subject, 10_000)
  })

  list.each(test_helpers.int_list(from: 1, to: num_writers), fn(i) {
    let assert Ok(val) = trove.get(db, key: i)
    assert val == "writer_" <> int.to_string(i)
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// A panicking transaction callback must not kill the DB actor.
pub fn transaction_callback_panic_preserves_actor_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "before")

  // Run the panicking transaction in an unlinked process so the re-raised
  // panic does not propagate to the test process.
  let pid =
    process.spawn_unlinked(fn() {
      trove.transaction(db, timeout: 5000, callback: fn(_tx) { panic as "boom" })
    })

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  let assert Ok(Nil) = process.selector_receive(selector, 5000)

  let assert Ok("before") = trove.get(db, key: 1)

  trove.put(db, key: 2, value: "after")
  let assert Ok("after") = trove.get(db, key: 2)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// Returning a stale Tx handle from a different transaction must crash
/// the caller (token mismatch via reraise), but the actor must survive.
pub fn stale_transaction_handle_crashes_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: 1, value: "before")

  // Capture a tx handle from a cancelled transaction
  let captured_subject = process.new_subject()
  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    process.send(captured_subject, tx)
    trove.Cancel(result: Nil)
  })
  let assert Ok(stale_tx) = process.receive(captured_subject, 0)

  // Use a stale tx in a spawned process so the reraise doesn't kill the test
  let escaped_subject = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      trove.transaction(db, timeout: 5000, callback: fn(_fresh_tx) {
        trove.Commit(tx: stale_tx, result: Nil)
      })
      // Should never reach here — reraise kills this process
      process.send(escaped_subject, Nil)
    })

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  let assert Ok(Nil) = process.selector_receive(selector, 5000)

  let assert Error(Nil) = process.receive(escaped_subject, 0)

  let assert Ok("before") = trove.get(db, key: 1)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// Concurrent readers never see inconsistent state while a writer is active.
pub fn concurrent_read_during_write_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let entries =
    test_helpers.int_list(from: 0, to: 99)
    |> list.map(fn(i) { #(i, "v" <> int.to_string(i)) })
  trove.put_multi(db, entries: entries)

  let read_done: process.Subject(Bool) = process.new_subject()

  list.each(test_helpers.int_list(from: 1, to: 5), fn(_) {
    process.spawn(fn() {
      let all_ok =
        list.all(test_helpers.int_list(from: 1, to: 10), fn(_) {
          let results =
            trove.range(
              db,
              min: option.None,
              max: option.None,
              direction: range.Forward,
            )
          let sorted = list.sort(results, fn(a, b) { int.compare(a.0, b.0) })
          let is_sorted = results == sorted

          let all_valid =
            list.all(results, fn(pair) {
              let #(i, v) = pair
              v == "v" <> int.to_string(i)
              || v == "updated_" <> int.to_string(i)
            })

          // Writer updates keys 0..99 sequentially, so a consistent snapshot
          // must show all "updated_" keys before any "v" keys (no interleaving).
          // Once we see a non-updated value, every subsequent must also be
          // non-updated.
          let is_consistent = {
            let #(ok, _) =
              list.fold(results, #(True, False), fn(acc, pair) {
                let #(ok_so_far, seen_old) = acc
                let is_old = string.starts_with(pair.1, "v")
                use <- bool.guard(!ok_so_far, #(False, seen_old))
                case seen_old, is_old {
                  False, False -> #(True, False)
                  False, True -> #(True, True)
                  True, True -> #(True, True)
                  // Torn read: old region followed by updated
                  True, False -> #(False, True)
                }
              })
            ok
          }

          is_sorted && all_valid && is_consistent
        })
      process.send(read_done, all_ok)
    })
  })

  list.each(test_helpers.int_list(from: 0, to: 99), fn(i) {
    trove.put(db, key: i, value: "updated_" <> int.to_string(i))
  })

  list.each(test_helpers.int_list(from: 1, to: 5), fn(_) {
    let assert Ok(True) = process.receive(read_done, 10_000)
  })

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn double_open_same_path_returns_lock_error_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let assert Error(trove.LockError(_)) = trove.open(config)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn lock_released_after_close_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.close(db)

  let assert Ok(db2) = trove.open(config)
  trove.close(db2)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn stale_lock_cleaned_on_reopen_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)

  let done_subject = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      let assert Ok(_db) = trove.open(config)
      process.send(done_subject, Nil)
      // Block forever — we'll kill this process
      let blocker = process.new_subject()
      let _ = process.receive(blocker, 60_000)
      Nil
    })
  let assert Ok(Nil) = process.receive(done_subject, 5000)

  process.kill(pid)
  process.sleep(50)

  let assert Ok(db) = trove.open(config)
  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn locked_open_does_not_delete_files_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)
  trove.put(db, key: 1, value: "safe")

  // Plant a bogus store file that find_valid_store_number would delete
  let assert Ok(Nil) = simplifile.write(dir <> "/99.trv", "garbage")

  let assert Error(trove.LockError(_)) = trove.open(config)

  let assert Ok(True) = simplifile.is_file(dir <> "/99.trv")

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn stale_tx_within_callback_crashes_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  let escaped_subject = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      trove.transaction(db, timeout: 5000, callback: fn(tx) {
        let stale_tx = trove.tx_put(tx, key: 1, value: "a")
        let _latest_tx = trove.tx_put(stale_tx, key: 2, value: "b")
        // Commit the stale handle — drops key=2 write
        trove.Commit(tx: stale_tx, result: Nil)
      })
      process.send(escaped_subject, Nil)
    })

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  let assert Ok(Nil) = process.selector_receive(selector, 5000)

  let assert Error(Nil) = process.receive(escaped_subject, 0)

  let assert Error(Nil) = trove.get(db, key: 1)
  let assert Error(Nil) = trove.get(db, key: 2)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

/// When the caller times out but the callback commits, the write must
/// still be applied by the actor.  The caller's process crashes (the
/// OTP call raises `CallTimeout`), but the actor keeps running.
pub fn transaction_timeout_still_commits_test() {
  let dir = test_helpers.temp_dir()
  let config = test_helpers.int_string_config(dir)
  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "before")

  let pid =
    process.spawn_unlinked(fn() {
      trove.transaction(db, timeout: 50, callback: fn(tx) {
        process.sleep(300)
        let tx = trove.tx_put(tx, key: 2, value: "committed-after-timeout")
        trove.Commit(tx:, result: Nil)
      })
    })

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  let assert Ok(Nil) = process.selector_receive(selector, 5000)

  // Give the actor time to finish processing the callback
  process.sleep(500)

  let assert Ok("committed-after-timeout") = trove.get(db, key: 2)
  let assert Ok("before") = trove.get(db, key: 1)

  trove.close(db)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}
