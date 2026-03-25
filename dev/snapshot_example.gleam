import gleam/int
import gleam/io
import gleam/option
import gleam/yielder
import trove
import trove/codec
import trove/range

pub fn main() {
  let config =
    trove.Config(
      path: "/tmp/trove_snapshot_example",
      key_codec: codec.int(),
      value_codec: codec.string(),
      key_compare: int.compare,
      auto_compact: trove.NoAutoCompact,
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  trove.put(db, key: 1, value: "original_one")
  trove.put(db, key: 2, value: "original_two")
  trove.put(db, key: 3, value: "original_three")

  io.println("=== Snapshot isolation demo ===")

  trove.with_snapshot(db, fn(snap) {
    trove.put(db, key: 2, value: "updated_two")
    trove.put(db, key: 4, value: "new_four")
    trove.delete(db, key: 3)

    io.println("\nSnapshot view (frozen at time of acquisition):")
    trove.snapshot_range(
      snapshot: snap,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
    |> yielder.each(fn(entry) {
      io.println("  " <> int.to_string(entry.0) <> " -> " <> entry.1)
    })
  })

  io.println("\nLive database view (reflects mutations):")
  trove.range(db, min: option.None, max: option.None, direction: range.Forward)
  |> print_entries()

  trove.close(db)
  io.println("\ndone")
}

fn print_entries(entries: List(#(Int, String))) -> Nil {
  case entries {
    [] -> Nil
    [#(key, value), ..rest] -> {
      io.println("  " <> int.to_string(key) <> " -> " <> value)
      print_entries(rest)
    }
  }
}
