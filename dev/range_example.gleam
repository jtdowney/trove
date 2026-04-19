import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/yielder
import trove
import trove/codec
import trove/range

pub fn main() -> Nil {
  let config =
    trove.Config(
      path: "/tmp/trove_range_example",
      key_codec: codec.int(),
      value_codec: codec.string(),
      key_compare: int.compare,
      auto_compact: trove.NoAutoCompact,
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  trove.put_multi(
    db,
    entries: int.range(from: 20, to: 0, with: [], run: list.prepend)
      |> list.map(fn(i) { #(i, "item_" <> int.to_string(i)) }),
  )

  io.println("=== All entries (forward) ===")
  trove.range(db, min: option.None, max: option.None, direction: range.Forward)
  |> print_entries()

  io.println("\n=== All entries (reverse) ===")
  trove.range(db, min: option.None, max: option.None, direction: range.Reverse)
  |> print_entries()

  io.println("\n=== Keys 5..10 (inclusive) ===")
  trove.range(
    db,
    min: option.Some(range.Inclusive(5)),
    max: option.Some(range.Inclusive(10)),
    direction: range.Forward,
  )
  |> print_entries()

  io.println("\n=== Keys > 15 ===")
  trove.range(
    db,
    min: option.Some(range.Exclusive(15)),
    max: option.None,
    direction: range.Forward,
  )
  |> print_entries()

  io.println("\n=== Lazy streaming (first 3 entries) ===")
  trove.with_snapshot(db, fn(snap) {
    trove.snapshot_range(
      snapshot: snap,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
    |> yielder.take(3)
    |> yielder.each(fn(entry) {
      io.println("  " <> int.to_string(entry.0) <> " -> " <> entry.1)
    })
  })

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
