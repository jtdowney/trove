import gleam/int
import gleam/io
import gleam/string
import trove
import trove/codec

pub fn main() -> Nil {
  let config =
    trove.Config(
      path: "/tmp/trove_basic_example",
      key_codec: codec.string(),
      value_codec: codec.string(),
      key_compare: string.compare,
      auto_compact: trove.NoAutoCompact,
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "language", value: "gleam")
  trove.put(db, key: "runtime", value: "beam")
  trove.put(db, key: "author", value: "jtdowney")

  let assert Ok(value) = trove.get(db, key: "language")
  io.println("language = " <> value)

  let assert True = trove.has_key(db, key: "runtime")
  io.println("has_key(runtime) = True")

  trove.delete(db, key: "author")
  let assert Error(Nil) = trove.get(db, key: "author")
  io.println("author deleted")

  trove.put_multi(db, entries: [
    #("version", "1"),
    #("status", "stable"),
    #("license", "apache-2.0"),
  ])

  trove.delete_multi(db, keys: ["status", "license"])

  trove.put_and_delete_multi(db, puts: [#("new_key", "new_value")], deletes: [
    "version",
  ])

  io.println(
    "size = "
    <> int.to_string(trove.size(db))
    <> ", is_empty = "
    <> case trove.is_empty(db) {
      True -> "true"
      False -> "false"
    },
  )

  trove.close(db)
  io.println("done")
}
