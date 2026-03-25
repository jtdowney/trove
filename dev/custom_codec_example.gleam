import gleam/bit_array
import gleam/int
import gleam/io
import gleam/option
import gleam/string
import trove
import trove/codec
import trove/range

pub type User {
  User(name: String, age: Int)
}

fn user_codec() -> codec.Codec(User) {
  codec.Codec(
    encode: fn(user: User) {
      let name_bytes = bit_array.from_string(user.name)
      let name_size = bit_array.byte_size(name_bytes)
      <<name_size:32, name_bytes:bits, user.age:32>>
    },
    decode: fn(bits) {
      case bits {
        <<name_size:32, name_bytes:bytes-size(name_size), age:32>> ->
          case bit_array.to_string(name_bytes) {
            Ok(name) -> Ok(User(name, age))
            Error(_) -> Error(Nil)
          }
        _ -> Error(Nil)
      }
    },
  )
}

pub fn main() {
  let config =
    trove.Config(
      path: "/tmp/trove_custom_codec_example",
      key_codec: codec.string(),
      value_codec: user_codec(),
      key_compare: string.compare,
      auto_compact: trove.NoAutoCompact,
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "alice", value: User("Alice", 30))
  trove.put(db, key: "bob", value: User("Bob", 25))
  trove.put(db, key: "charlie", value: User("Charlie", 35))

  io.println("=== Users stored with custom codec ===")

  trove.range(db, min: option.None, max: option.None, direction: range.Forward)
  |> print_users()

  trove.close(db)
  let assert Ok(db2) = trove.open(config)

  io.println("\n=== Users after reopen ===")
  trove.range(db2, min: option.None, max: option.None, direction: range.Forward)
  |> print_users()

  let assert Ok(Nil) = trove.compact(db2, timeout: 60_000)

  io.println("\n=== Users after compaction ===")
  trove.range(db2, min: option.None, max: option.None, direction: range.Forward)
  |> print_users()

  trove.close(db2)
  io.println("\ndone")
}

fn print_users(entries: List(#(String, User))) -> Nil {
  case entries {
    [] -> Nil
    [#(key, User(name, age)), ..rest] -> {
      io.println(
        "  " <> key <> " -> " <> name <> " (age " <> int.to_string(age) <> ")",
      )
      print_users(rest)
    }
  }
}
