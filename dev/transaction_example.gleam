import gleam/int
import gleam/io
import gleam/string
import trove
import trove/codec

pub fn main() -> Nil {
  let config =
    trove.Config(
      path: "/tmp/trove_transaction_example",
      key_codec: codec.string(),
      value_codec: codec.int(),
      key_compare: string.compare,
      auto_compact: trove.AutoCompact(min_dirt: 1000, min_dirt_factor: 0.25),
      auto_file_sync: trove.AutoSync,
      call_timeout: 5000,
    )

  let assert Ok(db) = trove.open(config)

  trove.put(db, key: "alice", value: 1000)
  trove.put(db, key: "bob", value: 500)

  io.println("=== Before transfer ===")
  print_balance(db, "alice")
  print_balance(db, "bob")

  let transfer_amount = 200
  let result =
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let assert Ok(alice_balance) = trove.tx_get(tx, key: "alice")
      let assert Ok(bob_balance) = trove.tx_get(tx, key: "bob")

      case alice_balance >= transfer_amount {
        True -> {
          let tx =
            trove.tx_put(
              tx,
              key: "alice",
              value: alice_balance - transfer_amount,
            )
          let tx =
            trove.tx_put(tx, key: "bob", value: bob_balance + transfer_amount)
          trove.Commit(tx:, result: "transferred")
        }
        False -> trove.Cancel(result: "insufficient funds")
      }
    })

  io.println("\ntransaction result: " <> result)

  io.println("\n=== After transfer ===")
  print_balance(db, "alice")
  print_balance(db, "bob")

  let result2 =
    trove.transaction(db, timeout: 5000, callback: fn(tx) {
      let assert Ok(alice_balance) = trove.tx_get(tx, key: "alice")
      case alice_balance >= 999_999 {
        True -> {
          let tx =
            trove.tx_put(tx, key: "alice", value: alice_balance - 999_999)
          trove.Commit(tx:, result: "transferred")
        }
        False -> trove.Cancel(result: "insufficient funds")
      }
    })

  io.println("\noverdraw result: " <> result2)

  io.println("\n=== Balances unchanged ===")
  print_balance(db, "alice")
  print_balance(db, "bob")

  trove.transaction(db, timeout: 5000, callback: fn(tx) {
    let exists = trove.tx_has_key(tx, key: "alice")
    io.println(
      "\ntx_has_key(alice) = "
      <> case exists {
        True -> "true"
        False -> "false"
      },
    )
    let missing = trove.tx_has_key(tx, key: "charlie")
    io.println(
      "tx_has_key(charlie) = "
      <> case missing {
        True -> "true"
        False -> "false"
      },
    )
    trove.Commit(tx:, result: Nil)
  })

  trove.close(db)
  io.println("\ndone")
}

fn print_balance(db: trove.Db(String, Int), name: String) -> Nil {
  let assert Ok(balance) = trove.get(db, key: name)
  io.println("  " <> name <> ": " <> int.to_string(balance))
}
