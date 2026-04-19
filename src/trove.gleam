//// An embedded, crash-safe key-value store for Gleam.
////
//// trove stores data in an append-only B+ tree on disk. Every write appends
//// new nodes and creates a new root — old data is never overwritten. This
//// gives you crash safety, zero-cost MVCC snapshots, and single-writer /
//// multiple-reader concurrency backed by an OTP actor.
////
//// ## Quick Start
////
//// ```gleam
//// import gleam/string
//// import trove
//// import trove/codec
////
//// let config = trove.Config(
////   path: "./my_db",
////   key_codec: codec.string(),
////   value_codec: codec.string(),
////   key_compare: string.compare,
////   auto_compact: trove.NoAutoCompact,
////   auto_file_sync: trove.AutoSync,
////   call_timeout: 5000,
//// )
////
//// let assert Ok(db) = trove.open(config)
//// trove.put(db, key: "language", value: "gleam")
//// let assert Ok("gleam") = trove.get(db, key: "language")
//// trove.close(db)
//// ```

import exception.{type Exception}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom
import gleam/erlang/process
import gleam/erlang/reference
import gleam/list
import gleam/option
import gleam/order
import gleam/result
import gleam/yielder
import trove/codec
import trove/internal/db
import trove/internal/snapshot
import trove/internal/tx
import trove/range

const reserved_default_keyspace_name = "__trove_default__"

@external(erlang, "erlang", "raise")
fn erlang_raise(
  class: atom.Atom,
  reason: Dynamic,
  stacktrace: List(Dynamic),
) -> a

fn reraise(ex: Exception) -> a {
  let #(class, reason) = case ex {
    exception.Errored(reason) -> #("error", reason)
    exception.Thrown(reason) -> #("throw", reason)
    exception.Exited(reason) -> #("exit", reason)
  }
  erlang_raise(atom.create(class), reason, [])
}

/// Errors that can occur when opening a database.
pub type OpenError {
  /// The database directory could not be created or accessed.
  DirectoryError(detail: String)
  /// The store file could not be opened or its header could not be recovered.
  StoreError(detail: String)
  /// The database path is already open by another actor on this node.
  LockError(detail: String)
  /// The OTP actor failed to start.
  ActorStartError
}

/// Controls automatic compaction behavior. When auto-compaction is enabled,
/// compaction triggers after a write if both `min_dirt` and `min_dirt_factor`
/// thresholds are exceeded simultaneously.
///
/// **Note:** Auto-compaction runs synchronously inside the database actor.
/// While compaction is in progress, all other operations (reads, writes,
/// snapshots) are queued and may time out on large databases. For
/// latency-sensitive workloads, prefer `NoAutoCompact` and call `compact`
/// manually from a separate process with an appropriate timeout.
pub type AutoCompact {
  /// Enable auto-compaction. `min_dirt` is the minimum number of mutation
  /// operations (inserts, updates, and deletes each add one to the dirt count)
  /// and `min_dirt_factor` is the minimum dirt ratio (0.0–1.0) — both must be
  /// exceeded for compaction to trigger.
  AutoCompact(min_dirt: Int, min_dirt_factor: Float)
  /// Disable auto-compaction. Compaction can still be triggered manually
  /// with `compact`.
  NoAutoCompact
}

/// Controls whether writes are automatically fsynced to disk.
pub type FileSync {
  /// Automatically fsync after every write for maximum durability.
  AutoSync
  /// Do not fsync automatically. Use `file_sync` to flush manually.
  ManualSync
}

/// Database configuration passed to `open`.
///
/// - `path` — directory where store files are kept (created if needed)
/// - `key_codec` / `value_codec` — how to serialize keys and values to bytes.
///   Must satisfy `decode(encode(v)) == Ok(v)` and be deterministic (same input
///   always produces the same bytes).
/// - `key_compare` — total order over keys: must be deterministic, antisymmetric,
///   and transitive. Must be consistent with `key_codec` — keys that compare
///   `Eq` must encode to identical bytes.
/// - `auto_compact` — controls automatic compaction after writes
/// - `auto_file_sync` — controls whether writes are automatically fsynced
/// - `call_timeout` — milliseconds to wait for actor responses (5000 is a good starting point)
pub type Config(k, v) {
  Config(
    path: String,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
    auto_compact: AutoCompact,
    auto_file_sync: FileSync,
    call_timeout: Int,
  )
}

/// An open database handle. Parameterized by key type `k` and value type `v`.
pub opaque type Db(k, v) {
  Db(subject: process.Subject(db.Message(k, v)), call_timeout: Int)
}

/// A handle to a named keyspace. Obtained via `trove.keyspace(...)`, which
/// registers the keyspace's codecs and comparator with the database so later
/// operations (`put_in`, `get_in`, compaction) can operate on it.
pub opaque type Keyspace(k, v) {
  Keyspace(
    name: String,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
  )
}

/// Obtain a typed handle to a named keyspace. First use of a name registers it
/// in this session; later uses update the codecs.
///
/// **Panics** if `name` collides with the reserved default-keyspace sentinel.
///
/// **Codec trust model.** Passing codecs that don't match those previously
/// used for the same keyspace is undefined behavior: reads will likely
/// produce garbage values or panics. Keep the
/// `(key_codec, value_codec, key_compare)` tuple stable across opens for a
/// given keyspace name. Matches the trust model of `Config.key_codec` and
/// `Config.value_codec`.
///
/// ```gleam
/// let users =
///   trove.keyspace(
///     db,
///     name: "users",
///     key_codec: codec.string(),
///     value_codec: codec.string(),
///     key_compare: string.compare,
///   )
/// ```
pub fn keyspace(
  db db: Db(_, _),
  name name: String,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
) -> Keyspace(k, v) {
  let assert True = name != reserved_default_keyspace_name
  db.register_keyspace(
    subject: db.subject,
    name: name,
    byte_compare: adapt_compare(key_codec, key_compare),
    timeout: db.call_timeout,
  )
  Keyspace(
    name: name,
    key_codec: key_codec,
    value_codec: value_codec,
    key_compare: key_compare,
  )
}

fn adapt_compare(
  key_codec: codec.Codec(k),
  key_compare: fn(k, k) -> order.Order,
) -> fn(BitArray, BitArray) -> order.Order {
  fn(a: BitArray, b: BitArray) {
    let assert Ok(decoded_a) = key_codec.decode(a)
    let assert Ok(decoded_b) = key_codec.decode(b)
    key_compare(decoded_a, decoded_b)
  }
}

/// List the names of every keyspace currently registered on this database.
///
/// Returns names in sorted order. Includes every keyspace that has been
/// opened with `trove.keyspace(...)` in this session, plus every keyspace
/// that was persisted in the store file (even if not yet registered in this
/// session). Reading or writing a persisted-but-unregistered keyspace
/// without first calling `trove.keyspace(...)` panics.
///
/// ```gleam
/// let names = trove.list_keyspaces(db)
/// ```
pub fn list_keyspaces(db db: Db(_, _)) -> List(String) {
  db.list_keyspaces(subject: db.subject, timeout: db.call_timeout)
}

/// Insert or update a key-value pair in a named keyspace.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.put_in(db, keyspace: users, key: "alice", value: "admin")
/// ```
pub fn put_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
  value value: v,
) -> Nil {
  db.put_in(
    subject: db.subject,
    name: keyspace.name,
    key_bytes: keyspace.key_codec.encode(key),
    value_bytes: keyspace.value_codec.encode(value),
    timeout: db.call_timeout,
  )
}

/// Look up a key in a named keyspace. Returns `Ok(value)` if found,
/// `Error(Nil)` if the key does not exist.
///
/// **Panics** on store I/O or decode errors (e.g. file corruption, codec
/// mismatch against on-disk bytes).
///
/// ```gleam
/// let assert Ok("admin") = trove.get_in(db, keyspace: users, key: "alice")
/// ```
pub fn get_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Result(v, Nil) {
  case
    db.get_in(
      subject: db.subject,
      name: keyspace.name,
      key_bytes: keyspace.key_codec.encode(key),
      timeout: db.call_timeout,
    )
  {
    option.Some(value_bytes) -> keyspace.value_codec.decode(value_bytes)
    option.None -> Error(Nil)
  }
}

/// Remove a key from a named keyspace. No error if the key does not exist.
///
/// **Panics** on store I/O errors.
///
/// ```gleam
/// trove.delete_in(db, keyspace: users, key: "alice")
/// ```
pub fn delete_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Nil {
  db.delete_in(
    subject: db.subject,
    name: keyspace.name,
    key_bytes: keyspace.key_codec.encode(key),
    timeout: db.call_timeout,
  )
}

/// Check whether a key exists in a named keyspace.
///
/// **Panics** on store I/O errors.
///
/// ```gleam
/// let assert True = trove.has_key_in(db, keyspace: users, key: "alice")
/// ```
pub fn has_key_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Bool {
  db.has_key_in(
    subject: db.subject,
    name: keyspace.name,
    key_bytes: keyspace.key_codec.encode(key),
    timeout: db.call_timeout,
  )
}

/// Returns the number of live entries in a named keyspace.
///
/// ```gleam
/// let n = trove.size_in(db, keyspace: users)
/// ```
pub fn size_in(db db: Db(_, _), keyspace keyspace: Keyspace(_, _)) -> Int {
  db.size_in(subject: db.subject, name: keyspace.name, timeout: db.call_timeout)
}

/// Atomically insert multiple key-value pairs into a named keyspace. A
/// single header write covers the entire batch.
///
/// **Panics** on store I/O errors.
///
/// ```gleam
/// trove.put_multi_in(
///   db,
///   keyspace: users,
///   entries: [#("alice", "admin"), #("bob", "member")],
/// )
/// ```
pub fn put_multi_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  entries entries: List(#(k, v)),
) -> Nil {
  put_and_delete_multi_in(db:, keyspace:, puts: entries, deletes: [])
}

/// Atomically delete multiple keys from a named keyspace.
///
/// **Panics** on store I/O errors.
///
/// ```gleam
/// trove.delete_multi_in(db, keyspace: users, keys: ["alice", "bob"])
/// ```
pub fn delete_multi_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  keys keys: List(k),
) -> Nil {
  put_and_delete_multi_in(db:, keyspace:, puts: [], deletes: keys)
}

/// Atomically insert and delete entries in a named keyspace under a single
/// header write. Puts are applied first, then deletes.
///
/// **Panics** on store I/O errors.
///
/// ```gleam
/// trove.put_and_delete_multi_in(
///   db,
///   keyspace: users,
///   puts: [#("bob", "admin")],
///   deletes: ["alice"],
/// )
/// ```
pub fn put_and_delete_multi_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  puts puts: List(#(k, v)),
  deletes deletes: List(k),
) -> Nil {
  let encoded_puts =
    list.map(puts, fn(pair) {
      #(keyspace.key_codec.encode(pair.0), keyspace.value_codec.encode(pair.1))
    })
  let encoded_deletes = list.map(deletes, keyspace.key_codec.encode)
  db.put_and_delete_multi_in(
    subject: db.subject,
    name: keyspace.name,
    puts: encoded_puts,
    deletes: encoded_deletes,
    timeout: db.call_timeout,
  )
}

/// Open a database at the configured path. Creates the directory if it does
/// not exist. If a store file already exists, recovers the tree from the
/// latest valid header.
///
/// ```gleam
/// let config = trove.Config(
///   path: "./my_db",
///   key_codec: codec.string(),
///   value_codec: codec.string(),
///   key_compare: string.compare,
///   auto_compact: trove.NoAutoCompact,
///   auto_file_sync: trove.AutoSync,
///   call_timeout: 5000,
/// )
/// let assert Ok(db) = trove.open(config)
/// ```
pub fn open(config: Config(k, v)) -> Result(Db(k, v), OpenError) {
  db.open(
    path: config.path,
    key_codec: config.key_codec,
    value_codec: config.value_codec,
    key_compare: config.key_compare,
    auto_file_sync: to_internal_file_sync(config.auto_file_sync),
    auto_compact: to_internal_auto_compact(config.auto_compact),
    call_timeout: config.call_timeout,
  )
  |> result.map(Db(_, config.call_timeout))
  |> result.map_error(map_open_error)
}

/// Close the database and release the file handle and path lock. Does not
/// fsync — if using `ManualSync`, call `file_sync` before closing to ensure
/// durability. The `Db` handle must not be used after calling this.
///
/// **Panics** if the store file handle cannot be closed.
///
/// ```gleam
/// trove.close(db)
/// ```
pub fn close(db: Db(k, v)) -> Nil {
  db.close(subject: db.subject, timeout: db.call_timeout)
}

/// Trigger a manual compaction. Rebuilds the store file keeping only live
/// entries, resetting the dirt factor to zero. Returns `Ok(Nil)` on success
/// or `Error(reason)` if compaction failed. On failure the database remains
/// functional with the original store file.
///
/// The timeout is separate from `call_timeout` because compaction can take
/// much longer than normal operations.
///
/// ```gleam
/// let assert Ok(Nil) = trove.compact(db, timeout: 60_000)
/// ```
pub fn compact(db: Db(k, v), timeout timeout: Int) -> Result(Nil, String) {
  db.compact(subject: db.subject, timeout: timeout)
}

/// Returns the number of live entries in the database.
///
/// ```gleam
/// let count = trove.size(db)
/// ```
pub fn size(db: Db(k, v)) -> Int {
  db.size(subject: db.subject, timeout: db.call_timeout)
}

/// Returns `True` if the database contains no entries.
///
/// ```gleam
/// let empty = trove.is_empty(db)
/// ```
pub fn is_empty(db: Db(k, v)) -> Bool {
  size(db) == 0
}

/// Returns the current dirt factor: a float between 0.0 and 1.0 that
/// approximates how much of the store file is occupied by superseded data.
/// Overwrites and deletes increment the dirt counter because they write new
/// nodes that make old ones unreachable. New inserts do not increment dirt
/// since they don't supersede existing data. The formula is
/// `dirt / (1 + size + dirt)` — the `+1` ensures the result is always
/// well-defined, even for an empty tree. The value approaches but never
/// reaches 1.0. Higher values mean more wasted space that compaction would
/// reclaim.
///
/// ```gleam
/// let df = trove.dirt_factor(db)
/// ```
pub fn dirt_factor(db: Db(k, v)) -> Float {
  db.dirt_factor(subject: db.subject, timeout: db.call_timeout)
}

/// Force an fsync of the store file to disk. Useful when `auto_file_sync`
/// is set to `ManualSync` and you want to control when data is flushed.
///
/// **Panics** if the fsync system call fails.
///
/// ```gleam
/// let config = trove.Config(..config, auto_file_sync: trove.ManualSync)
/// let assert Ok(db) = trove.open(config)
/// trove.put(db, key: "hello", value: "world")
/// trove.file_sync(db)
/// ```
pub fn file_sync(db: Db(k, v)) -> Nil {
  db.file_sync(subject: db.subject, timeout: db.call_timeout)
}

/// Change the auto-compaction setting at runtime.
///
/// ```gleam
/// trove.set_auto_compact(db, trove.AutoCompact(min_dirt: 100, min_dirt_factor: 0.25))
/// ```
pub fn set_auto_compact(db: Db(k, v), setting setting: AutoCompact) -> Nil {
  db.set_auto_compact(
    subject: db.subject,
    setting: to_internal_auto_compact(setting),
    timeout: db.call_timeout,
  )
}

/// Look up a key. Returns `Ok(value)` if found, `Error(Nil)` if the
/// key does not exist.
///
/// **Panics** on store I/O or decode errors (e.g. file corruption).
///
/// ```gleam
/// let assert Ok("world") = trove.get(db, key: "hello")
/// ```
pub fn get(db: Db(k, v), key key: k) -> Result(v, Nil) {
  db.get(subject: db.subject, key: key, timeout: db.call_timeout)
  |> option.to_result(Nil)
}

/// Insert or update a key-value pair.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.put(db, key: "hello", value: "world")
/// ```
pub fn put(db: Db(k, v), key key: k, value value: v) -> Nil {
  db.put(subject: db.subject, key: key, value: value, timeout: db.call_timeout)
}

/// Remove a key. No error if the key does not exist.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.delete(db, key: "hello")
/// ```
pub fn delete(db: Db(k, v), key key: k) -> Nil {
  db.delete(subject: db.subject, key: key, timeout: db.call_timeout)
}

/// Check whether a key exists in the database.
///
/// **Panics** on store I/O or decode errors (e.g. file corruption).
///
/// ```gleam
/// let assert True = trove.has_key(db, key: "hello")
/// ```
pub fn has_key(db: Db(k, v), key key: k) -> Bool {
  db.has_key(subject: db.subject, key: key, timeout: db.call_timeout)
}

/// Atomically insert multiple key-value pairs. A single header write covers
/// the entire batch.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.put_multi(db, entries: [#("a", "1"), #("b", "2")])
/// ```
pub fn put_multi(db: Db(k, v), entries entries: List(#(k, v))) -> Nil {
  db.put_multi(subject: db.subject, entries: entries, timeout: db.call_timeout)
}

/// Atomically delete multiple keys.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.delete_multi(db, keys: ["a", "b"])
/// ```
pub fn delete_multi(db: Db(k, v), keys keys: List(k)) -> Nil {
  db.delete_multi(subject: db.subject, keys: keys, timeout: db.call_timeout)
}

/// Atomically insert and delete entries in a single operation. Puts are
/// applied first, then deletes, all under a single header write.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.put_and_delete_multi(
///   db,
///   puts: [#("new_key", "value")],
///   deletes: ["old_key"],
/// )
/// ```
pub fn put_and_delete_multi(
  db: Db(k, v),
  puts puts: List(#(k, v)),
  deletes deletes: List(k),
) -> Nil {
  db.put_and_delete_multi(
    subject: db.subject,
    puts: puts,
    deletes: deletes,
    timeout: db.call_timeout,
  )
}

/// A transaction handle for reading and writing within a transaction.
pub type Tx(k, v) =
  tx.Tx(k, v)

/// The result of a transaction callback. Return `Commit` to apply the
/// transaction's writes, or `Cancel` to discard them.
pub type TransactionResult(k, v, a) {
  /// Apply the transaction's writes and return the result value.
  Commit(tx: Tx(k, v), result: a)
  /// Discard the transaction's writes and return the result value.
  Cancel(result: a)
}

/// A point-in-time snapshot handle for consistent reads.
pub type Snapshot(k, v) =
  snapshot.Snapshot(k, v)

/// Run an atomic transaction. The callback receives a `Tx` handle and must
/// return `Commit(tx:, result: value)` to apply writes or
/// `Cancel(result: value)` to discard. The transaction holds exclusive
/// write access for its duration.
///
/// The `timeout` parameter (in milliseconds) controls how long the caller
/// waits for the transaction to complete, including queue wait time and
/// callback execution. Choose a value appropriate for your workload —
/// queued operations or auto-compaction may delay the start, and a
/// long-running callback consumes the remaining budget.
///
/// **Important:** The callback runs inside the database actor. Do not call
/// any `trove` functions (such as `get`, `put`, `compact`, etc.) on the
/// same `Db` handle from within the callback — this will deadlock the actor
/// until the call timeout fires. Use the `Tx` handle (`tx_get`, `tx_put`,
/// `tx_delete`) for all reads and writes inside the transaction.
///
/// **Panics** if the `Commit` variant contains a stale or replaced `Tx`
/// handle (e.g. the original handle instead of the latest one returned by
/// `tx_put`/`tx_delete`).
///
/// **Non-escaping:** The `Tx` handle is only valid inside the callback.
/// Do not store it in a variable, send it to another process, or return it —
/// using a `Tx` after the callback returns will panic or produce undefined
/// behavior.
///
/// **Timeout semantics:** If the timeout fires while the callback is still
/// executing, the caller panics but the actor continues running the callback
/// to completion. This means writes may be durably committed even though the
/// caller observes a timeout failure. Choose a timeout that accommodates your
/// expected callback duration and any queued operations ahead of it.
///
/// ```gleam
/// let result = trove.transaction(db, timeout: 5000, callback: fn(tx) {
///   let tx = trove.tx_put(tx, key: "key", value: "value")
///   trove.Commit(tx:, result: "done")
/// })
/// ```
pub fn transaction(
  db: Db(k, v),
  timeout timeout: Int,
  callback callback: fn(Tx(k, v)) -> TransactionResult(k, v, a),
) -> a {
  let result_subject = process.new_subject()
  let token = reference.new()

  let run = fn(transaction) {
    let nonce_subject = process.new_subject()
    let transaction = tx.set_token(tx: transaction, token: token)
    let transaction =
      tx.set_nonce_tracker(tx: transaction, tracker: option.Some(nonce_subject))
    case exception.rescue(fn() { callback(transaction) }) {
      Ok(Commit(tx_inner, value)) -> {
        case
          exception.rescue(fn() {
            let assert True = tx.token(tx: tx_inner) == token
            let latest_nonce = drain_latest(nonce_subject)
            case latest_nonce {
              option.None -> Nil
              option.Some(expected) -> {
                let assert True = tx.nonce(tx: tx_inner) == expected
                Nil
              }
            }
          })
        {
          Ok(Nil) -> {
            process.send(result_subject, Ok(value))
            db.CommitOutcome(
              tx.get_tree(tx: tx_inner),
              tx.get_other_trees(tx: tx_inner),
            )
          }
          Error(ex) -> {
            process.send(result_subject, Error(ex))
            db.CancelOutcome
          }
        }
      }
      Ok(Cancel(value)) -> {
        process.send(result_subject, Ok(value))
        db.CancelOutcome
      }
      Error(ex) -> {
        process.send(result_subject, Error(ex))
        db.CancelOutcome
      }
    }
  }

  // The `run` closure sends to `result_subject` before the actor sends its
  // own reply, so by the time `db.transaction` (a blocking call) returns the
  // message is guaranteed to be in our mailbox.  A 0-ms receive is safe here.
  db.transaction(subject: db.subject, timeout: timeout, run: run)
  let assert Ok(result) = process.receive(result_subject, 0)
  case result {
    Ok(value) -> value
    Error(ex) -> reraise(ex)
  }
}

fn drain_latest(subject: process.Subject(a)) -> option.Option(a) {
  drain_latest_loop(subject, option.None)
}

fn drain_latest_loop(
  subject: process.Subject(a),
  acc: option.Option(a),
) -> option.Option(a) {
  case process.receive(subject, 0) {
    Ok(value) -> drain_latest_loop(subject, option.Some(value))
    Error(Nil) -> acc
  }
}

/// Read a key within a transaction. Sees writes made earlier in the same
/// transaction. Returns `Error(Nil)` if the key does not exist.
///
/// **Panics** on store I/O or decode errors (e.g. file corruption).
///
/// ```gleam
/// trove.transaction(db, timeout: 5000, callback: fn(tx) {
///   let assert Ok(current) = trove.tx_get(tx, key: "counter")
///   let tx = trove.tx_put(tx, key: "counter", value: current <> "!")
///   trove.Commit(tx:, result: Nil)
/// })
/// ```
pub fn tx_get(tx tx: Tx(k, v), key key: k) -> Result(v, Nil) {
  tx.get(tx: tx, key: key)
  |> option.to_result(Nil)
}

/// Write a key-value pair within a transaction. Returns the updated `Tx`.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.transaction(db, timeout: 5000, callback: fn(tx) {
///   let tx = trove.tx_put(tx, key: "greeting", value: "hello")
///   trove.Commit(tx:, result: Nil)
/// })
/// ```
pub fn tx_put(tx tx: Tx(k, v), key key: k, value value: v) -> Tx(k, v) {
  tx.put(tx: tx, key: key, value: value)
}

/// Delete a key within a transaction. Returns the updated `Tx`.
///
/// **Panics** on store I/O errors (e.g. disk full, file corruption).
///
/// ```gleam
/// trove.transaction(db, timeout: 5000, callback: fn(tx) {
///   let tx = trove.tx_delete(tx, key: "old_key")
///   trove.Commit(tx:, result: Nil)
/// })
/// ```
pub fn tx_delete(tx tx: Tx(k, v), key key: k) -> Tx(k, v) {
  tx.delete(tx: tx, key: key)
}

/// Check whether a key exists within a transaction. Sees writes made
/// earlier in the same transaction.
///
/// **Panics** on store I/O or decode errors (e.g. file corruption).
///
/// ```gleam
/// trove.transaction(db, timeout: 5000, callback: fn(tx) {
///   let exists = trove.tx_has_key(tx, key: "counter")
///   trove.Commit(tx:, result: exists)
/// })
/// ```
pub fn tx_has_key(tx tx: Tx(k, v), key key: k) -> Bool {
  tx_get(tx: tx, key: key) |> result.is_ok
}

/// Look up a key in a named keyspace within a transaction. Sees writes made
/// earlier in the same transaction.
pub fn tx_get_in(
  tx tx: Tx(k_default, v_default),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Result(v, Nil) {
  case
    tx.get_in(
      tx: tx,
      name: keyspace.name,
      key_bytes: keyspace.key_codec.encode(key),
    )
  {
    option.Some(value_bytes) -> keyspace.value_codec.decode(value_bytes)
    option.None -> Error(Nil)
  }
}

/// Insert or update a key-value pair in a named keyspace within a
/// transaction. Returns the updated `Tx`.
pub fn tx_put_in(
  tx tx: Tx(k_default, v_default),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
  value value: v,
) -> Tx(k_default, v_default) {
  tx.put_in(
    tx: tx,
    name: keyspace.name,
    key_bytes: keyspace.key_codec.encode(key),
    value_bytes: keyspace.value_codec.encode(value),
  )
}

/// Delete a key from a named keyspace within a transaction.
pub fn tx_delete_in(
  tx tx: Tx(k_default, v_default),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Tx(k_default, v_default) {
  tx.delete_in(
    tx: tx,
    name: keyspace.name,
    key_bytes: keyspace.key_codec.encode(key),
  )
}

/// Check whether a key exists in a named keyspace within a transaction.
pub fn tx_has_key_in(
  tx tx: Tx(k_default, v_default),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Bool {
  tx_get_in(tx: tx, keyspace: keyspace, key: key) |> result.is_ok
}

/// Run a callback with a point-in-time snapshot. The snapshot sees the state
/// of the database at the moment it was acquired — subsequent writes are
/// invisible to it.
///
/// **Non-escaping:** The `Snapshot` handle is only valid inside the callback.
/// Do not store it in a variable, send it to another process, or return it —
/// using a `Snapshot` after the callback returns will panic or produce
/// undefined behavior because the underlying file handle is closed on exit.
///
/// **Panics** if the snapshot file handle cannot be opened.
///
/// ```gleam
/// let result = trove.with_snapshot(db, fn(snap) {
///   trove.snapshot_get(snapshot: snap, key: "my_key")
/// })
/// // result: Result(String, Nil)
/// ```
pub fn with_snapshot(
  db: Db(k, v),
  callback callback: fn(Snapshot(k, v)) -> a,
) -> a {
  let assert Ok(snap) =
    db.acquire_snapshot(subject: db.subject, timeout: db.call_timeout)
  use <- exception.defer(fn() { snapshot.close(snap) })
  callback(snap)
}

/// Look up a key in a snapshot. Returns `Error(Nil)` if the key does
/// not exist.
///
/// **Panics** on store read or decode errors (e.g. file corruption).
///
/// ```gleam
/// trove.with_snapshot(db, fn(snap) {
///   let assert Ok(value) = trove.snapshot_get(snapshot: snap, key: "my_key")
///   value
/// })
/// ```
pub fn snapshot_get(
  snapshot snapshot: Snapshot(k, v),
  key key: k,
) -> Result(v, Nil) {
  snapshot.get(snapshot: snapshot, key: key)
  |> option.to_result(Nil)
}

/// Iterate over entries in a snapshot within optional key bounds.
/// Returns a lazy `Yielder` that streams entries from disk on demand,
/// reading only one leaf node at a time.
///
/// The yielder holds a reference to the snapshot's file handle, so it
/// must be consumed before the snapshot is closed. For large ranges,
/// prefer this over `range` to avoid loading all entries into memory.
///
/// **Panics** on store read or decode errors during iteration
/// (e.g. file corruption).
///
/// Use `range.Inclusive(key)` or `range.Exclusive(key)` for bounds,
/// or `option.None` for unbounded. Use `range.Forward` or `range.Reverse`
/// for direction.
///
/// ```gleam
/// import gleam/option.{None, Some}
/// import gleam/yielder
/// import trove/range
///
/// let entries = trove.with_snapshot(db, fn(snap) {
///   let y = trove.snapshot_range(
///     snapshot: snap,
///     min: Some(range.Inclusive("a")),
///     max: None,
///     direction: range.Forward,
///   )
///   yielder.to_list(y)
/// })
/// ```
pub fn snapshot_range(
  snapshot snapshot: Snapshot(k, v),
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  snapshot.range(snapshot: snapshot, min: min, max: max, direction: direction)
}

/// Iterate over entries in the database within optional key bounds.
/// Returns a `List` of key-value pairs.
///
/// For large result sets, use `with_snapshot` and `snapshot_range` instead
/// to stream entries lazily without loading them all at once.
///
/// **Panics** if the snapshot file handle cannot be opened, or on store
/// read/decode errors during iteration.
///
/// Use `range.Inclusive(key)` or `range.Exclusive(key)` for bounds,
/// or `option.None` for unbounded. Use `range.Forward` or `range.Reverse`
/// for direction.
///
/// ```gleam
/// import gleam/option.{Some}
/// import trove/range
///
/// let results =
///   trove.range(
///     db,
///     min: Some(range.Inclusive("a")),
///     max: Some(range.Exclusive("z")),
///     direction: range.Forward,
///   )
/// ```
pub fn range(
  db db: Db(k, v),
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
) -> List(#(k, v)) {
  let assert Ok(snap) =
    db.acquire_snapshot(subject: db.subject, timeout: db.call_timeout)
  use <- exception.defer(fn() { snapshot.close(snap) })
  snapshot.range(snapshot: snap, min: min, max: max, direction: direction)
  |> yielder.to_list()
}

/// Look up a key in a named keyspace within a snapshot.
///
/// ```gleam
/// trove.with_snapshot(db, fn(snap) {
///   trove.snapshot_get_in(snap, keyspace: users, key: "alice")
/// })
/// ```
pub fn snapshot_get_in(
  snapshot snapshot: Snapshot(_, _),
  keyspace keyspace: Keyspace(k, v),
  key key: k,
) -> Result(v, Nil) {
  case
    snapshot.get_in(
      snapshot: snapshot,
      name: keyspace.name,
      key_bytes: keyspace.key_codec.encode(key),
    )
  {
    option.Some(value_bytes) -> keyspace.value_codec.decode(value_bytes)
    option.None -> Error(Nil)
  }
}

/// Iterate over entries in a named keyspace within a snapshot. Returns a
/// lazy `Yielder` streaming entries from disk.
///
/// The yielder holds a reference to the snapshot's file handle; consume it
/// before the snapshot closes.
///
/// ```gleam
/// trove.with_snapshot(db, fn(snap) {
///   trove.snapshot_range_in(
///     snap,
///     keyspace: users,
///     min: Some(range.Inclusive("a")),
///     max: None,
///     direction: range.Forward,
///   )
///   |> yielder.to_list
/// })
/// ```
pub fn snapshot_range_in(
  snapshot snapshot: Snapshot(_, _),
  keyspace keyspace: Keyspace(k, v),
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
) -> yielder.Yielder(#(k, v)) {
  snapshot.range_in(
    snapshot: snapshot,
    name: keyspace.name,
    min: encode_bound(min, keyspace.key_codec),
    max: encode_bound(max, keyspace.key_codec),
    direction: direction,
  )
  |> yielder.map(fn(pair) {
    let assert Ok(k) = keyspace.key_codec.decode(pair.0)
    let assert Ok(v) = keyspace.value_codec.decode(pair.1)
    #(k, v)
  })
}

fn encode_bound(
  bound: option.Option(range.Bound(k)),
  key_codec: codec.Codec(k),
) -> option.Option(range.Bound(BitArray)) {
  option.map(bound, fn(b) {
    case b {
      range.Inclusive(v) -> range.Inclusive(key_codec.encode(v))
      range.Exclusive(v) -> range.Exclusive(key_codec.encode(v))
    }
  })
}

/// Iterate over entries in a named keyspace within optional key bounds.
/// Returns a `List` of key-value pairs. For large result sets, use
/// `with_snapshot` and `snapshot_range_in` instead.
///
/// ```gleam
/// let results = trove.range_in(
///   db,
///   keyspace: users,
///   min: Some(range.Inclusive("a")),
///   max: Some(range.Exclusive("z")),
///   direction: range.Forward,
/// )
/// ```
pub fn range_in(
  db db: Db(_, _),
  keyspace keyspace: Keyspace(k, v),
  min min: option.Option(range.Bound(k)),
  max max: option.Option(range.Bound(k)),
  direction direction: range.Direction,
) -> List(#(k, v)) {
  let assert Ok(snap) =
    db.acquire_snapshot(subject: db.subject, timeout: db.call_timeout)
  use <- exception.defer(fn() { snapshot.close(snap) })
  snapshot_range_in(
    snapshot: snap,
    keyspace: keyspace,
    min: min,
    max: max,
    direction: direction,
  )
  |> yielder.to_list
}

fn map_open_error(error: db.InternalOpenError) -> OpenError {
  case error {
    db.InternalDirectoryError(reason) -> DirectoryError(reason)
    db.InternalStoreError(reason) -> StoreError(reason)
    db.InternalLockError(reason) -> LockError(reason)
    db.InternalActorStartError -> ActorStartError
  }
}

fn to_internal_auto_compact(setting: AutoCompact) -> db.AutoCompact {
  case setting {
    AutoCompact(min_dirt:, min_dirt_factor:) ->
      db.AutoCompact(min_dirt:, min_dirt_factor:)
    NoAutoCompact -> db.NoAutoCompact
  }
}

fn to_internal_file_sync(setting: FileSync) -> db.FileSync {
  case setting {
    AutoSync -> db.AutoSync
    ManualSync -> db.ManualSync
  }
}
