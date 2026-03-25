//// OTP actor managing database state, concurrency, and persistence.

import gleam/bool
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import trove/codec
import trove/internal/btree
import trove/internal/btree/node
import trove/internal/compactor
import trove/internal/snapshot
import trove/internal/store
import trove/internal/store/file_ffi
import trove/internal/tx

pub type InternalOpenError {
  InternalDirectoryError(String)
  InternalStoreError(String)
  InternalLockError(String)
  InternalActorStartError
}

pub type FileSync {
  AutoSync
  ManualSync
}

pub type AutoCompact {
  AutoCompact(min_dirt: Int, min_dirt_factor: Float)
  NoAutoCompact
}

pub type TransactionOutcome(k, v) {
  CommitOutcome(tree: btree.Btree(k, v))
  CancelOutcome
}

pub type Message(k, v) {
  Get(key: k, reply: process.Subject(option.Option(v)))
  Put(key: k, value: v, reply: process.Subject(Nil))
  Delete(key: k, reply: process.Subject(Nil))
  HasKey(key: k, reply: process.Subject(Bool))
  PutMulti(entries: List(#(k, v)), reply: process.Subject(Nil))
  DeleteMulti(keys: List(k), reply: process.Subject(Nil))
  PutAndDeleteMulti(
    puts: List(#(k, v)),
    deletes: List(k),
    reply: process.Subject(Nil),
  )
  ExecuteTransaction(
    run: fn(tx.Tx(k, v)) -> TransactionOutcome(k, v),
    reply: process.Subject(Nil),
  )
  AcquireSnapshot(
    reply: process.Subject(Result(snapshot.Snapshot(k, v), String)),
  )
  Compact(reply: process.Subject(Result(Nil, String)))
  Size(reply: process.Subject(Int))
  DirtFactor(reply: process.Subject(Float))
  SyncFile(reply: process.Subject(Nil))
  SetAutoCompact(setting: AutoCompact, reply: process.Subject(Nil))
  Close(reply: process.Subject(Nil))
}

type DbState(k, v) {
  DbState(
    tree: btree.Btree(k, v),
    store: store.Store,
    key_codec: codec.Codec(k),
    value_codec: codec.Codec(v),
    key_compare: fn(k, k) -> order.Order,
    auto_file_sync: FileSync,
    auto_compact: AutoCompact,
    db_path: String,
    current_file_number: Int,
    call_timeout: Int,
  )
}

const init_lock_prefix = "lock:"

fn store_path(dir: String, file_number: Int) -> String {
  dir <> "/" <> int.to_string(file_number) <> ".trv"
}

fn parse_trv_number(name: String) -> Result(Int, Nil) {
  use <- bool.guard(when: !string.ends_with(name, ".trv"), return: Error(Nil))
  name |> string.drop_end(4) |> int.parse
}

fn compute_dirt_factor(tree: btree.Btree(k, v)) -> Float {
  int.to_float(btree.dirt(tree))
  /. int.to_float(1 + btree.size(tree) + btree.dirt(tree))
}

fn do_compact(state: DbState(k, v)) -> Result(DbState(k, v), btree.BtreeError) {
  let new_file_number = state.current_file_number + 1
  let new_path = store_path(state.db_path, new_file_number)
  let old_path = store_path(state.db_path, state.current_file_number)

  // Remove any leftover partial file from a previous failed compaction
  let _ = file_ffi.delete_file(path: new_path)

  use #(new_tree, new_store) <- result.try(compactor.compact(
    tree: state.tree,
    old_store: state.store,
    new_store_path: new_path,
    capacity: btree.capacity(state.tree),
    key_codec: state.key_codec,
    value_codec: state.value_codec,
    key_compare: state.key_compare,
  ))

  // Ensure the new file's directory entry is durable before removing the old file
  use _ <- result.try(
    file_ffi.dir_fsync(path: state.db_path)
    |> result.map_error(fn(s) { btree.StoreError(store.IoError(detail: s)) }),
  )
  case store.close(store: state.store) {
    Ok(Nil) -> Nil
    Error(reason) ->
      io.println_error(
        "[trove] failed to close old store during compaction: "
        <> store.error_to_string(reason),
      )
  }
  let _ = file_ffi.delete_file(path: old_path)
  cleanup_old_store_files(state.db_path, new_file_number)
  let _ = file_ffi.dir_fsync(path: state.db_path)
  Ok(
    DbState(
      ..state,
      tree: new_tree,
      store: new_store,
      current_file_number: new_file_number,
    ),
  )
}

fn cleanup_old_store_files(path: String, current_file_number: Int) -> Nil {
  case file_ffi.list_dir(path: path) {
    Error(_) -> Nil
    Ok(files) -> {
      let old_numbers =
        list.filter_map(files, fn(name) {
          case parse_trv_number(name) {
            Ok(n) if n < current_file_number -> Ok(n)
            _ -> Error(Nil)
          }
        })
      list.each(old_numbers, fn(n) {
        let old_path = store_path(path, n)
        case file_ffi.delete_file(path: old_path) {
          Ok(Nil) -> Nil
          Error(reason) ->
            io.println_error(
              "[trove] failed to delete old store "
              <> old_path
              <> ": "
              <> reason,
            )
        }
      })
    }
  }
}

fn maybe_auto_compact(state: DbState(k, v)) -> DbState(k, v) {
  case state.auto_compact {
    NoAutoCompact -> state
    AutoCompact(min_dirt, min_dirt_factor) -> {
      let should_compact =
        btree.dirt(state.tree) >= min_dirt
        && compute_dirt_factor(state.tree) >=. min_dirt_factor
      use <- bool.guard(when: !should_compact, return: state)
      case do_compact(state) {
        Ok(new_state) -> new_state
        Error(reason) -> {
          io.println_error(
            "[trove] auto-compact failed: " <> btree.error_to_string(reason),
          )
          state
        }
      }
    }
  }
}

fn fold_inserts(
  tree: btree.Btree(k, v),
  entries: List(#(k, v)),
  state: DbState(k, v),
) -> btree.Btree(k, v) {
  list.fold(entries, tree, fn(tree, entry) {
    let assert Ok(t) =
      btree.insert(
        tree: tree,
        store: state.store,
        key: entry.0,
        value: entry.1,
        key_codec: state.key_codec,
        value_codec: state.value_codec,
        compare: state.key_compare,
      )
    t
  })
}

fn fold_deletes(
  tree: btree.Btree(k, v),
  keys: List(k),
  state: DbState(k, v),
) -> btree.Btree(k, v) {
  list.fold(keys, tree, fn(tree, key) {
    let assert Ok(t) =
      btree.delete(
        tree: tree,
        store: state.store,
        key: key,
        key_codec: state.key_codec,
        compare: state.key_compare,
      )
    t
  })
}

fn commit_tree(
  state: DbState(k, v),
  new_tree: btree.Btree(k, v),
  reply: process.Subject(Nil),
) -> actor.Next(DbState(k, v), Message(k, v)) {
  case
    btree.root(tree: state.tree) == btree.root(tree: new_tree)
    && btree.size(tree: state.tree) == btree.size(tree: new_tree)
  {
    True -> {
      actor.send(reply, Nil)
      actor.continue(state)
    }
    False -> {
      let assert Ok(Nil) =
        write_header(state.store, new_tree, state.auto_file_sync)
      actor.send(reply, Nil)
      actor.continue(maybe_auto_compact(DbState(..state, tree: new_tree)))
    }
  }
}

fn handle_message(
  state: DbState(k, v),
  msg: Message(k, v),
) -> actor.Next(DbState(k, v), Message(k, v)) {
  case msg {
    Get(key, reply) -> {
      let assert Ok(result) =
        btree.lookup(
          tree: state.tree,
          store: state.store,
          key: key,
          key_codec: state.key_codec,
          value_codec: state.value_codec,
          compare: state.key_compare,
        )
      actor.send(reply, result)
      actor.continue(state)
    }

    Put(key, value, reply) -> {
      let assert Ok(new_tree) =
        btree.insert(
          tree: state.tree,
          store: state.store,
          key: key,
          value: value,
          key_codec: state.key_codec,
          value_codec: state.value_codec,
          compare: state.key_compare,
        )
      commit_tree(state, new_tree, reply)
    }

    Delete(key, reply) -> {
      let assert Ok(new_tree) =
        btree.delete(
          tree: state.tree,
          store: state.store,
          key: key,
          key_codec: state.key_codec,
          compare: state.key_compare,
        )
      commit_tree(state, new_tree, reply)
    }

    HasKey(key, reply) -> {
      let assert Ok(result) =
        btree.contains(
          tree: state.tree,
          store: state.store,
          key: key,
          key_codec: state.key_codec,
          compare: state.key_compare,
        )
      actor.send(reply, result)
      actor.continue(state)
    }

    PutMulti(entries, reply) -> {
      let new_tree = fold_inserts(state.tree, entries, state)
      commit_tree(state, new_tree, reply)
    }

    DeleteMulti(keys, reply) -> {
      let new_tree = fold_deletes(state.tree, keys, state)
      commit_tree(state, new_tree, reply)
    }

    PutAndDeleteMulti(puts, deletes, reply) -> {
      let new_tree =
        fold_inserts(state.tree, puts, state)
        |> fold_deletes(deletes, state)
      commit_tree(state, new_tree, reply)
    }

    ExecuteTransaction(run, reply) -> {
      let assert Ok(offset_before) = store.current_offset(store: state.store)
      let transaction =
        tx.new(
          tree: state.tree,
          store: state.store,
          key_codec: state.key_codec,
          value_codec: state.value_codec,
          key_compare: state.key_compare,
        )
      case run(transaction) {
        CommitOutcome(new_tree) -> commit_tree(state, new_tree, reply)
        CancelOutcome -> {
          let wasted = case store.current_offset(store: state.store) {
            Ok(offset_after) -> int.max(0, offset_after - offset_before)
            Error(_) -> 0
          }
          case wasted > 0 {
            True -> {
              let dirt_estimate = int.max(1, wasted / 100)
              let new_tree =
                btree.add_dirt(tree: state.tree, amount: dirt_estimate)
              let assert Ok(Nil) =
                write_header(state.store, new_tree, state.auto_file_sync)
              let new_state = DbState(..state, tree: new_tree)
              actor.send(reply, Nil)
              actor.continue(maybe_auto_compact(new_state))
            }
            False -> {
              actor.send(reply, Nil)
              actor.continue(state)
            }
          }
        }
      }
    }

    AcquireSnapshot(reply) -> {
      let snap_result =
        store.open_reader(store: state.store)
        |> result.map(fn(reader) {
          snapshot.new(
            tree: state.tree,
            store: reader,
            key_codec: state.key_codec,
            value_codec: state.value_codec,
            key_compare: state.key_compare,
          )
        })
        |> result.map_error(store.error_to_string)
      actor.send(reply, snap_result)
      actor.continue(state)
    }

    Compact(reply) -> {
      case do_compact(state) {
        Ok(compacted) -> {
          actor.send(reply, Ok(Nil))
          actor.continue(compacted)
        }
        Error(reason) -> {
          actor.send(reply, Error(btree.error_to_string(reason)))
          actor.continue(state)
        }
      }
    }

    Size(reply) -> {
      actor.send(reply, btree.size(tree: state.tree))
      actor.continue(state)
    }

    DirtFactor(reply) -> {
      actor.send(reply, compute_dirt_factor(state.tree))
      actor.continue(state)
    }

    SyncFile(reply) -> {
      let assert Ok(Nil) = store.sync(store: state.store)
      actor.send(reply, Nil)
      actor.continue(state)
    }

    SetAutoCompact(setting, reply) -> {
      actor.send(reply, Nil)
      actor.continue(DbState(..state, auto_compact: setting))
    }

    Close(reply) -> {
      let assert Ok(Nil) = store.close(store: state.store)
      let _ = file_ffi.unlock(path: state.db_path)
      actor.send(reply, Nil)
      actor.stop()
    }
  }
}

fn write_header(
  store: store.Store,
  tree: btree.Btree(k, v),
  auto_sync: FileSync,
) -> Result(Nil, store.StoreError) {
  let header =
    store.Header(
      root: btree.root(tree),
      size: btree.size(tree),
      dirt: btree.dirt(tree),
    )
  use _ <- result.try(store.put_header(store: store, header: header))
  case auto_sync {
    AutoSync -> store.sync(store: store)
    ManualSync -> Ok(Nil)
  }
}

fn recover_tree(
  store: store.Store,
) -> Result(btree.Btree(k, v), btree.BtreeError) {
  use is_blank <- result.try(
    store.blank(store: store) |> result.map_error(btree.StoreError),
  )
  case is_blank {
    True -> Ok(btree.new())
    False -> {
      use header <- result.try(
        store.get_latest_header(store: store)
        |> result.map_error(btree.StoreError),
      )
      btree.from_header(
        root: header.root,
        size: header.size,
        dirt: header.dirt,
        capacity: 32,
      )
    }
  }
}

pub fn open(
  path path: String,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
  auto_file_sync auto_file_sync: FileSync,
  auto_compact auto_compact: AutoCompact,
  call_timeout call_timeout: Int,
) -> Result(process.Subject(Message(k, v)), InternalOpenError) {
  use _ <- result.try(
    file_ffi.mkdir_p(path: path)
    |> result.map_error(InternalDirectoryError),
  )

  let start_result =
    actor.new_with_initialiser(call_timeout, fn(subject) {
      use _ <- result.try(
        file_ffi.try_lock(path: path)
        |> result.map_error(fn(reason) { init_lock_prefix <> reason }),
      )
      use file_number <- result.try(
        find_highest_store_number(path)
        |> result.map_error(fn(reason) {
          let _ = file_ffi.unlock(path: path)
          "failed to find store: " <> reason
        }),
      )
      let file_path = store_path(path, file_number)
      use store <- result.try(
        store.open(path: file_path)
        |> result.map_error(fn(e) {
          let _ = file_ffi.unlock(path: path)
          "failed to open store: " <> store.error_to_string(e)
        }),
      )
      use _ <- result.try(
        file_ffi.dir_fsync(path: path)
        |> result.map_error(fn(reason) {
          let _ = store.close(store: store)
          let _ = file_ffi.unlock(path: path)
          "failed to fsync directory: " <> reason
        }),
      )
      use tree <- result.try(
        recover_tree(store)
        |> result.map_error(fn(e) {
          let _ = store.close(store: store)
          let _ = file_ffi.unlock(path: path)
          "failed to recover tree: " <> btree.error_to_string(e)
        }),
      )
      let state =
        DbState(
          tree: tree,
          store: store,
          key_codec: key_codec,
          value_codec: value_codec,
          key_compare: key_compare,
          auto_file_sync: auto_file_sync,
          auto_compact: auto_compact,
          db_path: path,
          current_file_number: file_number,
          call_timeout: call_timeout,
        )
      actor.initialised(state)
      |> actor.returning(subject)
      |> Ok
    })
    |> actor.on_message(handle_message)
    |> actor.start

  case start_result {
    Ok(started) -> Ok(started.data)
    Error(actor.InitFailed(reason)) ->
      case string.starts_with(reason, init_lock_prefix) {
        True ->
          Error(
            InternalLockError(string.drop_start(
              reason,
              string.length(init_lock_prefix),
            )),
          )
        False -> Error(InternalStoreError(reason))
      }
    Error(actor.InitTimeout) | Error(actor.InitExited(_)) ->
      Error(InternalActorStartError)
  }
}

fn find_highest_store_number(path: String) -> Result(Int, String) {
  use files <- result.try(file_ffi.list_dir(path: path))
  let numbers =
    list.filter_map(files, parse_trv_number)
    |> list.sort(by: fn(a, b) { int.compare(b, a) })
  find_valid_store_number(path, numbers)
}

fn validate_store_file(
  file_path: String,
  has_fallback: Bool,
) -> Result(Bool, String) {
  case store.open(path: file_path) {
    Error(reason) -> Error(store.error_to_string(reason))
    Ok(store) -> {
      let valid = case store.blank(store: store) {
        Ok(True) -> !has_fallback
        Ok(False) ->
          case store.get_latest_header(store: store) {
            Ok(header) -> validate_root_readable(store, header)
            Error(_) -> False
          }
        Error(_) -> False
      }
      let _ = store.close(store: store)
      Ok(valid)
    }
  }
}

fn validate_root_readable(store: store.Store, header: store.Header) -> Bool {
  case
    btree.from_header(
      root: header.root,
      size: header.size,
      dirt: header.dirt,
      capacity: 32,
    )
  {
    Error(_) -> False
    Ok(_) ->
      case header.root {
        option.None -> True
        option.Some(offset) ->
          case store.get_node(store: store, location: offset) {
            Ok(data) ->
              node.validate_structure(data:) && validate_tree_deep(store, data)
            Error(_) -> False
          }
      }
  }
}

fn validate_tree_deep(store: store.Store, tree_data: BitArray) -> Bool {
  case node.extract_locations(data: tree_data) {
    Error(_) -> False
    Ok(locations) ->
      list.all(locations, fn(loc) {
        case store.get_node(store: store, location: loc) {
          Ok(child_data) ->
            case
              node.is_leaf(data: tree_data),
              node.node_kind(data: child_data)
            {
              // Leaf children must be data nodes
              True, Ok(node.DataKind) -> True
              // Branch children must be tree nodes
              False, Ok(node.TreeKind) ->
                node.validate_structure(data: child_data)
                && validate_tree_deep(store, child_data)
              _, _ -> False
            }
          Error(_) -> False
        }
      })
  }
}

fn find_valid_store_number(
  path: String,
  candidates: List(Int),
) -> Result(Int, String) {
  case candidates {
    [] -> Ok(0)
    [n] -> {
      let file_path = store_path(path, n)
      case validate_store_file(file_path, False) {
        Ok(True) -> Ok(n)
        Ok(False) ->
          Error(
            "store file "
            <> file_path
            <> " is corrupt and no valid fallback exists",
          )
        Error(reason) ->
          Error("unable to validate store " <> file_path <> ": " <> reason)
      }
    }
    [n, ..rest] -> {
      let file_path = store_path(path, n)
      case validate_store_file(file_path, True) {
        Ok(True) -> {
          // Keep one fallback (next-newest) for crash safety; delete older files
          let older = case rest {
            [_fallback, ..older] -> older
            [] -> []
          }
          list.each(older, fn(old_n) {
            let old_path = store_path(path, old_n)
            case file_ffi.delete_file(path: old_path) {
              Ok(Nil) -> Nil
              Error(reason) ->
                io.println_error(
                  "[trove] failed to delete old store "
                  <> old_path
                  <> ": "
                  <> reason,
                )
            }
          })
          Ok(n)
        }
        Ok(False) -> {
          let _ = file_ffi.delete_file(path: file_path)
          find_valid_store_number(path, rest)
        }
        Error(reason) -> {
          io.println_error(
            "[trove] unable to validate store "
            <> file_path
            <> ", trying fallback: "
            <> reason,
          )
          let _ = file_ffi.delete_file(path: file_path)
          find_valid_store_number(path, rest)
        }
      }
    }
  }
}

pub fn get(
  subject subject: process.Subject(Message(k, v)),
  key key: k,
  timeout timeout: Int,
) -> option.Option(v) {
  actor.call(subject, waiting: timeout, sending: Get(key, _))
}

pub fn put(
  subject subject: process.Subject(Message(k, v)),
  key key: k,
  value value: v,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: Put(key, value, _))
}

pub fn delete(
  subject subject: process.Subject(Message(k, v)),
  key key: k,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: Delete(key, _))
}

pub fn has_key(
  subject subject: process.Subject(Message(k, v)),
  key key: k,
  timeout timeout: Int,
) -> Bool {
  actor.call(subject, waiting: timeout, sending: HasKey(key, _))
}

pub fn put_multi(
  subject subject: process.Subject(Message(k, v)),
  entries entries: List(#(k, v)),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: PutMulti(entries, _))
}

pub fn delete_multi(
  subject subject: process.Subject(Message(k, v)),
  keys keys: List(k),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: DeleteMulti(keys, _))
}

pub fn put_and_delete_multi(
  subject subject: process.Subject(Message(k, v)),
  puts puts: List(#(k, v)),
  deletes deletes: List(k),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: PutAndDeleteMulti(
    puts,
    deletes,
    _,
  ))
}

pub fn transaction(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
  run run: fn(tx.Tx(k, v)) -> TransactionOutcome(k, v),
) -> Nil {
  actor.call(subject, waiting: timeout, sending: ExecuteTransaction(run, _))
}

pub fn acquire_snapshot(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Result(snapshot.Snapshot(k, v), String) {
  actor.call(subject, waiting: timeout, sending: AcquireSnapshot)
}

pub fn compact(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Result(Nil, String) {
  actor.call(subject, waiting: timeout, sending: Compact)
}

pub fn size(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Int {
  actor.call(subject, waiting: timeout, sending: Size)
}

pub fn dirt_factor(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Float {
  actor.call(subject, waiting: timeout, sending: DirtFactor)
}

pub fn file_sync(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: SyncFile)
}

pub fn set_auto_compact(
  subject subject: process.Subject(Message(k, v)),
  setting setting: AutoCompact,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: SetAutoCompact(setting, _))
}

pub fn close(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: Close)
}
