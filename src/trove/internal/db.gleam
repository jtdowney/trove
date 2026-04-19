//// OTP actor managing database state, concurrency, and persistence.

import gleam/bool
import gleam/dict
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

pub type OpenError {
  DirectoryError(String)
  StoreError(String)
  LockError(String)
  ActorStartError
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
  CommitOutcome(
    tree: btree.Btree(k, v),
    other_trees: dict.Dict(String, tx.KeyspaceEntry),
  )
  CancelOutcome
}

pub type Message(k, v) {
  Get(key: k, reply: process.Subject(Result(v, Nil)))
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
  RegisterKeyspace(
    name: String,
    byte_compare: fn(BitArray, BitArray) -> order.Order,
    reply: process.Subject(Nil),
  )
  ListKeyspaces(reply: process.Subject(List(String)))
  PutIn(
    name: String,
    key_bytes: BitArray,
    value_bytes: BitArray,
    reply: process.Subject(Nil),
  )
  GetIn(
    name: String,
    key_bytes: BitArray,
    reply: process.Subject(Result(BitArray, Nil)),
  )
  DeleteIn(name: String, key_bytes: BitArray, reply: process.Subject(Nil))
  HasKeyIn(name: String, key_bytes: BitArray, reply: process.Subject(Bool))
  SizeIn(name: String, reply: process.Subject(Int))
  PutAndDeleteMultiIn(
    name: String,
    puts: List(#(BitArray, BitArray)),
    deletes: List(BitArray),
    reply: process.Subject(Nil),
  )
  Close(reply: process.Subject(Nil))
}

pub type KeyspaceState {
  KeyspaceState(
    tree: btree.Btree(BitArray, BitArray),
    byte_compare: fn(BitArray, BitArray) -> order.Order,
  )
}

type State(k, v) {
  State(
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
    keyspaces: dict.Dict(String, KeyspaceState),
  )
}

fn unregistered_compare(name: String) -> fn(BitArray, BitArray) -> order.Order {
  fn(_, _) {
    panic as {
      "keyspace '"
      <> name
      <> "' used before trove.keyspace(...) was called in this session"
    }
  }
}

const init_lock_prefix = "lock:"

fn store_path(dir: String, file_number: Int) -> String {
  dir <> "/" <> int.to_string(file_number) <> ".trv"
}

fn parse_trv_number(name: String) -> Result(Int, Nil) {
  use <- bool.guard(when: !string.ends_with(name, ".trv"), return: Error(Nil))
  name |> string.drop_end(4) |> int.parse
}

fn aggregate_counts(state: State(k, v)) -> #(Int, Int) {
  let default_size = btree.size(state.tree)
  let default_dirt = btree.dirt(state.tree)
  dict.fold(state.keyspaces, #(default_size, default_dirt), fn(acc, _, entry) {
    #(acc.0 + btree.size(entry.tree), acc.1 + btree.dirt(entry.tree))
  })
}

fn compute_dirt_factor(state: State(k, v)) -> Float {
  let #(total_size, total_dirt) = aggregate_counts(state)
  int.to_float(total_dirt) /. int.to_float(1 + total_size + total_dirt)
}

fn do_compact(state: State(k, v)) -> Result(State(k, v), btree.Error) {
  let new_file_number = state.current_file_number + 1
  let new_path = store_path(state.db_path, new_file_number)
  let old_path = store_path(state.db_path, state.current_file_number)

  // Remove any leftover partial file from a previous failed compaction
  let _ = file_ffi.delete_file(path: new_path)

  let keyspaces_input =
    dict.to_list(state.keyspaces)
    |> list.map(fn(pair) {
      let #(name, entry) = pair
      compactor.KeyspaceCompaction(
        name: name,
        tree: entry.tree,
        byte_compare: entry.byte_compare,
      )
    })

  use #(new_tree, new_keyspaces, new_store) <- result.try(compactor.compact(
    tree: state.tree,
    keyspaces: keyspaces_input,
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
    State(
      ..state,
      tree: new_tree,
      store: new_store,
      current_file_number: new_file_number,
      keyspaces: merge_compacted_keyspaces(state.keyspaces, new_keyspaces),
    ),
  )
}

fn merge_compacted_keyspaces(
  old: dict.Dict(String, KeyspaceState),
  compacted: List(compactor.CompactedKeyspace),
) -> dict.Dict(String, KeyspaceState) {
  list.fold(compacted, dict.new(), fn(acc, ks) {
    let byte_compare = case dict.get(old, ks.name) {
      Ok(entry) -> entry.byte_compare
      Error(_) -> unregistered_compare(ks.name)
    }
    dict.insert(
      acc,
      ks.name,
      KeyspaceState(tree: ks.tree, byte_compare: byte_compare),
    )
  })
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

fn maybe_auto_compact(state: State(k, v)) -> State(k, v) {
  case state.auto_compact {
    NoAutoCompact -> state
    AutoCompact(min_dirt, min_dirt_factor) -> {
      let #(_, total_dirt) = aggregate_counts(state)
      let should_compact =
        total_dirt >= min_dirt && compute_dirt_factor(state) >=. min_dirt_factor
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
  state: State(k, v),
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
  state: State(k, v),
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
  state: State(k, v),
  new_tree: btree.Btree(k, v),
  reply: process.Subject(Nil),
) -> actor.Next(State(k, v), Message(k, v)) {
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
        write_header(
          state.store,
          new_tree,
          state.keyspaces,
          state.auto_file_sync,
        )
      actor.send(reply, Nil)
      actor.continue(maybe_auto_compact(State(..state, tree: new_tree)))
    }
  }
}

fn tx_keyspaces(
  keyspaces: dict.Dict(String, KeyspaceState),
) -> dict.Dict(String, tx.KeyspaceEntry) {
  dict.map_values(keyspaces, fn(_, entry) {
    tx.KeyspaceEntry(tree: entry.tree, byte_compare: entry.byte_compare)
  })
}

fn commit_transaction(
  state: State(k, v),
  new_tree: btree.Btree(k, v),
  new_other: dict.Dict(String, tx.KeyspaceEntry),
  reply: process.Subject(Nil),
) -> actor.Next(State(k, v), Message(k, v)) {
  let new_keyspaces = merge_tx_keyspaces(state.keyspaces, new_other)
  case
    btree.root(tree: state.tree) == btree.root(tree: new_tree)
    && btree.size(tree: state.tree) == btree.size(tree: new_tree)
    && keyspaces_equal(state.keyspaces, new_keyspaces)
  {
    True -> {
      actor.send(reply, Nil)
      actor.continue(state)
    }
    False -> {
      let assert Ok(Nil) =
        write_header(state.store, new_tree, new_keyspaces, state.auto_file_sync)
      actor.send(reply, Nil)
      actor.continue(maybe_auto_compact(
        State(..state, tree: new_tree, keyspaces: new_keyspaces),
      ))
    }
  }
}

fn merge_tx_keyspaces(
  old: dict.Dict(String, KeyspaceState),
  updated: dict.Dict(String, tx.KeyspaceEntry),
) -> dict.Dict(String, KeyspaceState) {
  dict.fold(updated, old, fn(acc, name, tx_entry) {
    dict.insert(
      acc,
      name,
      KeyspaceState(tree: tx_entry.tree, byte_compare: tx_entry.byte_compare),
    )
  })
}

fn keyspaces_equal(
  a: dict.Dict(String, KeyspaceState),
  b: dict.Dict(String, KeyspaceState),
) -> Bool {
  dict.size(a) == dict.size(b) && all_keyspaces_match(a, b)
}

fn all_keyspaces_match(
  a: dict.Dict(String, KeyspaceState),
  b: dict.Dict(String, KeyspaceState),
) -> Bool {
  dict.to_list(a)
  |> list.all(fn(pair) { keyspace_trees_match(pair.0, pair.1, b) })
}

fn keyspace_trees_match(
  name: String,
  entry: KeyspaceState,
  other: dict.Dict(String, KeyspaceState),
) -> Bool {
  case dict.get(other, name) {
    Ok(counterpart) ->
      btree.root(entry.tree) == btree.root(counterpart.tree)
      && btree.size(entry.tree) == btree.size(counterpart.tree)
    Error(_) -> False
  }
}

fn commit_keyspace_write(
  state: State(k, v),
  name: String,
  old_entry: KeyspaceState,
  new_entry: KeyspaceState,
  reply: process.Subject(Nil),
) -> actor.Next(State(k, v), Message(k, v)) {
  case
    btree.root(tree: old_entry.tree) == btree.root(tree: new_entry.tree)
    && btree.size(tree: old_entry.tree) == btree.size(tree: new_entry.tree)
  {
    True -> {
      actor.send(reply, Nil)
      actor.continue(state)
    }
    False -> {
      let new_keyspaces = dict.insert(state.keyspaces, name, new_entry)
      let assert Ok(Nil) =
        write_header(
          state.store,
          state.tree,
          new_keyspaces,
          state.auto_file_sync,
        )
      actor.send(reply, Nil)
      actor.continue(maybe_auto_compact(
        State(..state, keyspaces: new_keyspaces),
      ))
    }
  }
}

fn handle_message(
  state: State(k, v),
  msg: Message(k, v),
) -> actor.Next(State(k, v), Message(k, v)) {
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
      actor.send(reply, option.to_result(result, Nil))
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
          other_trees: tx_keyspaces(state.keyspaces),
        )
      case run(transaction) {
        CommitOutcome(new_tree, new_other) ->
          commit_transaction(state, new_tree, new_other, reply)
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
                write_header(
                  state.store,
                  new_tree,
                  state.keyspaces,
                  state.auto_file_sync,
                )
              let new_state = State(..state, tree: new_tree)
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
            keyspaces: keyspace_views(state.keyspaces),
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
      actor.send(reply, compute_dirt_factor(state))
      actor.continue(state)
    }

    SyncFile(reply) -> {
      let assert Ok(Nil) = store.sync(store: state.store)
      actor.send(reply, Nil)
      actor.continue(state)
    }

    SetAutoCompact(setting, reply) -> {
      actor.send(reply, Nil)
      actor.continue(State(..state, auto_compact: setting))
    }

    RegisterKeyspace(name, byte_compare, reply) -> {
      let existing =
        dict.get(state.keyspaces, name)
        |> result.unwrap(KeyspaceState(
          tree: btree.new(),
          byte_compare: byte_compare,
        ))
      let new_entry =
        KeyspaceState(tree: existing.tree, byte_compare: byte_compare)
      let new_keyspaces = dict.insert(state.keyspaces, name, new_entry)
      actor.send(reply, Nil)
      actor.continue(State(..state, keyspaces: new_keyspaces))
    }

    ListKeyspaces(reply) -> {
      let names = dict.keys(state.keyspaces) |> list.sort(string.compare)
      actor.send(reply, names)
      actor.continue(state)
    }

    PutIn(name, key_bytes, value_bytes, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      let assert Ok(new_tree) =
        btree.insert(
          tree: entry.tree,
          store: state.store,
          key: key_bytes,
          value: value_bytes,
          key_codec: codec.bit_array(),
          value_codec: codec.bit_array(),
          compare: entry.byte_compare,
        )
      let new_entry = KeyspaceState(..entry, tree: new_tree)
      commit_keyspace_write(state, name, entry, new_entry, reply)
    }

    GetIn(name, key_bytes, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      let assert Ok(result) =
        btree.lookup(
          tree: entry.tree,
          store: state.store,
          key: key_bytes,
          key_codec: codec.bit_array(),
          value_codec: codec.bit_array(),
          compare: entry.byte_compare,
        )
      actor.send(reply, option.to_result(result, Nil))
      actor.continue(state)
    }

    DeleteIn(name, key_bytes, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      let assert Ok(new_tree) =
        btree.delete(
          tree: entry.tree,
          store: state.store,
          key: key_bytes,
          key_codec: codec.bit_array(),
          compare: entry.byte_compare,
        )
      let new_entry = KeyspaceState(..entry, tree: new_tree)
      commit_keyspace_write(state, name, entry, new_entry, reply)
    }

    HasKeyIn(name, key_bytes, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      let assert Ok(result) =
        btree.contains(
          tree: entry.tree,
          store: state.store,
          key: key_bytes,
          key_codec: codec.bit_array(),
          compare: entry.byte_compare,
        )
      actor.send(reply, result)
      actor.continue(state)
    }

    SizeIn(name, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      actor.send(reply, btree.size(entry.tree))
      actor.continue(state)
    }

    PutAndDeleteMultiIn(name, puts, deletes, reply) -> {
      let assert Ok(entry) = dict.get(state.keyspaces, name)
      let tree_with_puts =
        list.fold(puts, entry.tree, fn(tree, put) {
          let assert Ok(t) =
            btree.insert(
              tree: tree,
              store: state.store,
              key: put.0,
              value: put.1,
              key_codec: codec.bit_array(),
              value_codec: codec.bit_array(),
              compare: entry.byte_compare,
            )
          t
        })
      let new_tree =
        list.fold(deletes, tree_with_puts, fn(tree, key) {
          let assert Ok(t) =
            btree.delete(
              tree: tree,
              store: state.store,
              key: key,
              key_codec: codec.bit_array(),
              compare: entry.byte_compare,
            )
          t
        })
      let new_entry = KeyspaceState(..entry, tree: new_tree)
      commit_keyspace_write(state, name, entry, new_entry, reply)
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
  keyspaces: dict.Dict(String, KeyspaceState),
  auto_sync: FileSync,
) -> Result(Nil, store.Error) {
  let header =
    store.Header(
      root: btree.root(tree),
      size: btree.size(tree),
      dirt: btree.dirt(tree),
      keyspaces: keyspace_headers(keyspaces),
    )
  use _ <- result.try(store.put_header(store: store, header: header))
  case auto_sync {
    AutoSync -> store.sync(store: store)
    ManualSync -> Ok(Nil)
  }
}

fn keyspace_views(
  keyspaces: dict.Dict(String, KeyspaceState),
) -> dict.Dict(String, snapshot.KeyspaceView) {
  dict.map_values(keyspaces, fn(_, entry) {
    snapshot.KeyspaceView(tree: entry.tree, byte_compare: entry.byte_compare)
  })
}

fn keyspace_headers(
  keyspaces: dict.Dict(String, KeyspaceState),
) -> List(store.KeyspaceHeader) {
  dict.to_list(keyspaces)
  |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
  |> list.map(fn(entry) {
    let #(name, state) = entry
    store.KeyspaceHeader(
      name: name,
      root: btree.root(state.tree),
      size: btree.size(state.tree),
      dirt: btree.dirt(state.tree),
    )
  })
}

fn recover_state(
  store: store.Store,
) -> Result(#(btree.Btree(k, v), dict.Dict(String, KeyspaceState)), btree.Error) {
  use is_blank <- result.try(
    store.blank(store: store) |> result.map_error(btree.StoreError),
  )
  case is_blank {
    True -> Ok(#(btree.new(), dict.new()))
    False -> {
      use header <- result.try(
        store.get_latest_header(store: store)
        |> result.map_error(btree.StoreError),
      )
      use tree <- result.try(btree.from_header(
        root: header.root,
        size: header.size,
        dirt: header.dirt,
        capacity: 32,
      ))
      use keyspaces <- result.try(recover_keyspaces(header.keyspaces))
      Ok(#(tree, keyspaces))
    }
  }
}

fn recover_keyspaces(
  entries: List(store.KeyspaceHeader),
) -> Result(dict.Dict(String, KeyspaceState), btree.Error) {
  list.try_fold(entries, dict.new(), fn(acc, entry) {
    use tree <- result.try(btree.from_header(
      root: entry.root,
      size: entry.size,
      dirt: entry.dirt,
      capacity: 32,
    ))
    Ok(dict.insert(
      acc,
      entry.name,
      KeyspaceState(tree: tree, byte_compare: unregistered_compare(entry.name)),
    ))
  })
}

pub fn open(
  path path: String,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
  auto_file_sync auto_file_sync: FileSync,
  auto_compact auto_compact: AutoCompact,
  call_timeout call_timeout: Int,
) -> Result(process.Subject(Message(k, v)), OpenError) {
  use _ <- result.try(
    file_ffi.mkdir_p(path: path)
    |> result.map_error(DirectoryError),
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
      use #(tree, keyspaces) <- result.try(
        recover_state(store)
        |> result.map_error(fn(e) {
          let _ = store.close(store: store)
          let _ = file_ffi.unlock(path: path)
          "failed to recover tree: " <> btree.error_to_string(e)
        }),
      )
      let state =
        State(
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
          keyspaces: keyspaces,
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
            LockError(string.drop_start(reason, string.length(init_lock_prefix))),
          )
        False -> Error(StoreError(reason))
      }
    Error(actor.InitTimeout) | Error(actor.InitExited(_)) ->
      Error(ActorStartError)
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
  validate_tree_header(store, header.root, header.size, header.dirt)
  && list.all(header.keyspaces, fn(ks) {
    validate_tree_header(store, ks.root, ks.size, ks.dirt)
  })
}

fn validate_tree_header(
  store: store.Store,
  root: option.Option(Int),
  size: Int,
  dirt: Int,
) -> Bool {
  case btree.from_header(root:, size:, dirt:, capacity: 32) {
    Error(_) -> False
    Ok(_) ->
      case root {
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
) -> Result(v, Nil) {
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

pub fn register_keyspace(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  byte_compare byte_compare: fn(BitArray, BitArray) -> order.Order,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: RegisterKeyspace(
    name,
    byte_compare,
    _,
  ))
}

pub fn list_keyspaces(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> List(String) {
  actor.call(subject, waiting: timeout, sending: ListKeyspaces)
}

pub fn put_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  key_bytes key_bytes: BitArray,
  value_bytes value_bytes: BitArray,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: PutIn(
    name,
    key_bytes,
    value_bytes,
    _,
  ))
}

pub fn get_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  key_bytes key_bytes: BitArray,
  timeout timeout: Int,
) -> Result(BitArray, Nil) {
  actor.call(subject, waiting: timeout, sending: GetIn(name, key_bytes, _))
}

pub fn delete_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  key_bytes key_bytes: BitArray,
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: DeleteIn(name, key_bytes, _))
}

pub fn has_key_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  key_bytes key_bytes: BitArray,
  timeout timeout: Int,
) -> Bool {
  actor.call(subject, waiting: timeout, sending: HasKeyIn(name, key_bytes, _))
}

pub fn size_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  timeout timeout: Int,
) -> Int {
  actor.call(subject, waiting: timeout, sending: SizeIn(name, _))
}

pub fn put_and_delete_multi_in(
  subject subject: process.Subject(Message(k, v)),
  name name: String,
  puts puts: List(#(BitArray, BitArray)),
  deletes deletes: List(BitArray),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: PutAndDeleteMultiIn(
    name,
    puts,
    deletes,
    _,
  ))
}

pub fn close(
  subject subject: process.Subject(Message(k, v)),
  timeout timeout: Int,
) -> Nil {
  actor.call(subject, waiting: timeout, sending: Close)
}
