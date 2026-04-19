import exception
import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/string
import gleam/yielder
import qcheck
import simplifile
import trove
import trove/codec
import trove/internal/btree
import trove/internal/btree/diff
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

@external(erlang, "erlang", "unique_integer")
pub fn erlang_unique_integer() -> Int

pub fn temp_dir() -> String {
  let dir = "/tmp/trove_test_" <> int.to_string(erlang_unique_integer())
  let _ = simplifile.delete_all([dir])
  let assert Ok(Nil) = simplifile.create_directory_all(dir)
  dir
}

pub fn with_store(callback: fn(store.Store) -> Nil) -> Nil {
  let dir = temp_dir()
  let path = dir <> "/test.db"
  let assert Ok(s) = store.open(path: path)
  callback(s)
  let assert Ok(Nil) = store.close(store: s)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn int_string_config(path: String) -> trove.Config(Int, String) {
  trove.Config(
    path: path,
    key_codec: codec.int(),
    value_codec: codec.string(),
    key_compare: int.compare,
    auto_compact: trove.NoAutoCompact,
    auto_file_sync: trove.ManualSync,
    call_timeout: 5000,
  )
}

pub fn string_string_config(path: String) -> trove.Config(String, String) {
  trove.Config(
    path: path,
    key_codec: codec.string(),
    value_codec: codec.string(),
    key_compare: string.compare,
    auto_compact: trove.NoAutoCompact,
    auto_file_sync: trove.ManualSync,
    call_timeout: 5000,
  )
}

pub fn with_open_db(callback: fn(trove.Db(Int, String)) -> Nil) -> Nil {
  let dir = temp_dir()
  let assert Ok(db) = trove.open(int_string_config(dir))
  use <- exception.defer(fn() {
    trove.close(db)
    let _ = simplifile.delete_all([dir])
    Nil
  })
  callback(db)
}

pub fn with_string_db(callback: fn(trove.Db(String, String)) -> Nil) -> Nil {
  let dir = temp_dir()
  let assert Ok(db) = trove.open(string_string_config(dir))
  use <- exception.defer(fn() {
    trove.close(db)
    let _ = simplifile.delete_all([dir])
    Nil
  })
  callback(db)
}

pub fn insert(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  key key: Int,
  value value: String,
) -> Result(btree.Btree(Int, String), btree.Error) {
  btree.insert(
    tree: tree,
    store: store,
    key: key,
    value: value,
    key_codec: codec.int(),
    value_codec: codec.string(),
    compare: int.compare,
  )
}

pub fn insert_all(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  entries entries: List(#(Int, String)),
) -> btree.Btree(Int, String) {
  list.fold(entries, tree, fn(t, entry) {
    let #(k, v) = entry
    let assert Ok(new_tree) = insert(tree: t, store: store, key: k, value: v)
    new_tree
  })
}

pub fn lookup(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  key key: Int,
) -> Result(option.Option(String), btree.Error) {
  btree.lookup(
    tree: tree,
    store: store,
    key: key,
    key_codec: codec.int(),
    value_codec: codec.string(),
    compare: int.compare,
  )
}

pub fn delete(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  key key: Int,
) -> Result(btree.Btree(Int, String), btree.Error) {
  btree.delete(
    tree: tree,
    store: store,
    key: key,
    key_codec: codec.int(),
    compare: int.compare,
  )
}

pub fn delete_all(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  keys keys: List(Int),
) -> btree.Btree(Int, String) {
  list.fold(keys, tree, fn(t, k) {
    let assert Ok(new_tree) = delete(tree: t, store: store, key: k)
    new_tree
  })
}

pub fn mark_deleted(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  key key: Int,
) -> Result(btree.Btree(Int, String), btree.Error) {
  btree.mark_deleted(
    tree: tree,
    store: store,
    key: key,
    key_codec: codec.int(),
    compare: int.compare,
  )
}

pub fn query(
  tree tree: btree.Btree(Int, String),
  store store: store.Store,
  min min: option.Option(range.Bound(Int)),
  max max: option.Option(range.Bound(Int)),
  direction direction: range.Direction,
) -> yielder.Yielder(#(Int, String)) {
  btree_range.query(
    tree: tree,
    store: store,
    min: min,
    max: max,
    direction: direction,
    key_codec: codec.int(),
    value_codec: codec.string(),
    compare: int.compare,
  )
}

pub fn compute_diff(
  old old: btree.Btree(Int, String),
  new new: btree.Btree(Int, String),
  store store: store.Store,
) -> yielder.Yielder(#(Int, diff.Entry(String))) {
  diff.diff(
    old: old,
    new: new,
    store: store,
    key_codec: codec.int(),
    value_codec: codec.string(),
    compare: int.compare,
  )
}

pub fn property_config() -> qcheck.Config {
  qcheck.default_config() |> qcheck.with_test_count(100)
}

pub fn small_property_config() -> qcheck.Config {
  qcheck.default_config() |> qcheck.with_test_count(25)
}

pub fn int_list(from start: Int, to stop: Int) -> List(Int) {
  case start > stop {
    True -> []
    False -> int.range(stop, start - 1, [], fn(acc, i) { [i, ..acc] })
  }
}

pub fn make_entries(from: Int, to: Int) -> List(#(Int, String)) {
  int_list(from: from, to: to)
  |> list.map(fn(i) { #(i, "val" <> int.to_string(i)) })
}

pub type Op {
  OpPut(key: Int, value: String)
  OpDelete(key: Int)
}

pub fn op_generator() -> qcheck.Generator(Op) {
  let key_gen = qcheck.bounded_int(0, 99)
  let value_gen = qcheck.non_empty_string()

  qcheck.from_generators(
    qcheck.map2(key_gen, value_gen, fn(k, v) { OpPut(k, v) }),
    [qcheck.map(key_gen, fn(k) { OpDelete(k) })],
  )
}

pub fn apply_ops(
  db: trove.Db(Int, String),
  ops: List(Op),
) -> dict.Dict(Int, String) {
  list.fold(ops, dict.new(), fn(reference, op) {
    case op {
      OpPut(key, value) -> {
        trove.put(db, key: key, value: value)
        dict.insert(reference, key, value)
      }
      OpDelete(key) -> {
        trove.delete(db, key: key)
        dict.delete(reference, key)
      }
    }
  })
}

pub fn reference_sorted(
  reference: dict.Dict(Int, String),
) -> List(#(Int, String)) {
  dict.to_list(reference)
  |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
}

pub fn assert_range_matches_reference(
  db: trove.Db(Int, String),
  reference: dict.Dict(Int, String),
) -> Nil {
  let expected = reference_sorted(reference)

  let forward_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Forward,
    )
  assert forward_list == expected

  let reverse_list =
    trove.range(
      db,
      min: option.None,
      max: option.None,
      direction: range.Reverse,
    )
  assert reverse_list == list.reverse(expected)
}
