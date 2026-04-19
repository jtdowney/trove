//// Compacts a B+ tree store by rewriting live entries into a fresh file.

import gleam/list
import gleam/option
import gleam/order
import gleam/result
import trove/codec
import trove/internal/btree
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

/// A named keyspace to rewrite during compaction. Keys and values are
/// streamed as raw `BitArray` (no user codec required) using the supplied
/// byte-level compare adapter.
pub type KeyspaceCompaction {
  KeyspaceCompaction(
    name: String,
    tree: btree.Btree(BitArray, BitArray),
    byte_compare: fn(BitArray, BitArray) -> order.Order,
  )
}

/// Result of compacting a single named keyspace.
pub type CompactedKeyspace {
  CompactedKeyspace(name: String, tree: btree.Btree(BitArray, BitArray))
}

/// Compact the default tree and every named keyspace into a fresh store at
/// `new_store_path`. Entries are processed lazily — only one leaf chunk is
/// held in memory at a time per tree. Writes one v2 header covering every
/// keyspace, so compaction is atomic across keyspaces.
pub fn compact(
  tree tree: btree.Btree(k, v),
  keyspaces keyspaces: List(KeyspaceCompaction),
  old_store old_store: store.Store,
  new_store_path new_store_path: String,
  capacity capacity: Int,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
) -> Result(
  #(btree.Btree(k, v), List(CompactedKeyspace), store.Store),
  btree.Error,
) {
  use new_store <- result.try(
    store.open(path: new_store_path) |> result.map_error(btree.StoreError),
  )

  let res =
    rewrite_all(
      tree,
      keyspaces,
      old_store,
      new_store,
      capacity,
      key_codec,
      value_codec,
      key_compare,
    )

  case res {
    Ok(val) -> Ok(val)
    Error(e) -> {
      let _ = store.close(store: new_store)
      Error(e)
    }
  }
}

fn rewrite_all(
  tree: btree.Btree(k, v),
  keyspaces: List(KeyspaceCompaction),
  old_store: store.Store,
  new_store: store.Store,
  capacity: Int,
  key_codec: codec.Codec(k),
  value_codec: codec.Codec(v),
  key_compare: fn(k, k) -> order.Order,
) -> Result(
  #(btree.Btree(k, v), List(CompactedKeyspace), store.Store),
  btree.Error,
) {
  let default_entries =
    btree_range.query(
      tree: tree,
      store: old_store,
      min: option.None,
      max: option.None,
      direction: range.Forward,
      key_codec: key_codec,
      value_codec: value_codec,
      compare: key_compare,
    )
  use new_tree <- result.try(btree.load_from_yielder(
    entries: default_entries,
    store: new_store,
    capacity: capacity,
    key_codec: key_codec,
    value_codec: value_codec,
    compare: key_compare,
  ))
  use new_keyspaces <- result.try(
    list.try_map(keyspaces, rewrite_keyspace(_, old_store, new_store, capacity)),
  )
  let header =
    store.Header(
      root: btree.root(new_tree),
      size: btree.size(new_tree),
      dirt: btree.dirt(new_tree),
      keyspaces: list.map(new_keyspaces, keyspace_to_header),
    )
  use _ <- result.try(
    store.put_header(store: new_store, header: header)
    |> result.map_error(btree.StoreError),
  )
  use _ <- result.try(
    store.sync(store: new_store) |> result.map_error(btree.StoreError),
  )
  Ok(#(new_tree, new_keyspaces, new_store))
}

fn rewrite_keyspace(
  keyspace: KeyspaceCompaction,
  old_store: store.Store,
  new_store: store.Store,
  capacity: Int,
) -> Result(CompactedKeyspace, btree.Error) {
  let bytes_codec = codec.bit_array()
  let entries =
    btree_range.query(
      tree: keyspace.tree,
      store: old_store,
      min: option.None,
      max: option.None,
      direction: range.Forward,
      key_codec: bytes_codec,
      value_codec: bytes_codec,
      compare: keyspace.byte_compare,
    )
  use new_tree <- result.try(btree.load_from_yielder(
    entries: entries,
    store: new_store,
    capacity: capacity,
    key_codec: bytes_codec,
    value_codec: bytes_codec,
    compare: keyspace.byte_compare,
  ))
  Ok(CompactedKeyspace(name: keyspace.name, tree: new_tree))
}

fn keyspace_to_header(ks: CompactedKeyspace) -> store.KeyspaceHeader {
  store.KeyspaceHeader(
    name: ks.name,
    root: btree.root(ks.tree),
    size: btree.size(ks.tree),
    dirt: btree.dirt(ks.tree),
  )
}
