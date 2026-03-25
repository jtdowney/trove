//// Compacts a B+ tree store by rewriting live entries into a fresh file.

import gleam/option
import gleam/order
import gleam/result
import trove/codec
import trove/internal/btree
import trove/internal/btree/range as btree_range
import trove/internal/store
import trove/range

/// Compact the tree by streaming live entries from `old_store` into a fresh
/// store at `new_store_path`. Entries are processed lazily — only one leaf
/// chunk is held in memory at a time. Returns the new tree and store.
pub fn compact(
  tree tree: btree.Btree(k, v),
  old_store old_store: store.Store,
  new_store_path new_store_path: String,
  capacity capacity: Int,
  key_codec key_codec: codec.Codec(k),
  value_codec value_codec: codec.Codec(v),
  key_compare key_compare: fn(k, k) -> order.Order,
) -> Result(#(btree.Btree(k, v), store.Store), btree.BtreeError) {
  let entries_yielder =
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

  use new_store <- result.try(
    store.open(path: new_store_path) |> result.map_error(btree.StoreError),
  )

  let res = {
    use new_tree <- result.try(btree.load_from_yielder(
      entries: entries_yielder,
      store: new_store,
      capacity: capacity,
      key_codec: key_codec,
      value_codec: value_codec,
      compare: key_compare,
    ))
    let header =
      store.Header(
        root: btree.root(new_tree),
        size: btree.size(new_tree),
        dirt: btree.dirt(new_tree),
      )
    use _ <- result.try(
      store.put_header(store: new_store, header: header)
      |> result.map_error(btree.StoreError),
    )
    use _ <- result.try(
      store.sync(store: new_store) |> result.map_error(btree.StoreError),
    )
    Ok(#(new_tree, new_store))
  }

  case res {
    Ok(val) -> Ok(val)
    Error(e) -> {
      let _ = store.close(store: new_store)
      Error(e)
    }
  }
}
