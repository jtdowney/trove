# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `trove.Keyspace(k, v)` handle type, obtained via `trove.keyspace(db, name:, key_codec:, value_codec:, key_compare:)`. Each keyspace is a typed, named subspace of the database with its own codecs and comparator.
- Non-transactional keyspace operations: `put_in`, `get_in`, `delete_in`, `has_key_in`, `put_multi_in`, `delete_multi_in`, `put_and_delete_multi_in`, `size_in`, `range_in`, `list_keyspaces`.
- Snapshot operations on keyspaces: `snapshot_get_in`, `snapshot_range_in`.
- Transactional keyspace operations: `tx_get_in`, `tx_put_in`, `tx_delete_in`, `tx_has_key_in`. A single transaction can write across the default keyspace and any named keyspaces atomically.
- Auto-compaction and `dirt_factor` now aggregate across the default tree and every named keyspace.
- Compaction atomically rewrites the default tree plus every named keyspace under a single header write.

### Changed

- On-disk header format bumped from marker `0x2A` to `0x2B` with a length-prefixed payload carrying a flat list of named keyspace entries. Databases written by 1.0.0 continue to open transparently; the first write after open emits the v2 format.
- The `store.header_size` constant is replaced by `store.encoded_size(header)` since the v2 header is variable-length.
- v2 headers span multiple 1024-byte blocks when needed. The payload length is self-describing, and the `0x2B` marker lands at a block boundary so the backward scan still finds it in one step. No practical cap on the number of keyspaces beyond the 65,535 count field.

## [1.0.0] - 2026-03-24

### Added

- Append-only copy-on-write B+ tree storage engine
- Crash-safe file format with checksummed nodes and block-aligned headers
- Type-safe codecs for keys and values via `Codec(a)`
- OTP actor with single-writer / multiple-reader concurrency
- Point lookups, range queries, and fold operations
- Read-only snapshots pinned to a tree root
- Transactions with commit and cancel support
- Compaction with configurable auto-compact thresholds

[1.0.0]: https://github.com/jtdowney/trove/releases/tag/v1.0.0
