# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
