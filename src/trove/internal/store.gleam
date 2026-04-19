//// Append-only file store with block-aligned headers.

import gleam/bit_array
import gleam/bool
import gleam/list
import gleam/option
import gleam/result
import trove/internal/store/file_ffi

pub type StoreError {
  IoError(detail: String)
  ChecksumMismatch
  InvalidNodeHeader
  NodeExceedsBounds
  NoHeaderFound
}

pub fn error_to_string(error: StoreError) -> String {
  case error {
    IoError(detail) -> "I/O error: " <> detail
    ChecksumMismatch -> "checksum mismatch"
    InvalidNodeHeader -> "invalid node header"
    NodeExceedsBounds -> "node size exceeds file bounds"
    NoHeaderFound -> "no header found"
  }
}

const block_size = 1024

const data_marker = 0x00

const legacy_header_marker = 0x2A

const header_marker = 0x2B

const checksum_size = 16

const legacy_header_size = 42

const header_length_prefix_size = 4

const max_keyspaces = 65_535

fn map_io(result: Result(a, String)) -> Result(a, StoreError) {
  result.map_error(result, fn(s) { IoError(detail: s) })
}

/// An opaque handle to an open store file.
pub opaque type Store {
  Store(handle: file_ffi.FileHandle, path: String)
}

/// Per-keyspace metadata carried in a v2 header.
pub type KeyspaceHeader {
  KeyspaceHeader(name: String, root: option.Option(Int), size: Int, dirt: Int)
}

/// A persisted header recording the default root, entry count, dirt count,
/// and any named keyspaces.
pub type Header {
  Header(
    root: option.Option(Int),
    size: Int,
    dirt: Int,
    keyspaces: List(KeyspaceHeader),
  )
}

/// The total on-disk size of this header's record, in bytes. Used by crash
/// tests to compute post-write file offsets.
pub fn encoded_size(header header: Header) -> Int {
  let payload_size = payload_bytes(header) |> bit_array.byte_size
  1 + header_length_prefix_size + payload_size + checksum_size
}

/// Open a store file for reading and writing. Creates the file if needed.
pub fn open(path path: String) -> Result(Store, StoreError) {
  file_ffi.open(path: path)
  |> result.map(Store(_, path))
  |> map_io
}

/// Open a read-only handle to the same store file, suitable for snapshots.
pub fn open_reader(store store: Store) -> Result(Store, StoreError) {
  file_ffi.open_read(path: store.path)
  |> result.map(Store(_, store.path))
  |> map_io
}

/// Close the store, releasing the file handle.
pub fn close(store store: Store) -> Result(Nil, StoreError) {
  file_ffi.close(handle: store.handle)
  |> map_io
}

/// Append a serialized node to the store. Returns the byte offset where the
/// node was written.
pub fn put_node(
  store store: Store,
  data data: BitArray,
) -> Result(Int, StoreError) {
  let size = bit_array.byte_size(data)
  let checksum = file_ffi.hash(data: data)
  let payload = <<data_marker:8, size:32, data:bits, checksum:bits>>
  file_ffi.append(handle: store.handle, data: payload)
  |> map_io
}

/// Read a node from the store at the given byte offset.
pub fn get_node(
  store store: Store,
  location location: Int,
) -> Result(BitArray, StoreError) {
  use prefix <- result.try(
    file_ffi.pread(handle: store.handle, offset: location, length: 5)
    |> map_io,
  )
  case prefix {
    <<0x00, size:32>> -> {
      use current_size <- result.try(
        file_ffi.file_size(handle: store.handle) |> map_io,
      )
      let required = location + 5 + size + checksum_size
      use <- bool.guard(
        when: required > current_size,
        return: Error(NodeExceedsBounds),
      )
      use data <- result.try(
        file_ffi.pread(handle: store.handle, offset: location + 5, length: size)
        |> map_io,
      )
      use stored_checksum <- result.try(
        file_ffi.pread(
          handle: store.handle,
          offset: location + 5 + size,
          length: checksum_size,
        )
        |> map_io,
      )
      let computed = file_ffi.hash(data: data)
      case computed == stored_checksum {
        True -> Ok(data)
        False -> Error(ChecksumMismatch)
      }
    }
    _ -> Error(InvalidNodeHeader)
  }
}

/// Write a header to the store, block-aligned. Always emits the v2 format
/// (marker `0x2B`, length prefix, payload, checksum). Returns the byte offset
/// where the header was written.
///
/// Records larger than one 1024-byte block span multiple consecutive blocks.
/// Recovery scans block boundaries backwards looking for the marker; since
/// the record is block-aligned and the payload length is prefixed, multi-block
/// headers decode with the same scan as single-block ones.
pub fn put_header(
  store store: Store,
  header header: Header,
) -> Result(Int, StoreError) {
  let assert True = list.length(header.keyspaces) <= max_keyspaces
  use current_size <- result.try(
    file_ffi.file_size(handle: store.handle) |> map_io,
  )
  use _ <- result.try(pad_to_block_boundary(store.handle, current_size))
  let payload = payload_bytes(header)
  let payload_size = bit_array.byte_size(payload)
  let checksum = file_ffi.hash(data: payload)
  let record = <<
    header_marker:8,
    payload_size:int-big-size(32),
    payload:bits,
    checksum:bits,
  >>
  file_ffi.append(handle: store.handle, data: record)
  |> map_io
}

fn payload_bytes(header: Header) -> BitArray {
  let default = default_bytes(header.root, header.size, header.dirt)
  let count = list.length(header.keyspaces)
  let keyspaces =
    list.fold(header.keyspaces, <<>>, fn(acc, ks) {
      <<acc:bits, keyspace_bytes(ks):bits>>
    })
  <<default:bits, count:int-big-size(16), keyspaces:bits>>
}

fn default_bytes(root: option.Option(Int), size: Int, dirt: Int) -> BitArray {
  let #(has_root, root_val) = case root {
    option.Some(r) -> #(1, r)
    option.None -> #(0, 0)
  }
  <<
    has_root:8,
    root_val:int-big-size(64),
    size:int-big-size(64),
    dirt:int-big-size(64),
  >>
}

fn keyspace_bytes(ks: KeyspaceHeader) -> BitArray {
  let name_bytes = bit_array.from_string(ks.name)
  let name_len = bit_array.byte_size(name_bytes)
  let #(has_root, root_val) = case ks.root {
    option.Some(r) -> #(1, r)
    option.None -> #(0, 0)
  }
  <<
    name_len:int-big-size(16),
    name_bytes:bits,
    has_root:8,
    root_val:int-big-size(64),
    ks.size:int-big-size(64),
    ks.dirt:int-big-size(64),
  >>
}

fn pad_to_block_boundary(
  handle: file_ffi.FileHandle,
  current_size: Int,
) -> Result(Int, StoreError) {
  let remainder = current_size % block_size
  case remainder {
    0 -> Ok(0)
    _ -> {
      let padding_size = block_size - remainder
      let padding = <<0:size(padding_size)-unit(8)>>
      file_ffi.append(handle: handle, data: padding)
      |> map_io
    }
  }
}

/// Read the most recent header from the store by scanning backwards from the
/// last block. Supports both the v2 (`0x2B`) format and the legacy v1
/// (`0x2A`) format; legacy headers decode to a `Header` with an empty
/// keyspace list.
pub fn get_latest_header(store store: Store) -> Result(Header, StoreError) {
  use file_size <- result.try(
    file_ffi.file_size(handle: store.handle) |> map_io,
  )
  case file_size {
    0 -> Error(NoHeaderFound)
    _ -> {
      let last_block_offset = { file_size - 1 } / block_size * block_size
      scan_for_header(store, last_block_offset)
    }
  }
}

fn scan_for_header(store: Store, offset: Int) -> Result(Header, StoreError) {
  use <- bool.guard(when: offset < 0, return: Error(NoHeaderFound))
  case try_parse_header(store, offset) {
    Ok(header) -> Ok(header)
    Error(Nil) -> scan_for_header(store, offset - block_size)
  }
}

fn try_parse_header(store: Store, offset: Int) -> Result(Header, Nil) {
  use marker_bytes <- result.try(
    file_ffi.pread(handle: store.handle, offset: offset, length: 1)
    |> result.replace_error(Nil),
  )
  case marker_bytes {
    <<marker>> if marker == header_marker -> parse_v2_header(store, offset)
    <<marker>> if marker == legacy_header_marker ->
      parse_legacy_header(store, offset)
    _ -> Error(Nil)
  }
}

fn parse_v2_header(store: Store, offset: Int) -> Result(Header, Nil) {
  use len_bytes <- result.try(
    file_ffi.pread(
      handle: store.handle,
      offset: offset + 1,
      length: header_length_prefix_size,
    )
    |> result.replace_error(Nil),
  )
  use payload_size <- result.try(decode_payload_length(len_bytes))
  use file_size <- result.try(
    file_ffi.file_size(handle: store.handle) |> result.replace_error(Nil),
  )
  let record_end =
    offset + 1 + header_length_prefix_size + payload_size + checksum_size
  use <- bool.guard(
    when: payload_size < 0 || record_end > file_size,
    return: Error(Nil),
  )
  use payload <- result.try(
    file_ffi.pread(
      handle: store.handle,
      offset: offset + 1 + header_length_prefix_size,
      length: payload_size,
    )
    |> result.replace_error(Nil),
  )
  use stored_checksum <- result.try(
    file_ffi.pread(
      handle: store.handle,
      offset: offset + 1 + header_length_prefix_size + payload_size,
      length: checksum_size,
    )
    |> result.replace_error(Nil),
  )
  let computed = file_ffi.hash(data: payload)
  use <- bool.guard(when: computed != stored_checksum, return: Error(Nil))
  decode_payload(payload)
}

fn decode_payload(payload: BitArray) -> Result(Header, Nil) {
  case payload {
    <<
      default_fields:bytes-size(25),
      count:int-big-size(16),
      keyspaces_bits:bits,
    >> -> {
      use #(root, size, dirt) <- result.try(decode_default_fields(
        default_fields,
      ))
      use keyspaces <- result.try(decode_keyspaces(keyspaces_bits, count, []))
      Ok(Header(root: root, size: size, dirt: dirt, keyspaces: keyspaces))
    }
    _ -> Error(Nil)
  }
}

fn decode_payload_length(bytes: BitArray) -> Result(Int, Nil) {
  case bytes {
    <<n:int-big-size(32)>> -> Ok(n)
    _ -> Error(Nil)
  }
}

fn decode_default_fields(
  fields: BitArray,
) -> Result(#(option.Option(Int), Int, Int), Nil) {
  case fields {
    <<
      has_root:8,
      root_val:int-big-size(64),
      size:int-big-size(64),
      dirt:int-big-size(64),
    >> -> {
      use root <- result.try(decode_root_flag(has_root, root_val))
      Ok(#(root, size, dirt))
    }
    _ -> Error(Nil)
  }
}

fn decode_root_flag(
  has_root: Int,
  root_val: Int,
) -> Result(option.Option(Int), Nil) {
  case has_root {
    1 -> Ok(option.Some(root_val))
    0 -> Ok(option.None)
    _ -> Error(Nil)
  }
}

fn decode_keyspaces(
  bits: BitArray,
  remaining: Int,
  acc: List(KeyspaceHeader),
) -> Result(List(KeyspaceHeader), Nil) {
  case remaining {
    0 -> {
      use <- bool.guard(
        when: bit_array.byte_size(bits) != 0,
        return: Error(Nil),
      )
      Ok(list.reverse(acc))
    }
    _ -> {
      use #(entry, tail) <- result.try(decode_one_keyspace(bits))
      decode_keyspaces(tail, remaining - 1, [entry, ..acc])
    }
  }
}

fn decode_one_keyspace(
  bits: BitArray,
) -> Result(#(KeyspaceHeader, BitArray), Nil) {
  case bits {
    <<
      name_len:int-big-size(16),
      name_bytes:bytes-size(name_len),
      has_root:8,
      root_val:int-big-size(64),
      size:int-big-size(64),
      dirt:int-big-size(64),
      tail:bits,
    >> -> {
      use name <- result.try(
        bit_array.to_string(name_bytes) |> result.replace_error(Nil),
      )
      use root <- result.try(decode_root_flag(has_root, root_val))
      Ok(#(KeyspaceHeader(name: name, root: root, size: size, dirt: dirt), tail))
    }
    _ -> Error(Nil)
  }
}

fn parse_legacy_header(store: Store, offset: Int) -> Result(Header, Nil) {
  use raw <- result.try(
    file_ffi.pread(
      handle: store.handle,
      offset: offset,
      length: legacy_header_size,
    )
    |> result.replace_error(Nil),
  )
  case raw {
    <<_:8, fields:bytes-size(25), stored_checksum:bytes-size(16)>> -> {
      let computed = file_ffi.hash(data: fields)
      use <- bool.guard(when: computed != stored_checksum, return: Error(Nil))
      use #(root, size, dirt) <- result.try(decode_default_fields(fields))
      Ok(Header(root: root, size: size, dirt: dirt, keyspaces: []))
    }
    _ -> Error(Nil)
  }
}

/// Flush all buffered data to stable storage.
pub fn sync(store store: Store) -> Result(Nil, StoreError) {
  file_ffi.datasync(handle: store.handle)
  |> map_io
}

/// Return the current byte offset (file size) of the store.
pub fn current_offset(store store: Store) -> Result(Int, StoreError) {
  file_ffi.file_size(handle: store.handle)
  |> map_io
}

/// Return `True` if the store file is empty (zero bytes).
pub fn blank(store store: Store) -> Result(Bool, StoreError) {
  file_ffi.file_size(handle: store.handle)
  |> result.map(fn(size) { size == 0 })
  |> map_io
}
