//// Append-only file store with block-aligned headers.

import gleam/bit_array
import gleam/bool
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

const header_marker = 0x2A

const checksum_size = 16

fn map_io(result: Result(a, String)) -> Result(a, StoreError) {
  result.map_error(result, fn(s) { IoError(detail: s) })
}

/// An opaque handle to an open store file.
pub opaque type Store {
  Store(handle: file_ffi.FileHandle, path: String)
}

/// A persisted header recording the root offset, entry count, and dirt count.
pub type Header {
  Header(root: option.Option(Int), size: Int, dirt: Int)
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

/// Write a header to the store, block-aligned. Returns the byte offset
/// where the header was written.
pub fn put_header(
  store store: Store,
  header header: Header,
) -> Result(Int, StoreError) {
  use current_size <- result.try(
    file_ffi.file_size(handle: store.handle) |> map_io,
  )
  use _ <- result.try(pad_to_block_boundary(store.handle, current_size))
  let #(has_root, root_val) = case header.root {
    option.Some(r) -> #(1, r)
    option.None -> #(0, 0)
  }
  let fields = <<
    has_root:8,
    root_val:int-big-size(64),
    header.size:int-big-size(64),
    header.dirt:int-big-size(64),
  >>
  let checksum = file_ffi.hash(data: fields)
  let payload = <<header_marker:8, fields:bits, checksum:bits>>
  file_ffi.append(handle: store.handle, data: payload)
  |> map_io
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
/// last block.
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

/// Size of a serialized header in bytes (1 marker + 25 fields + 16 checksum).
pub const header_size = 42

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
  use <- bool.guard(when: marker_bytes != <<header_marker>>, return: Error(Nil))
  use raw <- result.try(
    file_ffi.pread(handle: store.handle, offset: offset, length: header_size)
    |> result.replace_error(Nil),
  )
  case raw {
    <<_:8, fields:bytes-size(25), stored_checksum:bytes-size(16)>> -> {
      let computed = file_ffi.hash(data: fields)
      use <- bool.guard(when: computed != stored_checksum, return: Error(Nil))
      case fields {
        <<
          has_root:8,
          root_val:int-big-size(64),
          size:int-big-size(64),
          dirt:int-big-size(64),
        >> ->
          case has_root {
            1 -> Ok(Header(root: option.Some(root_val), size: size, dirt: dirt))
            0 -> Ok(Header(root: option.None, size: size, dirt: dirt))
            _ -> Error(Nil)
          }
        _ -> Error(Nil)
      }
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
