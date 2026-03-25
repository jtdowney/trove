//// Erlang FFI bindings for raw file operations.

/// An opaque handle to an open file descriptor.
pub type FileHandle

/// Open a file for reading and appending. Creates the file if it does not exist.
@external(erlang, "trove_file_ffi", "open")
pub fn open(path path: String) -> Result(FileHandle, String)

/// Open a file in read-only mode.
@external(erlang, "trove_file_ffi", "open_read")
pub fn open_read(path path: String) -> Result(FileHandle, String)

/// Close a file handle.
@external(erlang, "trove_file_ffi", "close")
pub fn close(handle handle: FileHandle) -> Result(Nil, String)

/// Append data to the end of the file. Returns the byte offset where the
/// data was written.
@external(erlang, "trove_file_ffi", "append")
pub fn append(
  handle handle: FileHandle,
  data data: BitArray,
) -> Result(Int, String)

/// Read `length` bytes starting at `offset` from the file.
@external(erlang, "trove_file_ffi", "pread")
pub fn pread(
  handle handle: FileHandle,
  offset offset: Int,
  length length: Int,
) -> Result(BitArray, String)

/// Flush file data to stable storage.
@external(erlang, "trove_file_ffi", "datasync")
pub fn datasync(handle handle: FileHandle) -> Result(Nil, String)

/// Return the current size of the file in bytes.
@external(erlang, "trove_file_ffi", "file_size")
pub fn file_size(handle handle: FileHandle) -> Result(Int, String)

/// Recursively create a directory and all parent directories.
@external(erlang, "trove_file_ffi", "mkdir_p")
pub fn mkdir_p(path path: String) -> Result(Nil, String)

/// List the entries in a directory.
@external(erlang, "trove_file_ffi", "list_dir")
pub fn list_dir(path path: String) -> Result(List(String), String)

/// Delete a file.
@external(erlang, "trove_file_ffi", "delete_file")
pub fn delete_file(path path: String) -> Result(Nil, String)

/// Fsync a directory to make file creation/deletion durable.
/// On platforms that support opening directories (Linux), this fsyncs the
/// directory entry and propagates any errors (including `eacces`). On macOS,
/// opening a directory returns `eisdir` — that case is suppressed since APFS
/// provides metadata ordering guarantees by default.
@external(erlang, "trove_file_ffi", "dir_fsync")
pub fn dir_fsync(path path: String) -> Result(Nil, String)

/// Acquire a BEAM-level lock on a database path. Returns `Ok(Nil)` if the
/// lock was acquired, or `Error(reason)` if the path is already locked by a
/// live process. Stale locks (dead owner PIDs) are automatically cleaned up.
@external(erlang, "trove_file_ffi", "try_lock")
pub fn try_lock(path path: String) -> Result(Nil, String)

/// Release a previously acquired database path lock.
@external(erlang, "trove_file_ffi", "unlock")
pub fn unlock(path path: String) -> Result(Nil, String)

/// Compute a truncated blake2b hash (16 bytes) of the given data.
@external(erlang, "trove_file_ffi", "hash")
pub fn hash(data data: BitArray) -> BitArray
