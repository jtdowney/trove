import gleam/bit_array
import gleam/erlang/process
import gleam/list
import gleam/string
import qcheck
import simplifile
import trove/internal/store/file_ffi
import trove/test_helpers

fn with_temp_file(callback: fn(String) -> Nil) -> Nil {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/test.db"
  callback(path)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn open_creates_file_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)
  let assert Ok(Nil) = file_ffi.close(handle: handle)
  let assert Ok(True) = simplifile.is_file(path)
  Nil
}

pub fn open_nonexistent_directory_returns_error_test() {
  let assert Error(_) = file_ffi.open(path: "/no/such/dir/test.db")
  Nil
}

pub fn close_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)
  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn append_returns_position_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)

  let chunk1 = <<"hello":utf8>>
  let assert Ok(0) = file_ffi.append(handle: handle, data: chunk1)

  let chunk2 = <<" world":utf8>>
  let assert Ok(5) = file_ffi.append(handle: handle, data: chunk2)

  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn pread_reads_back_written_data_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)

  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"hello":utf8>>)
  let assert Ok(5) = file_ffi.append(handle: handle, data: <<" world":utf8>>)

  let assert Ok(data) = file_ffi.pread(handle: handle, offset: 0, length: 5)
  assert data == <<"hello":utf8>>

  let assert Ok(data2) = file_ffi.pread(handle: handle, offset: 5, length: 6)
  assert data2 == <<" world":utf8>>

  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn datasync_succeeds_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)
  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"data":utf8>>)
  let assert Ok(Nil) = file_ffi.datasync(handle: handle)
  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn file_size_tracks_appends_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)

  let assert Ok(0) = file_ffi.file_size(handle: handle)

  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"abc":utf8>>)
  let assert Ok(3) = file_ffi.file_size(handle: handle)

  let assert Ok(3) = file_ffi.append(handle: handle, data: <<"defgh":utf8>>)
  let assert Ok(8) = file_ffi.file_size(handle: handle)

  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn open_read_can_read_data_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)
  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"hello":utf8>>)
  let assert Ok(Nil) = file_ffi.close(handle: handle)

  let assert Ok(reader) = file_ffi.open_read(path: path)
  let assert Ok(data) = file_ffi.pread(handle: reader, offset: 0, length: 5)
  assert data == <<"hello":utf8>>
  let assert Ok(Nil) = file_ffi.close(handle: reader)
  Nil
}

pub fn pread_past_eof_returns_error_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)
  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"hi":utf8>>)
  let assert Error(_) = file_ffi.pread(handle: handle, offset: 100, length: 10)
  let assert Ok(Nil) = file_ffi.close(handle: handle)
  Nil
}

pub fn unlock_from_non_owner_does_not_release_lock_test() {
  let dir = test_helpers.temp_dir()
  let assert Ok(Nil) = file_ffi.try_lock(path: dir)

  // Spawn a child process that tries to unlock
  let subject = process.new_subject()
  process.spawn(fn() {
    let _ = file_ffi.unlock(path: dir)
    process.send(subject, Nil)
  })
  let assert Ok(Nil) = process.receive(subject, 1000)

  // Lock should still be held by us
  let assert Error(_) = file_ffi.try_lock(path: dir)

  // Clean up: unlock from the actual owner
  let assert Ok(Nil) = file_ffi.unlock(path: dir)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn dir_fsync_valid_directory_returns_ok_test() {
  let dir = test_helpers.temp_dir()
  let assert Ok(Nil) = file_ffi.dir_fsync(path: dir)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn dir_fsync_nonexistent_path_returns_error_test() {
  let assert Error(_) = file_ffi.dir_fsync(path: "/no/such/dir/does_not_exist")
  Nil
}

pub fn append_pread_roundtrip_property_test() {
  let length_gen = qcheck.bounded_int(from: 1, to: 20)
  let chunk_gen =
    qcheck.generic_byte_aligned_bit_array(
      values_from: qcheck.bounded_int(from: 0, to: 255),
      byte_size_from: qcheck.bounded_int(from: 1, to: 100),
    )
  let chunks_gen =
    qcheck.generic_list(elements_from: chunk_gen, length_from: length_gen)

  use chunks <- qcheck.run(test_helpers.property_config(), chunks_gen)

  let dir = test_helpers.temp_dir()
  let path = dir <> "/prop.db"
  let assert Ok(handle) = file_ffi.open(path: path)

  let _ = do_roundtrip(handle, chunks, 0)

  let assert Ok(Nil) = file_ffi.close(handle: handle)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

fn do_roundtrip(
  handle: file_ffi.FileHandle,
  chunks: List(BitArray),
  offset: Int,
) -> Int {
  case chunks {
    [] -> offset
    [chunk, ..rest] -> {
      let size = bit_array.byte_size(chunk)
      let assert Ok(pos) = file_ffi.append(handle: handle, data: chunk)
      assert pos == offset
      let assert Ok(read_back) =
        file_ffi.pread(handle: handle, offset: offset, length: size)
      assert read_back == chunk
      do_roundtrip(handle, rest, offset + size)
    }
  }
}

pub fn mkdir_p_creates_nested_directories_test() {
  let dir = test_helpers.temp_dir()
  let nested = dir <> "/a/b/c"
  let assert Ok(Nil) = file_ffi.mkdir_p(path: nested)
  let assert Ok(True) = simplifile.is_directory(nested)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn list_dir_returns_filenames_test() {
  let dir = test_helpers.temp_dir()
  let assert Ok(Nil) = simplifile.write(dir <> "/alpha.txt", "a")
  let assert Ok(Nil) = simplifile.write(dir <> "/beta.txt", "b")
  let assert Ok(files) = file_ffi.list_dir(path: dir)
  let sorted = list.sort(files, string.compare)
  assert sorted == ["alpha.txt", "beta.txt"]
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn delete_file_removes_existing_file_test() {
  let dir = test_helpers.temp_dir()
  let path = dir <> "/doomed.txt"
  let assert Ok(Nil) = simplifile.write(path, "bye")
  let assert Ok(Nil) = file_ffi.delete_file(path: path)
  let assert Ok(False) = simplifile.is_file(path)
  let assert Ok(_) = simplifile.delete_all([dir])
  Nil
}

pub fn delete_file_error_for_nonexistent_test() {
  let assert Error(_) = file_ffi.delete_file(path: "/no/such/file.txt")
  Nil
}
