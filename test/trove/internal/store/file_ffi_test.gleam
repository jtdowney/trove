import gleam/bit_array
import gleam/erlang/process
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

pub fn append_returns_offset_test() {
  use path <- with_temp_file()
  let assert Ok(handle) = file_ffi.open(path: path)

  let assert Ok(0) = file_ffi.append(handle: handle, data: <<"hello":utf8>>)
  let assert Ok(5) = file_ffi.append(handle: handle, data: <<" world":utf8>>)

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

pub fn unlock_from_non_owner_does_not_release_lock_test() {
  let dir = test_helpers.temp_dir()
  let assert Ok(Nil) = file_ffi.try_lock(path: dir)

  let subject = process.new_subject()
  process.spawn(fn() {
    let _ = file_ffi.unlock(path: dir)
    process.send(subject, Nil)
  })
  let assert Ok(Nil) = process.receive(subject, 1000)

  let assert Error(_) = file_ffi.try_lock(path: dir)

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
