import qcheck
import trove/codec

pub fn string_codec_roundtrip_test() {
  let string_codec = codec.string()
  use s <- qcheck.given(qcheck.string())
  let assert Ok(decoded) = string_codec.decode(string_codec.encode(s))
  assert decoded == s
}

pub fn int_codec_roundtrip_test() {
  let int_codec = codec.int()
  use n <- qcheck.given(qcheck.uniform_int())
  let assert Ok(decoded) = int_codec.decode(int_codec.encode(n))
  assert decoded == n
}

pub fn bit_array_codec_roundtrip_test() {
  let bit_array_codec = codec.bit_array()
  use bits <- qcheck.given(qcheck.bit_array())
  let assert Ok(decoded) = bit_array_codec.decode(bit_array_codec.encode(bits))
  assert decoded == bits
}

pub fn int_codec_boundary_values_test() {
  let int_codec = codec.int()

  // Min signed 64-bit: -2^63
  let min = -9_223_372_036_854_775_808
  let assert Ok(decoded_min) = int_codec.decode(int_codec.encode(min))
  assert decoded_min == min

  // Max signed 64-bit: 2^63 - 1
  let max = 9_223_372_036_854_775_807
  let assert Ok(decoded_max) = int_codec.decode(int_codec.encode(max))
  assert decoded_max == max

  // 9 bytes is invalid (too long)
  let assert Error(Nil) = int_codec.decode(<<0, 0, 0, 0, 0, 0, 0, 0, 0>>)
}

pub fn int_codec_decode_wrong_size_returns_error_test() {
  let assert Error(Nil) = codec.int().decode(<<1, 2, 3>>)
  Nil
}

pub fn string_codec_decode_invalid_utf8_returns_error_test() {
  let assert Error(Nil) = codec.string().decode(<<0xFF, 0xFE>>)
  Nil
}
