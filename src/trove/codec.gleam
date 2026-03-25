//// Codecs for serializing keys and values to and from `BitArray`.
////
//// A `Codec(a)` pairs an encode function with a decode function. Built-in
//// codecs cover strings, integers, and raw bytes. For custom types, construct
//// a `Codec` directly with your own encode/decode functions.
////
//// Custom codecs must satisfy `decode(encode(v)) == Ok(v)` and produce
//// deterministic output (same value always encodes to the same bytes).

import gleam/bit_array
import gleam/function

/// A codec that can serialize a value of type `a` to `BitArray` and back.
///
/// Built-in codecs cover common types. For custom types, construct a `Codec`
/// directly:
///
/// ```gleam
/// let float_codec = codec.Codec(
///   encode: fn(f) { <<f:float>> },
///   decode: fn(bits) {
///     case bits {
///       <<f:float>> -> Ok(f)
///       _ -> Error(Nil)
///     }
///   },
/// )
/// ```
pub type Codec(a) {
  Codec(encode: fn(a) -> BitArray, decode: fn(BitArray) -> Result(a, Nil))
}

/// UTF-8 string codec.
///
/// ```gleam
/// let c = codec.string()
/// let bits = c.encode("hello")
/// let assert Ok("hello") = c.decode(bits)
/// ```
pub fn string() -> Codec(String) {
  Codec(encode: bit_array.from_string, decode: bit_array.to_string)
}

/// 64-bit big-endian signed integer codec (two's complement).
/// Values outside the signed 64-bit range (`-2^63` to `2^63 - 1`) silently
/// wrap on encode.
///
/// **Note:** This means `decode(encode(v))` may not equal `Ok(v)` for values
/// outside the signed 64-bit range. If you need arbitrary-precision integers,
/// provide a custom codec.
///
/// ```gleam
/// let c = codec.int()
/// let bits = c.encode(42)
/// let assert Ok(42) = c.decode(bits)
/// ```
pub fn int() -> Codec(Int) {
  Codec(encode: fn(n) { <<n:int-big-size(64)>> }, decode: fn(bits) {
    case bits {
      <<n:signed-big-size(64)>> -> Ok(n)
      _ -> Error(Nil)
    }
  })
}

/// Identity codec for raw bytes. Encodes and decodes as-is.
///
/// ```gleam
/// let c = codec.bit_array()
/// let bits = c.encode(<<1, 2, 3>>)
/// let assert Ok(<<1, 2, 3>>) = c.decode(bits)
/// ```
pub fn bit_array() -> Codec(BitArray) {
  Codec(encode: function.identity, decode: Ok)
}
