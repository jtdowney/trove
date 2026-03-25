//// Public types for range queries.
////
//// Use `Bound(k)` to set lower and upper limits, and `Direction` to control
//// iteration order. Pass these to `trove.range` or `trove.snapshot_range`.
////
//// ```gleam
//// import gleam/option.{None, Some}
//// import trove
//// import trove/range
////
//// let entries =
////   trove.range(
////     db,
////     min: Some(range.Inclusive("a")),
////     max: Some(range.Exclusive("z")),
////     direction: range.Forward,
////   )
//// ```

/// A bound for range queries.
///
/// Use `Inclusive(key)` to include the boundary key in results,
/// or `Exclusive(key)` to exclude it.
pub type Bound(k) {
  /// Include the boundary key in the result set.
  Inclusive(k)
  /// Exclude the boundary key from the result set.
  Exclusive(k)
}

/// Controls the iteration direction for range queries.
pub type Direction {
  /// Iterate from the smallest matching key to the largest.
  Forward
  /// Iterate from the largest matching key to the smallest.
  Reverse
}
