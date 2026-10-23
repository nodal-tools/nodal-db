//! The tier decision — step 4 of the canonicalization pipeline (VMP Part 3).
//!
//! Pure and deterministic: given the derived axis lengths, the declared
//! `value_kind`, and the index tuples already sorted row-major with distinct
//! positions (canon steps 1-3), decide the physical tier and shape the payload.
//! The rule is exactly VMP Part 3 step 4:
//!   - rank 0                                            -> Tier::Scalar
//!   - rank>=1, float-valued, tuple set == Cartesian     -> Tier::Array
//!   - otherwise                                          -> Tier::Long
//! "Cartesian" is: all tuples distinct AND count == product of axis lengths.
//! Distinctness is guaranteed upstream (canon rejects duplicate tuples), so
//! here the product check alone decides it.

use crate::types::{LongRow, ScalarValue, ShapedData, Tier, ValueKind};

/// Decide the tier and shape the payload. `rows` MUST be canon's output: NULL-
/// eliminated, position-mapped, sorted row-major, and duplicate-free.
///
/// For rank 0, `rows` holds exactly one cell (canon enforces it) and its value
/// is the scalar. For the array tier the sorted rows ARE the row-major
/// flattening — every cell of the full Cartesian grid is present exactly once,
/// in row-major order — so their values in order are the dense array.
pub fn decide(
    axis_lengths: &[usize],
    value_kind: ValueKind,
    rows: &[LongRow],
) -> (Tier, ShapedData) {
    if axis_lengths.is_empty() {
        return (Tier::Scalar, ShapedData::Scalar(rows[0].value.clone()));
    }

    let product: usize = axis_lengths.iter().product();
    let is_cartesian = rows.len() == product;

    if value_kind == ValueKind::Float && is_cartesian {
        let values: Vec<f64> = rows
            .iter()
            .map(|r| match r.value {
                ScalarValue::Float(x) => x,
                // canon guarantees float measures when value_kind == Float.
                _ => unreachable!("float tier with a non-float measure"),
            })
            .collect();
        return (Tier::Array, ShapedData::Array(values));
    }

    (Tier::Long, ShapedData::Long(rows.to_vec()))
}
