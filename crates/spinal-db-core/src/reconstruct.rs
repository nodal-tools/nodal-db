//! Array-tier reconstruction: the flat `DOUBLE[]` payload -> logical long rows
//! (the inverse of what `canon`/`tier` produced). Pure, deterministic, no DB.
//!
//! The array tier stores a dense float value as a flat payload laid out
//! **row-major in declared-dimension order** (C order: the last dimension varies
//! fastest), length = product of the axis lengths. Reconstruction maps each flat
//! index `k` back to the 0-based index tuple `(p1..pn)` by the standard row-major
//! decomposition, yielding one `LongRow` per cell in flat order.
//!
//! This MUST agree bit-for-bit with the SQL `value_array_cells` view
//! (`schema/080_views_read.sql`), which computes the same positions as
//!   i1 = k / (n2·n3·n4),  i2 = (k / (n3·n4)) % n2,
//!   i3 = (k / n4) % n3,   i4 = k % n4
//! — the row-major strides `stride[i] = ∏_{j>i} nⱼ`, `p_i = (k / stride[i]) % nᵢ`.
//! The normative statement and the column-major (Julia/Fortran) transpose hazard
//! are in `spec/reconstruction.md`; the machine-checkable fixtures live in
//! `conformance/reconstruction_fixtures/` (runner
//! `tests/reconstruction_fixtures.rs`).

use crate::types::{LongRow, ScalarValue};

/// Reconstruction rejects a payload whose length does not match the axes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReconstructError {
    /// The flat payload length must equal the product of the axis lengths — the
    /// array tier is dense (`schema/050_values.sql`, "length = product of axis
    /// lengths (audited), contains no NULLs").
    #[error("payload length {got} != product of axis lengths {expected}")]
    LengthMismatch { expected: usize, got: usize },
}

/// Expand a dense float array payload into canonical long rows.
///
/// `axis_lengths` are the derived axis lengths in **declared-dimension order**
/// (`|A1|..|An|`); `values` is the flat `DOUBLE[]` payload, row-major. Returns
/// one `LongRow` per cell in flat (row-major) order, so `out[k].positions` is the
/// row-major decomposition of `k` and `out[k].value == values[k]`.
///
/// A rank-0 (empty `axis_lengths`) payload of length 1 reconstructs to a single
/// row with an empty position tuple; any other rank-0 length is a mismatch.
pub fn reconstruct(axis_lengths: &[usize], values: &[f64]) -> Result<Vec<LongRow>, ReconstructError> {
    let expected: usize = axis_lengths.iter().product();
    if values.len() != expected {
        return Err(ReconstructError::LengthMismatch {
            expected,
            got: values.len(),
        });
    }

    // Row-major strides: stride[i] = product of the lengths of the dimensions
    // AFTER i (stride of the last dimension is 1). Built right-to-left.
    let rank = axis_lengths.len();
    let mut strides = vec![1usize; rank];
    for i in (0..rank.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * axis_lengths[i + 1];
    }

    let mut rows = Vec::with_capacity(values.len());
    for (k, &v) in values.iter().enumerate() {
        let positions: Vec<i32> = (0..rank)
            .map(|i| ((k / strides[i]) % axis_lengths[i]) as i32)
            .collect();
        rows.push(LongRow {
            positions,
            value: ScalarValue::Float(v),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank1_is_identity() {
        let rows = reconstruct(&[3], &[10.0, 20.0, 30.0]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].positions, vec![0]);
        assert_eq!(rows[2].positions, vec![2]);
    }

    #[test]
    fn rank2_is_row_major_not_transposed() {
        // 2x3 payload [1..6]; row-major puts p2 (the length-3 dim) fastest.
        let rows = reconstruct(&[2, 3], &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).unwrap();
        assert_eq!(rows[0].positions, vec![0, 0]); // value 1
        assert_eq!(rows[1].positions, vec![0, 1]); // value 2 — NOT (1,0) (column-major)
        assert_eq!(rows[3].positions, vec![1, 0]); // value 4
    }

    #[test]
    fn length_mismatch_is_rejected() {
        assert!(reconstruct(&[2, 3], &[1.0, 2.0]).is_err());
    }
}
