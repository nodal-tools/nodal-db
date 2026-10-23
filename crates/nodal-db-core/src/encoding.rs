//! Canonical axis byte encoding and the 64-bit content hash.
//!
//! Pure, deterministic, and free of any `duckdb` use: this module is the one
//! canonicalizer's serialization half (`CLAUDE.md` invariant 2). It implements
//! `specs/canonical_encoding.md` to the byte — two independent implementations
//! that feed the same canonical content MUST produce identical bytes and thus
//! identical hashes. The machine-checkable oracle is
//! `conformance/encoding_vectors/*.json`, driven by `tests/encoding_vectors.rs`.
//!
//! Scope: axis encoding (spec §4–§7) + content hash (§2). The *value* encoding
//! (spec §9) is deferred — see the `// deferred: OPEN-7` note at the bottom.

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::types::{ScalarValue, ValueKey, ValueKind};

/// Micros per day; the fixed conversion for the day-vs-micros split (spec §3.5).
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The `(months, micros)` duration pair (spec §3.5). `INTERVAL` never appears;
/// both components are always materialized, an absent one being `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Duration {
    pub months: i32,
    pub micros: i64,
}

/// Expressed, never-asserted axis properties (spec §6). Each is hash input;
/// a NULL/absent property contributes zero bytes (§8 rehash-stability).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AxisProperties {
    pub calendar: Option<String>,
    pub timezone: Option<String>,
    pub crs: Option<String>,
}

/// One `label`-axis position (spec §7.5): axis-scoped ordinal + label text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelPos {
    pub ordinal: i32,
    pub label: String,
}

/// One `period`-axis position (spec §7.6): explicit `[start, end)` bounds and a
/// per-row optional `years_presented`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeriodPos {
    pub start_ts: i64,
    pub end_ts: i64,
    #[serde(default)]
    pub years_presented: Option<f64>,
}

/// One term of a `time_pattern` position (spec §7.7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimePatternTerm {
    pub unit: String,
    pub range_start: i16,
    pub range_end: i16,
}

/// One `time_pattern` position: a set of terms that union together (spec §7.7).
/// `ordinal` is a property of the position, read from `term_no = 1` (M4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimePatternPos {
    pub ordinal: i32,
    pub terms: Vec<TimePatternTerm>,
}

/// The kind body (spec §7). The variant fixes both the kind tag (§5.1) and the
/// form byte (§5.2): parametric variants are `form = 0x00`, everything else
/// `0x01`. Datetime/integer/float coordinates are µs / i64 / f64 respectively.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AxisBody {
    DatetimeParametric { start_ts: i64, step: Duration },
    DatetimeMaterialized { coords: Vec<i64> },
    IntegerParametric { start: i64, step: i64 },
    IntegerMaterialized { coords: Vec<i64> },
    FloatParametric { start: f64, step: f64 },
    FloatMaterialized { coords: Vec<f64> },
    Label { positions: Vec<LabelPos> },
    Period { positions: Vec<PeriodPos> },
    TimePattern { positions: Vec<TimePatternPos> },
}

/// A fully canonicalized axis, ready to encode. `length` is stored explicitly
/// (spec §5.3) even for materialized bodies, mirroring `axis.length`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalAxis {
    pub length: i32,
    #[serde(default)]
    pub properties: AxisProperties,
    pub body: AxisBody,
}

/// The encoder rejects a value it must not serialize.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// A NaN reached an **axis** float (spec §3.3.1). Axis coordinates must be
    /// orderable/parametric, and NaN has no ordering, so a NaN axis float is
    /// rejected at the writer boundary, never hashed. (VALUE floats, by
    /// contrast, canonicalize NaN — see [`NanPolicy`].)
    #[error("NaN is not an encodable axis value (spec §3.3.1)")]
    NanForbidden,
}

impl CanonicalAxis {
    /// The `axis.kind` tag for this axis (spec §5.1.1). ASCII, so NFC-invariant.
    /// Public so the DB-facing `axis` dedup module can populate `axis.kind` and
    /// filter candidate rows without a second copy of the tag strings.
    pub fn kind_tag(&self) -> &'static str {
        match self.body {
            AxisBody::DatetimeParametric { .. } | AxisBody::DatetimeMaterialized { .. } => {
                "datetime"
            }
            AxisBody::IntegerParametric { .. } | AxisBody::IntegerMaterialized { .. } => "integer",
            AxisBody::FloatParametric { .. } | AxisBody::FloatMaterialized { .. } => "float",
            AxisBody::Label { .. } => "label",
            AxisBody::Period { .. } => "period",
            AxisBody::TimePattern { .. } => "time_pattern",
        }
    }

    /// The form byte (spec §5.2): `0x00` parametric, `0x01` materialized.
    fn form_byte(&self) -> u8 {
        match self.body {
            AxisBody::DatetimeParametric { .. }
            | AxisBody::IntegerParametric { .. }
            | AxisBody::FloatParametric { .. } => 0x00,
            _ => 0x01,
        }
    }
}

/// A string, NFC-normalized (Unicode 16.0.0), `u32`-length-prefixed (spec §3.4).
fn push_string(out: &mut Vec<u8>, s: &str) {
    let nfc: String = s.nfc().collect();
    let bytes = nfc.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// The one canonical quiet-NaN bit pattern for VALUE floats (spec §3.3.1):
/// positive quiet NaN, zero payload. Every NaN value — whatever its incoming
/// sign/quiet/payload bits — canonicalizes to this single pattern, exactly
/// analogous to the `-0.0 → +0.0` normalization, so byte-equality stays
/// well-defined for values (one canonical pattern per value, NaN included).
pub const CANONICAL_NAN_BITS: u64 = 0x7ff8000000000000;

/// How the double-canonicalizer treats a NaN, split by role (spec §3.3.3):
/// axes reject it (no ordering); values canonicalize it (a NaN is a real
/// stored value). This is the whole axis-vs-value asymmetry in one flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NanPolicy {
    /// AXIS floats (`p_start_dbl`, `p_step_dbl`, `coord`, `years_presented`):
    /// a NaN is a programming error at the writer boundary — reject it.
    Reject,
    /// VALUE floats (`v_float`; the `DOUBLE[]` array payload; long-tier
    /// `v_float`): canonicalize every NaN to [`CANONICAL_NAN_BITS`].
    Canonicalize,
}

/// The canonical stored and encoded form of a VALUE float (spec §3.3.1):
/// `-0.0` becomes `+0.0`, every NaN becomes the one quiet NaN
/// ([`CANONICAL_NAN_BITS`]), everything else is unchanged. The one definition
/// shared by the encoder (hash) and the write pipeline (storage), so the two
/// cannot drift. Idempotent.
#[inline]
pub fn canonical_value_f64(x: f64) -> f64 {
    if x.is_nan() {
        f64::from_bits(CANONICAL_NAN_BITS)
    } else if x == 0.0 {
        // `-0.0 == 0.0` is true, so this catches every negative-zero pattern.
        0.0
    } else {
        x
    }
}

/// The canonical form of an AXIS float (spec §3.3.1): `-0.0` becomes `+0.0`;
/// NaN is rejected (axis coordinates must be orderable).
#[inline]
pub fn canonical_axis_f64(x: f64) -> Result<f64, EncodeError> {
    if x.is_nan() {
        Err(EncodeError::NanForbidden)
    } else if x == 0.0 {
        Ok(0.0)
    } else {
        Ok(x)
    }
}

/// A canonical `DOUBLE` (spec §3.3): the role's canonical form
/// ([`canonical_axis_f64`] / [`canonical_value_f64`]), then the 8 IEEE-754
/// binary64 bytes little-endian.
fn push_double(out: &mut Vec<u8>, x: f64, nan: NanPolicy) -> Result<(), EncodeError> {
    let c = match nan {
        NanPolicy::Reject => canonical_axis_f64(x)?,
        NanPolicy::Canonicalize => canonical_value_f64(x),
    };
    out.extend_from_slice(&c.to_bits().to_le_bytes());
    Ok(())
}

/// Encode an axis to its canonical byte string (spec §4). Errors only on a NaN
/// double (§3.3.1); every other axis encodes.
pub fn encode_axis(axis: &CanonicalAxis) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();

    // §4.1 (1) kind tag, (2) form byte, (3) length.
    push_string(&mut out, axis.kind_tag());
    out.push(axis.form_byte());
    out.extend_from_slice(&axis.length.to_le_bytes());

    // §4.1 (4) property block: u8 count of present properties, then present
    // entries in ascending property-id order (calendar=1, timezone=2, crs=3).
    let props = [
        (1u8, axis.properties.calendar.as_deref()),
        (2u8, axis.properties.timezone.as_deref()),
        (3u8, axis.properties.crs.as_deref()),
    ];
    let present = props.iter().filter(|(_, v)| v.is_some()).count() as u8;
    out.push(present);
    for (id, value) in props.iter() {
        if let Some(s) = value {
            out.push(*id);
            push_string(&mut out, s);
        }
    }

    // §4.1 (5) kind body.
    match &axis.body {
        AxisBody::DatetimeParametric { start_ts, step } => {
            out.extend_from_slice(&start_ts.to_le_bytes());
            out.extend_from_slice(&step.months.to_le_bytes());
            out.extend_from_slice(&step.micros.to_le_bytes());
        }
        AxisBody::DatetimeMaterialized { coords } => {
            debug_assert_eq!(
                axis.length as usize,
                coords.len(),
                "length must equal materialized coord count (spec §5.3)"
            );
            for ts in coords {
                out.extend_from_slice(&ts.to_le_bytes());
            }
        }
        AxisBody::IntegerParametric { start, step } => {
            out.extend_from_slice(&start.to_le_bytes());
            out.extend_from_slice(&step.to_le_bytes());
        }
        AxisBody::IntegerMaterialized { coords } => {
            debug_assert_eq!(
                axis.length as usize,
                coords.len(),
                "length must equal materialized coord count (spec §5.3)"
            );
            for n in coords {
                out.extend_from_slice(&n.to_le_bytes());
            }
        }
        AxisBody::FloatParametric { start, step } => {
            push_double(&mut out, *start, NanPolicy::Reject)?;
            push_double(&mut out, *step, NanPolicy::Reject)?;
        }
        AxisBody::FloatMaterialized { coords } => {
            debug_assert_eq!(
                axis.length as usize,
                coords.len(),
                "length must equal materialized coord count (spec §5.3)"
            );
            for c in coords {
                push_double(&mut out, *c, NanPolicy::Reject)?;
            }
        }
        AxisBody::Label { positions } => {
            debug_assert_eq!(
                axis.length as usize,
                positions.len(),
                "length must equal position count (spec §5.3)"
            );
            for p in positions {
                out.extend_from_slice(&p.ordinal.to_le_bytes());
                push_string(&mut out, &p.label);
            }
        }
        AxisBody::Period { positions } => {
            debug_assert_eq!(
                axis.length as usize,
                positions.len(),
                "length must equal position count (spec §5.3)"
            );
            for p in positions {
                out.extend_from_slice(&p.start_ts.to_le_bytes());
                out.extend_from_slice(&p.end_ts.to_le_bytes());
                match p.years_presented {
                    Some(y) => {
                        out.push(0x01);
                        push_double(&mut out, y, NanPolicy::Reject)?;
                    }
                    None => out.push(0x00),
                }
            }
        }
        AxisBody::TimePattern { positions } => {
            debug_assert_eq!(
                axis.length as usize,
                positions.len(),
                "length must equal position count (spec §5.3)"
            );
            for p in positions {
                out.extend_from_slice(&p.ordinal.to_le_bytes());
                out.extend_from_slice(&(p.terms.len() as u32).to_le_bytes());
                for t in &p.terms {
                    push_string(&mut out, &t.unit);
                    out.extend_from_slice(&t.range_start.to_le_bytes());
                    out.extend_from_slice(&t.range_end.to_le_bytes());
                }
            }
        }
    }

    Ok(out)
}

/// The content hash: XXH3-64, seed 0, over the complete canonical byte string
/// (spec §2). The returned `u64` is stored verbatim into `axis.content_hash`.
pub fn content_hash(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

/// Convenience: encode then hash.
pub fn hash_axis(axis: &CanonicalAxis) -> Result<u64, EncodeError> {
    Ok(content_hash(&encode_axis(axis)?))
}

// ---------------------------------------------------------------------------
// Regularity / parametric-first store helpers (spec §7.1, §10).
//
// These decide reproduce-or-materialize: an axis is regular *iff* the pinned
// reconstruction seeded from `c[0]` and one step reproduces every materialized
// coordinate exactly. They are the store decision the canonicalization pipeline
// (Phase 2) and these conformance vectors share.
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 for a proleptic-Gregorian civil date (Hinnant).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let (m, d) = (m as i64, d as i64);
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Inverse of `days_from_civil`: civil `(year, month, day)` from a day count.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m as u32, d)
}

/// Last day of a civil month (handles leap years).
fn last_day_of_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
            if leap {
                29
            } else {
                28
            }
        }
        _ => unreachable!("month out of range"),
    }
}

/// Add `months` to a µs-since-epoch timestamp with DuckDB's end-of-month clamp
/// (spec §7.1.2.1): a single month multiply applied once, day clamped to the
/// target month's last day, time-of-day carried through unchanged.
fn add_months_clamp(ts_micros: i64, months: i64) -> Option<i64> {
    let day = ts_micros.div_euclid(MICROS_PER_DAY);
    let tod = ts_micros.rem_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(day);
    let total = y
        .checked_mul(12)?
        .checked_add(m as i64 - 1)?
        .checked_add(months)?;
    let ny = total.div_euclid(12);
    let nm = (total.rem_euclid(12) + 1) as u32;
    let nd = d.min(last_day_of_month(ny, nm));
    days_from_civil(ny, nm, nd)
        .checked_mul(MICROS_PER_DAY)?
        .checked_add(tod)
}

/// The pinned datetime reconstruction (spec §7.1.2.1): apply the month interval
/// `step.months * pos` to `start_ts` first (clamped), then add the exact micros
/// offset `step.micros * pos`. Returns `None` on i64 overflow.
pub fn datetime_coord(start_ts: i64, step: &Duration, pos: i64) -> Option<i64> {
    let months = (step.months as i64).checked_mul(pos)?;
    let after_months = add_months_clamp(start_ts, months)?;
    let micros = step.micros.checked_mul(pos)?;
    after_months.checked_add(micros)
}

/// Does `(start_ts, step)` reproduce every coordinate µs-exactly (spec §7.1.2.1)?
/// `true` ⇒ the axis is regular and MUST be stored parametric; `false` ⇒ this
/// candidate step is not the axis's step (materialize unless another step fits).
pub fn datetime_reproduces(start_ts: i64, step: &Duration, coords: &[i64]) -> bool {
    coords
        .iter()
        .enumerate()
        .all(|(pos, &c)| datetime_coord(start_ts, step, pos as i64) == Some(c))
}

/// Civil `(year, month, day)` of a µs-since-epoch timestamp. Exposed for
/// canon's datetime step *proposal* (canon owns step derivation; this module
/// owns the clamp/reproduce verification, spec §7.1.2) so the two share one
/// civil-calendar implementation — no second calendar to drift (CLAUDE.md
/// invariant 2). A wrong proposal is harmless: `datetime_reproduces` rejects it
/// and the axis materializes, so this only affects completeness, never
/// correctness.
pub fn ymd_from_micros(ts_micros: i64) -> (i64, u32, u32) {
    civil_from_days(ts_micros.div_euclid(MICROS_PER_DAY))
}

/// Integer regularity (spec §7.1.2.3): seed `start = c[0]`, `step = c[1]-c[0]`,
/// and verify `start + step*pos` reproduces every coordinate under checked i64
/// arithmetic. `Some(step)` ⇒ store parametric; `None` ⇒ materialize.
pub fn integer_regular_step(coords: &[i64]) -> Option<i64> {
    if coords.len() < 2 {
        // A length-1 axis is trivially parametric with any step; canonically 0.
        return Some(0);
    }
    let start = coords[0];
    let step = coords[1].checked_sub(coords[0])?;
    for (pos, &c) in coords.iter().enumerate() {
        let expected = step.checked_mul(pos as i64)?.checked_add(start)?;
        if expected != c {
            return None;
        }
    }
    Some(step)
}

/// Float regularity (spec §10.3): seed `start = c[0]`, `step = c[1]-c[0]`, and
/// verify `start + step*pos` reproduces every coordinate *bit-exactly*. The
/// arithmetic is pinned to `start + (step * pos)` — two separately-rounded
/// IEEE-754 ops, NO fused multiply-add (spec §10.2) — so it matches the
/// `axis_position` SQL view bit-for-bit. `Some(step)` ⇒ parametric; `None` ⇒
/// materialize (incl. the 1-ULP-off case, §10.4).
pub fn float_regular_step(coords: &[f64]) -> Option<f64> {
    if coords.len() < 2 {
        return Some(0.0);
    }
    let start = coords[0];
    let step = coords[1] - coords[0];
    for (pos, &c) in coords.iter().enumerate() {
        // Pinned: multiply first, then add; two roundings, not one. rustc/LLVM
        // does not fuse a non-`mul_add` expression, so this stays two ops.
        let expected = start + step * (pos as f64);
        if expected.to_bits() != c.to_bits() {
            return None;
        }
    }
    Some(step)
}

// ---------------------------------------------------------------------------
// Value canonical encoding (spec §9), OPEN-7 resolution.
//
// jkiviluo (2026-08-17) closed OPEN-7: the value encoding INCLUDES full
// context — the identity KEY (entity/parameter/alternative) and the
// position/shape (axis content hashes + per-cell positions) — NOT measure-only.
// The measure itself is still encoded component-wise / populated-columns-only
// (D-5, spec §9.4): a future second component slot (e.g. `v_imag`) is emitted
// only when populated, so it never rehashes an existing value.
// ---------------------------------------------------------------------------

/// The shaped content of a single canonicalized value, feeding the value
/// encoding (spec §9). Built by `canon` from a `CanonicalValue`; the borrow
/// keeps the axis-hash / cell buffers owned by the caller.
pub enum ValueContent<'a> {
    /// Rank-0 scalar: no axes, no positions.
    Scalar(&'a ScalarValue),
    /// Tier A dense float array: shape given by the axis content hashes, payload
    /// row-major flattened (spec §9.5). Float-only, matching the Tier-A rule.
    Array {
        axis_hashes: &'a [u64],
        values: &'a [f64],
    },
    /// Tier L rank>=1: per-cell positions + measure, in canonical row order.
    Long {
        value_kind: ValueKind,
        axis_hashes: &'a [u64],
        cells: &'a [(Vec<i32>, ScalarValue)],
    },
}

/// A `u8` component-count prefix then `k` present-only entries, each a `u8`
/// slot-id followed by the component's bytes (spec §9.3, mirroring the §6.3
/// property block). Today `k == 1` (slot 1, the primary measure); a future
/// populated slot 2 raises the count only on values that carry it, leaving every
/// existing single-component value's bytes unchanged (spec §9.4 / §8).
fn push_component_block(out: &mut Vec<u8>, components: &[(u8, Vec<u8>)]) {
    out.push(components.len() as u8);
    for (slot, bytes) in components {
        out.push(*slot);
        out.extend_from_slice(bytes);
    }
}

/// Rank byte then each axis content hash (`u64` LE) — the value's shape/identity
/// context (spec §9, OPEN-7). Scalars carry rank 0 and no hashes.
fn push_shape(out: &mut Vec<u8>, axis_hashes: &[u64]) {
    out.push(axis_hashes.len() as u8);
    for h in axis_hashes {
        out.extend_from_slice(&h.to_le_bytes());
    }
}

/// Encode a scalar measure by its physical type (spec §9.5). Returns the
/// slot-1 component bytes.
fn encode_measure(m: &ScalarValue) -> Result<Vec<u8>, EncodeError> {
    let mut b = Vec::new();
    match m {
        // VALUE float: NaN is a real stored value, canonicalized (spec §3.3.1).
        ScalarValue::Float(x) => push_double(&mut b, *x, NanPolicy::Canonicalize)?,
        ScalarValue::Int(n) => b.extend_from_slice(&n.to_le_bytes()),
        ScalarValue::Bool(v) => b.push(if *v { 0x01 } else { 0x00 }),
        ScalarValue::Str(s) => push_string(&mut b, s),
        ScalarValue::DateTime(ts) => b.extend_from_slice(&ts.to_le_bytes()),
        ScalarValue::Duration { months, micros } => {
            b.extend_from_slice(&months.to_le_bytes());
            b.extend_from_slice(&micros.to_le_bytes());
        }
    }
    Ok(b)
}

/// The tier tag byte (spec §9.3 step 3): an explicit discriminator so a scalar,
/// array, and long encoding can never byte-collide, independent of any structural
/// coincidence in the shape/measure block. This is the value-side analogue of the
/// axis form byte (§5.2). Emitted right after the `value_kind` tag, before the
/// shape block.
const TIER_SCALAR: u8 = 0x00;
const TIER_ARRAY: u8 = 0x01;
const TIER_LONG: u8 = 0x02;

/// The canonical byte string of a single value (spec §9, OPEN-7). Layout:
///   1. key: entity_id, parameter_definition_id, alternative_id (`i64` LE each);
///   2. `value_kind` tag (string, §3.4);
///   3. tier tag byte (§9.3 step 3): scalar `0x00`, array `0x01`, long `0x02`;
///   4. shape: rank `u8` + axis content hashes (`u64` LE each), §9 OPEN-7;
///   5. measure block, per tier:
///      - scalar: one component block (slot 1) of the measure;
///      - array:  one component block whose slot-1 bytes are a `u32` element
///        count then each canonical DOUBLE, row-major (§9.5);
///      - long:   a `u32` cell count, then per cell the positions (`i32` LE per
///        axis) + a component block of the cell measure.
///
/// Errors only on a NaN measure (§3.3.1).
pub fn encode_value(key: &ValueKey, content: &ValueContent) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();
    out.extend_from_slice(&key.entity_id.to_le_bytes());
    out.extend_from_slice(&key.parameter_definition_id.to_le_bytes());
    out.extend_from_slice(&key.alternative_id.to_le_bytes());

    match content {
        ValueContent::Scalar(m) => {
            push_string(&mut out, m.kind().tag());
            out.push(TIER_SCALAR);
            push_shape(&mut out, &[]);
            push_component_block(&mut out, &[(1u8, encode_measure(m)?)]);
        }
        ValueContent::Array {
            axis_hashes,
            values,
        } => {
            push_string(&mut out, ValueKind::Float.tag());
            out.push(TIER_ARRAY);
            push_shape(&mut out, axis_hashes);
            let mut payload = Vec::new();
            payload.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for x in *values {
                // VALUE array payload: NaN canonicalized, never rejected (§3.3.1).
                push_double(&mut payload, *x, NanPolicy::Canonicalize)?;
            }
            push_component_block(&mut out, &[(1u8, payload)]);
        }
        ValueContent::Long {
            value_kind,
            axis_hashes,
            cells,
        } => {
            push_string(&mut out, value_kind.tag());
            out.push(TIER_LONG);
            push_shape(&mut out, axis_hashes);
            out.extend_from_slice(&(cells.len() as u32).to_le_bytes());
            for (positions, m) in *cells {
                for p in positions {
                    out.extend_from_slice(&p.to_le_bytes());
                }
                push_component_block(&mut out, &[(1u8, encode_measure(m)?)]);
            }
        }
    }
    Ok(out)
}

/// Convenience: encode then hash a value (spec §2 over §9 bytes).
pub fn hash_value(key: &ValueKey, content: &ValueContent) -> Result<u64, EncodeError> {
    Ok(content_hash(&encode_value(key, content)?))
}

#[cfg(test)]
mod value_tests {
    use super::*;

    #[test]
    fn component_block_slot2_is_rehash_stable() {
        // A value with only slot 1 populated encodes with count = 1. Opening a
        // future slot-2 door (a `v_imag` component) appends bytes ONLY on values
        // that populate it, so a slot-1-only value's bytes are byte-identical
        // before and after the door exists (spec §9.4 / §8). This is the exact
        // freeze-safety guarantee the count prefix buys.
        let measure = vec![0xAA, 0xBB];

        let mut only_slot1 = Vec::new();
        push_component_block(&mut only_slot1, &[(1, measure.clone())]);
        assert_eq!(only_slot1, [1u8, 1u8, 0xAA, 0xBB]);

        // A value that DOES populate slot 2 is a different (count = 2) encoding —
        // correctly distinct content, never a silent rehash of the slot-1 value.
        let mut both = Vec::new();
        push_component_block(&mut both, &[(1, measure), (2, vec![0xCC])]);
        assert_eq!(both, [2u8, 1u8, 0xAA, 0xBB, 2u8, 0xCC]);
        assert_ne!(both, only_slot1);
    }

    #[test]
    fn value_encoder_canonicalizes_any_nan_to_the_one_pattern() {
        // A VALUE float NaN is canonicalized to 0x7ff8000000000000 (spec §3.3.1),
        // analogous to -0.0 → +0.0. Even a signaling / alternate-payload NaN bit
        // pattern collapses to the one canonical quiet NaN, so byte-equality
        // stays well-defined for values (one canonical pattern per value).
        let key = ValueKey {
            entity_id: 1,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        // A signaling NaN (top mantissa bit clear) with a non-zero payload, and
        // the sign bit set — as different from the canonical pattern as a NaN gets.
        let weird = f64::from_bits(0xfff0_0000_0000_0001);
        assert!(weird.is_nan(), "fixture must be a NaN bit pattern");
        let a = encode_value(&key, &ValueContent::Scalar(&ScalarValue::Float(weird))).unwrap();
        let b = encode_value(&key, &ValueContent::Scalar(&ScalarValue::Float(f64::NAN))).unwrap();
        assert_eq!(a, b, "every NaN bit pattern must encode identically");
        // The last 8 bytes are the measure: the canonical quiet-NaN pattern, LE.
        assert_eq!(
            &a[a.len() - 8..],
            &CANONICAL_NAN_BITS.to_le_bytes(),
            "value NaN must encode as 0x7ff8000000000000"
        );
        // And it does NOT error (the whole point — values keep the array tier).
        assert!(encode_value(&key, &ValueContent::Scalar(&ScalarValue::Float(f64::NAN))).is_ok());
    }

    /// The value-float pattern table: (input bits, expected canonical bits).
    const PATTERNS: [(u64, u64); 15] = [
        (0x8000_0000_0000_0000, 0x0000_0000_0000_0000), // -0.0
        (0x0000_0000_0000_0000, 0x0000_0000_0000_0000), // +0.0
        (0x7ff8_0000_0000_0000, 0x7ff8_0000_0000_0000), // canonical NaN
        (0xfff8_0000_0000_0000, 0x7ff8_0000_0000_0000), // negative quiet NaN
        (0x7ff0_0000_0000_0001, 0x7ff8_0000_0000_0000), // signaling NaN
        (0x7ff4_0000_0000_0000, 0x7ff8_0000_0000_0000), // signaling, high payload
        (0x7fff_ffff_ffff_ffff, 0x7ff8_0000_0000_0000), // quiet, max payload
        (0xfff0_0000_0000_0001, 0x7ff8_0000_0000_0000), // negative signaling
        (0x7ff0_0000_0000_0000, 0x7ff0_0000_0000_0000), // +inf
        (0xfff0_0000_0000_0000, 0xfff0_0000_0000_0000), // -inf
        (0x0000_0000_0000_0001, 0x0000_0000_0000_0001), // +min subnormal
        (0x8000_0000_0000_0001, 0x8000_0000_0000_0001), // -min subnormal
        (0x3ff8_0000_0000_0000, 0x3ff8_0000_0000_0000), // 1.5
        (0xbff8_0000_0000_0000, 0xbff8_0000_0000_0000), // -1.5
        (0x4059_0000_0000_0000, 0x4059_0000_0000_0000), // 100.0
    ];

    #[test]
    fn canonical_value_f64_follows_the_pattern_table_and_is_idempotent() {
        for (input, expected) in PATTERNS {
            let c = super::canonical_value_f64(f64::from_bits(input));
            assert_eq!(c.to_bits(), expected, "input {input:#018x}");
            assert_eq!(
                super::canonical_value_f64(c).to_bits(),
                c.to_bits(),
                "idempotent on {input:#018x}"
            );
        }
    }

    #[test]
    fn canonical_axis_f64_rejects_nan_and_normalizes_zero() {
        for (input, expected) in PATTERNS {
            let x = f64::from_bits(input);
            match super::canonical_axis_f64(x) {
                Ok(c) => {
                    assert!(!x.is_nan());
                    assert_eq!(c.to_bits(), expected, "input {input:#018x}");
                }
                Err(e) => {
                    assert!(x.is_nan(), "only NaN is rejected: {input:#018x}");
                    assert_eq!(e, EncodeError::NanForbidden);
                }
            }
        }
    }

    #[test]
    fn encoding_is_invariant_under_value_canonicalization() {
        // encode(x) == encode(c(x)) byte-for-byte: storing c(x) changes no hash.
        let key = ValueKey {
            entity_id: 1,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        for (input, _) in PATTERNS {
            let x = f64::from_bits(input);
            let c = super::canonical_value_f64(x);
            let a = encode_value(&key, &ValueContent::Scalar(&ScalarValue::Float(x))).unwrap();
            let b = encode_value(&key, &ValueContent::Scalar(&ScalarValue::Float(c))).unwrap();
            assert_eq!(a, b, "scalar {input:#018x}");
        }
        let raw: Vec<f64> = PATTERNS.iter().map(|(i, _)| f64::from_bits(*i)).collect();
        let canon: Vec<f64> = raw.iter().map(|x| super::canonical_value_f64(*x)).collect();
        let hashes = [7u64];
        let a = encode_value(
            &key,
            &ValueContent::Array {
                axis_hashes: &hashes,
                values: &raw,
            },
        )
        .unwrap();
        let b = encode_value(
            &key,
            &ValueContent::Array {
                axis_hashes: &hashes,
                values: &canon,
            },
        )
        .unwrap();
        assert_eq!(a, b, "array payload");
    }

    #[test]
    fn axis_encoder_still_rejects_nan() {
        // The axis path is UNCHANGED: a NaN coordinate has no ordering and is
        // rejected (spec §3.3.1). This is the load-bearing axis-vs-value split.
        let axis = crate::encoding::CanonicalAxis {
            length: 1,
            properties: Default::default(),
            body: crate::encoding::AxisBody::FloatMaterialized {
                coords: vec![f64::NAN],
            },
        };
        assert_eq!(
            crate::encoding::encode_axis(&axis),
            Err(EncodeError::NanForbidden),
            "a NaN axis coordinate must still be rejected"
        );
    }

    #[test]
    fn scalar_value_folds_in_key() {
        // OPEN-7: identical measures under different keys encode differently.
        let m = ScalarValue::Float(1.5);
        let k1 = ValueKey {
            entity_id: 1,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        let k2 = ValueKey {
            entity_id: 9,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        let a = encode_value(&k1, &ValueContent::Scalar(&m)).unwrap();
        let b = encode_value(&k2, &ValueContent::Scalar(&m)).unwrap();
        assert_ne!(
            a, b,
            "value encoding MUST fold in the identity key (OPEN-7)"
        );
    }
}
