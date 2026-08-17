//! Canonical axis byte encoding and the 64-bit content hash.
//!
//! Pure, deterministic, and free of any `duckdb` use: this module is the one
//! canonicalizer's serialization half (`CLAUDE.md` invariant 2). It implements
//! `spec/canonical_encoding.md` to the byte — two independent implementations
//! that feed the same canonical content MUST produce identical bytes and thus
//! identical hashes. The machine-checkable oracle is
//! `conformance/encoding_vectors/*.json`, driven by `tests/encoding_vectors.rs`.
//!
//! Scope: axis encoding (spec §4–§7) + content hash (§2). The *value* encoding
//! (spec §9) is deferred — see the `// deferred: OPEN-7` note at the bottom.

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

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
    /// A NaN reached the encoder. NaN is forbidden (spec §3.3.1); it must be
    /// rejected at the writer boundary, never hashed.
    #[error("NaN is not an encodable value (spec §3.3.1)")]
    NanForbidden,
}

impl CanonicalAxis {
    /// The `axis.kind` tag for this axis (spec §5.1.1). ASCII, so NFC-invariant.
    fn kind_tag(&self) -> &'static str {
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

/// A canonical `DOUBLE` (spec §3.3): NaN rejected, `-0.0` normalized to `+0.0`,
/// then the 8 IEEE-754 binary64 bytes little-endian.
fn push_double(out: &mut Vec<u8>, x: f64) -> Result<(), EncodeError> {
    if x.is_nan() {
        return Err(EncodeError::NanForbidden);
    }
    // `-0.0 == 0.0` is true, so this catches every negative-zero bit pattern.
    let canon = if x == 0.0 { 0.0_f64 } else { x };
    out.extend_from_slice(&canon.to_le_bytes());
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
            push_double(&mut out, *start)?;
            push_double(&mut out, *step)?;
        }
        AxisBody::FloatMaterialized { coords } => {
            debug_assert_eq!(
                axis.length as usize,
                coords.len(),
                "length must equal materialized coord count (spec §5.3)"
            );
            for c in coords {
                push_double(&mut out, *c)?;
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
                        push_double(&mut out, y)?;
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

// deferred: OPEN-7 — the VALUE component-wise encoding (spec §9) is NOT
// implemented here. It hinges on the value-equality semantics still awaiting
// jkiviluo's sign-off (spec §9.6 / OPEN-7). No value-encoding logic exists in
// this module by design; it lands once OPEN-7 is resolved.
