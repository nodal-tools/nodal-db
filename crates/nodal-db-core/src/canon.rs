//! The canonicalization pipeline (VMP Part 3) — the single point where tier /
//! axis-form drift would originate (CLAUDE.md invariant 2). Pure and
//! deterministic over Arrow arrays (`duckdb::arrow`, no DB connection). The four
//! normative steps, in order:
//!   1. NULL elimination (NULL ≡ absent): drop index tuples whose value is NULL.
//!   2. Axis derivation from data (parametric-first): per dimension, derive the
//!      axis from the distinct present coords — regular progressions MUST
//!      canonicalize to parametric `(start, step, n)`; label ordinals come from
//!      the caller's ordered domain restricted to present labels.
//!   3. Sort index tuples row-major in declared-dimension order.
//!   4. Tier (delegated to `crate::tier`).
//!
//! Input is a `RecordBatch` with `rank` dimension columns in declared order
//! followed by one value column (rank 0 = just the value column). Dimension
//! Arrow types by kind: `datetime` -> `Timestamp(µs)`, `integer` -> `Int64`,
//! `label` -> `Utf8`. The value column's type follows `value_kind`.

use std::collections::{HashMap, HashSet};

use duckdb::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::record_batch::RecordBatch;

use crate::encoding::{
    canonical_value_f64, datetime_coord, datetime_reproduces, encode_value, hash_axis,
    integer_regular_step, AxisBody, CanonicalAxis, Duration, EncodeError, LabelPos, ValueContent,
};
use crate::tier;
use crate::types::{
    DeclaredType, IndexKind, LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind,
};

/// Smallest timestamp (µs since the Unix epoch) the store accepts: DuckDB's
/// TIMESTAMP minimum (unchanged 1.5.5 → 2.0.0), 290309-12-22 (BC) 00:00:00. DuckDB *writes*
/// values below it (down to `i64::MIN`) without complaint, but every later
/// read of the column then fails with a conversion error, so the pipeline
/// rejects them up front.
pub const TS_MIN_US: i64 = -9_223_372_022_400_000_000;
/// Largest timestamp (µs) the store accepts: DuckDB's TIMESTAMP maximum
/// (unchanged 1.5.5 → 2.0.0),
/// 294247-01-10 04:00:54.775806. `i64::MAX` is DuckDB's `infinity` sentinel
/// (and `-i64::MAX` its `-infinity`, below `TS_MIN_US`); neither is a
/// storable timestamp.
pub const TS_MAX_US: i64 = 9_223_372_036_854_775_806;

/// Is `us` inside the store's timestamp range `[TS_MIN_US, TS_MAX_US]`?
#[inline]
pub fn timestamp_in_range(us: i64) -> bool {
    (TS_MIN_US..=TS_MAX_US).contains(&us)
}

/// A caller-supplied ordered domain for one label dimension (VMP Part 3 step 2).
/// `labels[i]` has ordinal `i`; present labels take their ordinal from this list.
#[derive(Clone, Debug)]
pub struct LabelDomain {
    pub position: u8,
    pub labels: Vec<String>,
}

/// The fully canonicalized value: tier, declared measure kind, the derived axes
/// (one per dimension, declared order), and the shaped payload.
#[derive(Clone, Debug, PartialEq)]
pub struct CanonicalValue {
    pub tier: Tier,
    pub value_kind: ValueKind,
    pub axes: Vec<CanonicalAxis>,
    pub shape: ShapedData,
}

impl CanonicalValue {
    /// Content hash of each derived axis (spec §2), declared order.
    pub fn axis_hashes(&self) -> Result<Vec<u64>, EncodeError> {
        self.axes.iter().map(hash_axis).collect()
    }

    /// The canonical value encoding for a given key (spec §9, OPEN-7).
    pub fn encode(&self, key: &ValueKey) -> Result<Vec<u8>, EncodeError> {
        let hashes = self.axis_hashes()?;
        match &self.shape {
            ShapedData::Scalar(m) => encode_value(key, &ValueContent::Scalar(m)),
            ShapedData::Array(values) => encode_value(
                key,
                &ValueContent::Array {
                    axis_hashes: &hashes,
                    values,
                },
            ),
            ShapedData::Long(rows) => {
                let cells: Vec<(Vec<i32>, ScalarValue)> = rows
                    .iter()
                    .map(|r| (r.positions.clone(), r.value.clone()))
                    .collect();
                encode_value(
                    key,
                    &ValueContent::Long {
                        value_kind: self.value_kind,
                        axis_hashes: &hashes,
                        cells: &cells,
                    },
                )
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CanonError {
    #[error("expected {} columns (rank {rank} + value), got {got}", rank + 1)]
    ColumnCount { rank: usize, got: usize },
    #[error("column {0} has the wrong Arrow type for its declared kind")]
    ColumnType(usize),
    #[error("null index coordinate in dimension {0} (index positions must be present)")]
    NullCoord(usize),
    #[error("missing ordered domain for label dimension at position {0}")]
    MissingDomain(u8),
    #[error("label {0:?} in dimension {1} is not in the caller-supplied ordered domain")]
    LabelNotInDomain(String, usize),
    /// Two rows of one value share an index tuple. `coords` is the tuple as
    /// the caller wrote it (labels quoted, integers, ISO timestamps), not the
    /// internal axis positions; `location` names the value (entity, class,
    /// parameter, alternative) once the write path knows it
    /// ([`CanonError::at_value`]).
    #[error("{}", duplicate_tuple_message(coords, location.as_deref()))]
    DuplicateTuple {
        coords: Vec<String>,
        location: Option<String>,
    },
    #[error(
        "the label domain of dimension {position}{} repeats the label {label:?}; \
         each label may appear only once in a domain",
        dimension.as_ref().map(|n| format!(" ({n:?})")).unwrap_or_default()
    )]
    RepeatedDomainLabel {
        label: String,
        position: u8,
        dimension: Option<String>,
    },
    #[error("rank-0 scalar must have exactly one non-null value, got {0}")]
    ScalarRows(usize),
    #[error("indexed value has no non-null cells after NULL elimination")]
    EmptyValue,
    #[error("index kind {0:?} is not yet derivable from long input (period/time_pattern)")]
    UnsupportedKind(IndexKind),
    #[error("value_kind {0:?} is not yet supported as a pipeline value column")]
    UnsupportedValueKind(ValueKind),
    #[error(
        "timestamp {value} µs in column {column} is outside the supported range \
         (290309 BC to 294247 AD)"
    )]
    TimestampOutOfRange { value: i64, column: usize },
    #[error(transparent)]
    Encode(#[from] EncodeError),
}

impl CanonError {
    /// Attach the value's location to an error that needs it to be found in
    /// the source data (today [`CanonError::DuplicateTuple`]); every other
    /// variant passes through unchanged.
    pub fn at_value(self, entity: &str, class: &str, parameter: &str, alternative: &str) -> Self {
        match self {
            CanonError::DuplicateTuple { coords, .. } => CanonError::DuplicateTuple {
                coords,
                location: Some(format!(
                    "entity {entity:?} (class {class:?}, parameter {parameter:?}, \
                     alternative {alternative:?})"
                )),
            },
            other => other,
        }
    }
}

fn duplicate_tuple_message(coords: &[String], location: Option<&str>) -> String {
    let tuple = coords.join(", ");
    match location {
        Some(loc) => format!(
            "{loc}: duplicate index tuple ({tuple}); a value maps one measure per index tuple"
        ),
        None => {
            format!("duplicate index tuple ({tuple}); a value maps one measure per index tuple")
        }
    }
}

/// A timestamp (µs since the Unix epoch) as ISO 8601 (`YYYY-MM-DDTHH:MM:SS`,
/// with `.ffffff` only when there are sub-second digits). Diagnostics only.
fn render_timestamp(us: i64) -> String {
    const DAY_US: i64 = 86_400_000_000;
    let (y, m, d) = crate::encoding::civil_from_days(us.div_euclid(DAY_US));
    let rem = us.rem_euclid(DAY_US);
    let (secs, micros) = (rem / 1_000_000, rem % 1_000_000);
    let (h, mi, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if micros == 0 {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{micros:06}")
    }
}

/// Reject a label domain that repeats a label. A label's ordinal is its
/// place in the domain, so a repeat makes the order ambiguous; it is almost
/// always a producer bug. `dim_name` gives the dimension name of a domain
/// position, for the message. The write path runs this once per segment,
/// before any value is canonicalized.
pub fn check_domain_labels(
    domains: &[LabelDomain],
    dim_name: impl Fn(u8) -> Option<String>,
) -> Result<(), CanonError> {
    for d in domains {
        let mut seen: HashSet<&str> = HashSet::with_capacity(d.labels.len());
        if let Some(label) = d.labels.iter().find(|l| !seen.insert(l.as_str())) {
            return Err(CanonError::RepeatedDomainLabel {
                label: label.clone(),
                position: d.position,
                dimension: dim_name(d.position),
            });
        }
    }
    Ok(())
}

/// One dimension column, downcast once before the row loop.
enum DimCol<'a> {
    Ts(&'a TimestampMicrosecondArray),
    Int(&'a Int64Array),
    Label(&'a StringArray),
}

/// One raw index coordinate (pre-axis-derivation).
#[derive(Clone, PartialEq)]
enum RawCoord {
    Num(i64),
    Label(String),
}

/// The value column, downcast once before the row loop.
enum ValCol<'a> {
    Float(&'a Float64Array),
    Int(&'a Int64Array),
    Bool(&'a BooleanArray),
    Str(&'a StringArray),
    DateTime(&'a TimestampMicrosecondArray),
}

/// Run the four-step pipeline. `domains` supplies the ordered domain of every
/// `label` dimension; datetime/integer dimensions ignore it.
pub fn canonicalize(
    batch: &RecordBatch,
    declared: &DeclaredType,
    value_kind: ValueKind,
    domains: &[LabelDomain],
) -> Result<CanonicalValue, CanonError> {
    let rank = declared.rank();
    if batch.num_columns() != rank + 1 {
        return Err(CanonError::ColumnCount {
            rank,
            got: batch.num_columns(),
        });
    }

    // Downcast the dimension columns per declared kind.
    let mut dim_cols: Vec<DimCol> = Vec::with_capacity(rank);
    for (i, (_, kind)) in declared.positions.iter().enumerate() {
        let col = batch.column(i);
        let dc = match kind {
            IndexKind::Datetime => DimCol::Ts(
                col.as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or(CanonError::ColumnType(i))?,
            ),
            IndexKind::Integer => DimCol::Int(
                col.as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(CanonError::ColumnType(i))?,
            ),
            IndexKind::Label => DimCol::Label(
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(CanonError::ColumnType(i))?,
            ),
            other => return Err(CanonError::UnsupportedKind(*other)),
        };
        dim_cols.push(dc);
    }

    // Downcast the value column per value_kind.
    let vcol = batch.column(rank);
    let value_col = match value_kind {
        ValueKind::Float => ValCol::Float(
            vcol.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or(CanonError::ColumnType(rank))?,
        ),
        ValueKind::Int => ValCol::Int(
            vcol.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(CanonError::ColumnType(rank))?,
        ),
        ValueKind::Bool => ValCol::Bool(
            vcol.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or(CanonError::ColumnType(rank))?,
        ),
        ValueKind::String => ValCol::Str(
            vcol.as_any()
                .downcast_ref::<StringArray>()
                .ok_or(CanonError::ColumnType(rank))?,
        ),
        ValueKind::DateTime => ValCol::DateTime(
            vcol.as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or(CanonError::ColumnType(rank))?,
        ),
        ValueKind::Duration => return Err(CanonError::UnsupportedValueKind(value_kind)),
    };

    // --- Step 1: NULL elimination (NULL ≡ absent) + read surviving cells. ---
    let n = batch.num_rows();
    let mut kept: Vec<(Vec<RawCoord>, ScalarValue)> = Vec::new();
    for row in 0..n {
        let measure = match read_value(&value_col, row, rank)? {
            Some(m) => m,
            None => continue, // NULL value tuple is absent (VMP Part 3 step 1).
        };
        let mut coords = Vec::with_capacity(rank);
        for (i, dc) in dim_cols.iter().enumerate() {
            coords.push(read_coord(dc, row, i)?);
        }
        kept.push((coords, measure));
    }

    if rank == 0 {
        if kept.len() != 1 {
            return Err(CanonError::ScalarRows(kept.len()));
        }
        let (_, m) = kept.into_iter().next().unwrap();
        return Ok(CanonicalValue {
            tier: Tier::Scalar,
            value_kind,
            axes: Vec::new(),
            shape: ShapedData::Scalar(m),
        });
    }
    if kept.is_empty() {
        return Err(CanonError::EmptyValue);
    }

    // --- Step 2: derive one axis per dimension + a coord -> position map. ---
    let mut axes: Vec<CanonicalAxis> = Vec::with_capacity(rank);
    let mut num_pos: Vec<HashMap<i64, i32>> = Vec::new();
    let mut label_pos: Vec<HashMap<String, i32>> = Vec::new();
    // Parallel bookkeeping so every dimension has an entry at its own index.
    let mut is_label = vec![false; rank];
    // Sorted distinct coords of each numeric dimension (position -> coord),
    // kept to report a duplicate tuple in the caller's coordinates.
    let mut num_coords: Vec<Vec<i64>> = Vec::with_capacity(rank);

    for (i, (position, kind)) in declared.positions.iter().enumerate() {
        match kind {
            IndexKind::Datetime | IndexKind::Integer => {
                let mut distinct: Vec<i64> = kept
                    .iter()
                    .map(|(c, _)| match &c[i] {
                        RawCoord::Num(v) => *v,
                        RawCoord::Label(_) => unreachable!("numeric dim holds a label"),
                    })
                    .collect();
                distinct.sort_unstable();
                distinct.dedup();
                let mut map = HashMap::with_capacity(distinct.len());
                for (pos, v) in distinct.iter().enumerate() {
                    map.insert(*v, pos as i32);
                }
                let body = if *kind == IndexKind::Datetime {
                    derive_datetime_axis(&distinct)
                } else {
                    derive_integer_axis(&distinct)
                };
                axes.push(CanonicalAxis {
                    length: distinct.len() as i32,
                    properties: Default::default(),
                    body,
                });
                num_pos.push(map);
                label_pos.push(HashMap::new());
                num_coords.push(distinct);
            }
            IndexKind::Label => {
                is_label[i] = true;
                let domain = domains
                    .iter()
                    .find(|d| d.position == *position)
                    .ok_or(CanonError::MissingDomain(*position))?;
                // distinct present labels, each with its domain ordinal.
                let mut present: Vec<(i32, String)> = Vec::new();
                let mut seen: HashMap<String, ()> = HashMap::new();
                for (c, _) in &kept {
                    let lab = match &c[i] {
                        RawCoord::Label(s) => s,
                        RawCoord::Num(_) => unreachable!("label dim holds a number"),
                    };
                    if seen.insert(lab.clone(), ()).is_none() {
                        let ord = domain
                            .labels
                            .iter()
                            .position(|d| d == lab)
                            .ok_or_else(|| CanonError::LabelNotInDomain(lab.clone(), i))?;
                        present.push((ord as i32, lab.clone()));
                    }
                }
                // Canonical order: ascending domain ordinal -> dense pos 0..k.
                present.sort_by_key(|a| a.0);
                let mut map = HashMap::with_capacity(present.len());
                let mut positions = Vec::with_capacity(present.len());
                for (pos, (ord, lab)) in present.iter().enumerate() {
                    map.insert(lab.clone(), pos as i32);
                    positions.push(LabelPos {
                        ordinal: *ord,
                        label: lab.clone(),
                    });
                }
                axes.push(CanonicalAxis {
                    length: positions.len() as i32,
                    properties: Default::default(),
                    body: AxisBody::Label { positions },
                });
                num_pos.push(HashMap::new());
                label_pos.push(map);
                num_coords.push(Vec::new());
            }
            other => return Err(CanonError::UnsupportedKind(*other)),
        }
    }

    // Map each surviving cell's raw coords to axis positions.
    let mut rows: Vec<LongRow> = Vec::with_capacity(kept.len());
    for (coords, m) in kept {
        let mut positions = Vec::with_capacity(rank);
        for (i, c) in coords.iter().enumerate() {
            let p = if is_label[i] {
                match c {
                    RawCoord::Label(s) => label_pos[i][s],
                    RawCoord::Num(_) => unreachable!(),
                }
            } else {
                match c {
                    RawCoord::Num(v) => num_pos[i][v],
                    RawCoord::Label(_) => unreachable!(),
                }
            };
            positions.push(p);
        }
        rows.push(LongRow {
            positions,
            value: m,
        });
    }

    // --- Step 3: sort row-major in declared-dimension order (lexicographic on
    // the position tuple), then reject duplicate index tuples. ---
    rows.sort_by(|a, b| a.positions.cmp(&b.positions));
    for w in rows.windows(2) {
        if w[0].positions == w[1].positions {
            return Err(CanonError::DuplicateTuple {
                coords: render_tuple(&w[0].positions, declared, &axes, &num_coords),
                location: None,
            });
        }
    }

    // --- Step 4: tier. ---
    let axis_lengths: Vec<usize> = axes.iter().map(|a| a.length as usize).collect();
    let (tier, shape) = tier::decide(&axis_lengths, value_kind, &rows);

    Ok(CanonicalValue {
        tier,
        value_kind,
        axes,
        shape,
    })
}

/// A position tuple back in the caller's coordinates: labels quoted,
/// integers as written, datetimes as ISO 8601.
fn render_tuple(
    positions: &[i32],
    declared: &DeclaredType,
    axes: &[CanonicalAxis],
    num_coords: &[Vec<i64>],
) -> Vec<String> {
    positions
        .iter()
        .enumerate()
        .map(|(i, &p)| match (&declared.positions[i].1, &axes[i].body) {
            (_, AxisBody::Label { positions }) => format!("{:?}", positions[p as usize].label),
            (IndexKind::Datetime, _) => render_timestamp(num_coords[i][p as usize]),
            _ => num_coords[i][p as usize].to_string(),
        })
        .collect()
}

/// Read one value cell; `Ok(None)` is a NULL cell (absent). `column` is the
/// value column's index, for the range error.
fn read_value(col: &ValCol, row: usize, column: usize) -> Result<Option<ScalarValue>, CanonError> {
    Ok(match col {
        // The one storage-normalization point for value floats: -0.0 -> +0.0,
        // every NaN -> the one quiet NaN (the encoder's own definition), so
        // what is stored is exactly what is hashed.
        ValCol::Float(a) if !a.is_null(row) => {
            Some(ScalarValue::Float(canonical_value_f64(a.value(row))))
        }
        ValCol::Int(a) if !a.is_null(row) => Some(ScalarValue::Int(a.value(row))),
        ValCol::Bool(a) if !a.is_null(row) => Some(ScalarValue::Bool(a.value(row))),
        ValCol::Str(a) if !a.is_null(row) => Some(ScalarValue::Str(a.value(row).to_string())),
        ValCol::DateTime(a) if !a.is_null(row) => {
            Some(ScalarValue::DateTime(checked_ts(a.value(row), column)?))
        }
        _ => None,
    })
}

/// Reject a timestamp outside the store's range (`TS_MIN_US..=TS_MAX_US`).
fn checked_ts(us: i64, column: usize) -> Result<i64, CanonError> {
    if timestamp_in_range(us) {
        Ok(us)
    } else {
        Err(CanonError::TimestampOutOfRange { value: us, column })
    }
}

fn read_coord(dc: &DimCol, row: usize, dim: usize) -> Result<RawCoord, CanonError> {
    match dc {
        DimCol::Ts(a) => {
            if a.is_null(row) {
                return Err(CanonError::NullCoord(dim));
            }
            Ok(RawCoord::Num(checked_ts(a.value(row), dim)?))
        }
        DimCol::Int(a) => {
            if a.is_null(row) {
                return Err(CanonError::NullCoord(dim));
            }
            Ok(RawCoord::Num(a.value(row)))
        }
        DimCol::Label(a) => {
            if a.is_null(row) {
                return Err(CanonError::NullCoord(dim));
            }
            Ok(RawCoord::Label(a.value(row).to_string()))
        }
    }
}

/// Derive a datetime axis body, parametric-first (VMP Part 3 step 2, spec §7.1).
fn derive_datetime_axis(coords_sorted: &[i64]) -> AxisBody {
    match datetime_candidate_step(coords_sorted) {
        Some(step) => AxisBody::DatetimeParametric {
            start_ts: coords_sorted[0],
            step,
        },
        None => AxisBody::DatetimeMaterialized {
            coords: coords_sorted.to_vec(),
        },
    }
}

/// The datetime `(months, micros)` step derivation (canon owns this; spec
/// §7.1.2 verification is `encoding::datetime_reproduces`). Candidate priority,
/// deterministic:
///   1. pure micros `(0, c[1]-c[0])` — the exact-µs progression (hourly, daily,
///      any fixed-µs grid). Tried first so "no day unit / prefer exact micros"
///      holds: a 24 h grid is micros, never `(months=1..)`.
///   2. month-bearing `(m, r)` where `m` is the civil-month delta c[0]->c[1] and
///      `r = c[1] - reproduce(c[0], (m, 0))` is the sub-month residual (0 for a
///      clean monthly/yearly grid).
///
/// The first candidate that reproduces EVERY coordinate (via the shared clamp
/// arithmetic) wins; if neither does, the axis materializes. A 2-point axis is
/// always reproduced by candidate 1, so it canonicalizes to a micros step —
/// deterministic, and the ambiguity is inherent at 2 points.
fn datetime_candidate_step(coords: &[i64]) -> Option<Duration> {
    if coords.len() < 2 {
        // A length-1 axis is trivially parametric; canonical step is zero.
        return Some(Duration {
            months: 0,
            micros: 0,
        });
    }
    let start = coords[0];

    // Candidate 1: pure micros.
    let micros = coords[1].checked_sub(coords[0])?;
    let c1 = Duration { months: 0, micros };
    if datetime_reproduces(start, &c1, coords) {
        return Some(c1);
    }

    // Candidate 2: month-bearing. Propose m from the civil-month delta; the
    // residual and the reproduce check both go through the shared clamp math.
    let (y0, m0, _) = crate::encoding::ymd_from_micros(coords[0]);
    let (y1, m1, _) = crate::encoding::ymd_from_micros(coords[1]);
    let months = (y1 - y0) * 12 + (m1 as i64 - m0 as i64);
    if let Ok(months_i32) = i32::try_from(months) {
        if months_i32 != 0 {
            let after = datetime_coord(
                start,
                &Duration {
                    months: months_i32,
                    micros: 0,
                },
                1,
            )?;
            let residual = coords[1].checked_sub(after)?;
            let c2 = Duration {
                months: months_i32,
                micros: residual,
            };
            if datetime_reproduces(start, &c2, coords) {
                return Some(c2);
            }
        }
    }
    None
}

/// Derive an integer axis body, parametric-first (spec §7.1.2.3).
fn derive_integer_axis(coords_sorted: &[i64]) -> AxisBody {
    match integer_regular_step(coords_sorted) {
        Some(step) => AxisBody::IntegerParametric {
            start: coords_sorted[0],
            step,
        },
        None => AxisBody::IntegerMaterialized {
            coords: coords_sorted.to_vec(),
        },
    }
}
