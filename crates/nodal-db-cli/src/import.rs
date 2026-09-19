//! `nodal-db import` — the Arrow-IPC bulk-format importer
//! (`spec/arrow_ipc_bulk_format.md`, implemented exactly; error classes E1–E10
//! per §9.1). The importer is a thin adapter: parse container → validate
//! metadata → structurally normalize (dictionary decode, s/ms→µs timestamp
//! upcast, dense→long expansion) → ONE `core::ingest_segments` call. It NEVER
//! re-canonicalizes (CLAUDE.md invariant 2): every transformation here is
//! structural (unit arithmetic on the same instant, order-preserving
//! coordinate pairing), never an interpretation of the data.

use std::collections::HashSet;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use nodal_db_core::api::{ingest_segments, Segment};
use nodal_db_core::error::DbError;
use nodal_db_core::reconstruct::expand_dense_row;
use nodal_db_core::types::{Tier, WarningKind, WriteOptions};

/// Compile-time proof (risk R2) that the pinned direct `arrow` and the
/// `duckdb::arrow` re-export are ONE crate: an IPC-read batch crosses into the
/// core's duckdb::arrow-typed signature with no conversion. If the versions
/// ever diverge, this function stops compiling — the intended tripwire.
#[allow(dead_code)]
fn arrow_versions_unify(
    b: arrow::record_batch::RecordBatch,
) -> nodal_db_core::duckdb::arrow::record_batch::RecordBatch {
    b
}

/// Schema-level metadata keys this reader knows (§4.1); any other `spinal:`
/// key is warn-and-ignore (E5, §3.2).
const KNOWN_SCHEMA_KEYS: [&str; 7] = [
    "spinal:format",
    "spinal:format_version",
    "spinal:segment_count",
    "spinal:class",
    "spinal:parameter",
    "spinal:alternative",
    "spinal:form",
];
/// Field-level metadata keys this reader knows (§7.1).
const KNOWN_FIELD_KEYS: [&str; 1] = ["spinal:label_domain"];

pub struct ImportArgs {
    pub file: PathBuf,
    pub db: PathBuf,
    pub alternative: Option<String>,
    pub class: Option<String>,
    pub parameter: Option<String>,
    pub user: Option<String>,
    pub message: String,
    pub force_replace: bool,
}

pub fn run(args: &ImportArgs) -> Result<()> {
    // The commit author is a CLI usage concern, resolved before the container
    // is even read (critique F5): a missing author is not a payload error and
    // must not wear an E-class after a possibly-large decode.
    let user = resolve_user(args)?;

    let bytes = std::fs::read(&args.file)
        .map_err(|e| anyhow!("cannot read {}: {e}", args.file.display()))?;
    // E1: not an IPC stream — empty file or the IPC *file* (Feather) magic.
    if bytes.is_empty() {
        bail!("E1: {} is empty (0 bytes) — not an Arrow IPC stream", args.file.display());
    }
    if bytes.len() >= 6 && &bytes[..6] == b"ARROW1" {
        bail!(
            "E1: {} is in the Arrow IPC *file* (Feather / random-access) format; \
             this importer accepts only the IPC *stream* format (spec §2.1)",
            args.file.display()
        );
    }

    let raws = split_segments(&bytes)?;

    // Per-segment metadata validation (E2–E5), then container-level integrity:
    // version agreement (E3), segment_count agreement + actual-count (E10),
    // duplicate targets (E7). All validation is front-loaded (§9.2) — no DB is
    // touched until the whole container has been validated and normalized.
    let metas: Vec<SegMeta> = raws
        .iter()
        .map(|r| parse_meta(r.ordinal, &r.schema))
        .collect::<Result<_>>()?;

    let first_version = metas[0].version;
    let first_count = metas[0].segment_count;
    for m in &metas {
        if m.version != first_version {
            bail!(
                "E3: segments disagree on spinal:format_version ({}.{} vs {}.{})",
                first_version.0, first_version.1, m.version.0, m.version.1
            );
        }
        if m.segment_count != first_count {
            bail!(
                "E10: segments disagree on spinal:segment_count ({first_count} vs {})",
                m.segment_count
            );
        }
    }
    if first_count != raws.len() as u64 {
        bail!(
            "E10: container holds {} segment(s) but every segment declares \
             spinal:segment_count = {first_count} — truncation or mis-assembly suspected",
            raws.len()
        );
    }

    // E7: duplicate (class, parameter, alternative) target, case-insensitive
    // (§4.2 — core name resolution folds via DuckDB lower(), which is Unicode;
    // alternative names carry no ASCII charset CHECK, so the fold here must be
    // Unicode too or Ä/ä-aliased targets slip past — critique F3).
    let mut targets: HashSet<(String, String, String)> = HashSet::new();
    let mut segments: Vec<Segment> = Vec::with_capacity(raws.len());
    for (raw, meta) in raws.iter().zip(&metas) {
        let alternative = effective_alternative(raw.ordinal, meta, args)?;
        assert_flags(raw.ordinal, meta, args)?;
        if !targets.insert((
            meta.class.to_lowercase(),
            meta.parameter.to_lowercase(),
            alternative.to_lowercase(),
        )) {
            bail!(
                "E7: duplicate target {}:{} (alternative {:?}) across segments \
                 (comparison is case-insensitive)",
                meta.class, meta.parameter, alternative
            );
        }
        let batches = normalize_segment(raw, meta)?;
        segments.push(Segment {
            class: meta.class.clone(),
            parameter: meta.parameter.clone(),
            alternative,
            batches,
            // Domains ride exclusively in `spinal:label_domain` field metadata
            // (§7.1) — the in-process arg is empty by spec.
            domains: vec![],
        });
    }

    let opts = WriteOptions {
        force_replace: args.force_replace,
        retain_override: None,
    };
    let n_segments = segments.len();
    let report = ingest_segments(&args.db, segments, &user, &args.message, opts).map_err(
        |e| match e {
            // E9: busy / conflict budget exhausted at commit.
            DbError::Busy { .. } | DbError::Conflict { .. } => anyhow!("E9: {e}"),
            // Everything else the core rejects is the E6 malformed-payload
            // class (core-surfaced canonicalization errors, §9.1).
            other => anyhow!("E6: {other}"),
        },
    )?;

    // Success summary + warnings (warnings MUST be printed — decision B).
    let tiers: Vec<&str> = report
        .tiers
        .iter()
        .map(|t| match t {
            Tier::Scalar => "scalar",
            Tier::Array => "array",
            Tier::Long => "long",
        })
        .collect();
    println!(
        "imported {}: commit {}, {} segment(s), {} value(s), {} row(s) written, \
         axes {} created / {} reused, tiers [{}]",
        args.file.display(),
        report.commit_id,
        n_segments,
        report.values_written,
        report.rows_written,
        report.axes_created,
        report.axes_reused,
        tiers.join(", ")
    );
    for w in &report.warnings {
        match &w.kind {
            WarningKind::AutoCreated => {
                println!("warning: auto-created {} {:?} (id {})", w.object_type, w.name, w.id)
            }
            WarningKind::TimezoneNormalized { column, tz } => println!(
                "warning: timezone-normalized column {column:?} (tz {tz}); \
                 stored as the UTC-µs instants unchanged"
            ),
            // Import never issues deletes, but the match is exhaustive: surface
            // these consistently if the shared write path ever emits one.
            WarningKind::UnknownReferent { key_index, level } => println!(
                "warning: unknown {level} for delete key {key_index} (skipped)"
            ),
            WarningKind::NothingToDelete { key_index } => println!(
                "warning: nothing to delete for key {key_index} (no live value)"
            ),
        }
    }
    Ok(())
}

/// Resolve the commit author (spec §8.2): `--user` if given (must be
/// non-blank), else the first of `USER`/`USERNAME`/`LOGNAME` whose value is
/// non-blank — a set-but-empty variable falls through (critique F5). No source
/// at all is a plain CLI usage error, not an E-class payload error.
/// `pub(crate)`: the ONE author-resolution rule for every verb (`import`,
/// `template`, `init --template`).
pub(crate) fn resolve_user_from(explicit: Option<&str>) -> Result<String> {
    if let Some(u) = explicit {
        let u = u.trim();
        if u.is_empty() {
            bail!("--user must be non-empty");
        }
        return Ok(u.to_string());
    }
    ["USER", "USERNAME", "LOGNAME"]
        .iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .ok_or_else(|| {
            anyhow!(
                "no commit author: --user was not given and none of USER/USERNAME/LOGNAME \
                 is set to a non-blank value (pass --user <name>)"
            )
        })
}

fn resolve_user(args: &ImportArgs) -> Result<String> {
    resolve_user_from(args.user.as_deref())
}

// ---------------------------------------------------------------------------
// Container framing: segment loop with per-segment EOS verification (§2.2/.1)
// ---------------------------------------------------------------------------

struct RawSegment {
    ordinal: usize,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

fn split_segments(bytes: &[u8]) -> Result<Vec<RawSegment>> {
    let mut out: Vec<RawSegment> = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let ordinal = out.len() + 1;
        // Plain (unbuffered) StreamReader: `try_new_buffered` reads ahead past
        // the segment boundary and breaks the resume-at-position loop (§2.2).
        let mut cursor = Cursor::new(&bytes[pos..]);
        let reader = StreamReader::try_new(&mut cursor, None).map_err(|e| {
            if ordinal == 1 {
                anyhow!("E1: not an Arrow IPC stream: {e}")
            } else {
                anyhow!(
                    "E6: trailing bytes after segment {} do not begin a valid IPC stream: {e}",
                    ordinal - 1
                )
            }
        })?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| anyhow!("E6: segment {ordinal}: {e}"))?);
        }
        let consumed = cursor.position() as usize;
        let end = pos + consumed;
        // Truncation defense (§2.2.1): every stock reader treats a bare EOF at
        // a message boundary as a clean end of stream, so EOS-terminated must
        // be distinguished from bare-EOF-terminated here — by walking the
        // message frames, never by comparing raw trailing bytes: a batch body
        // whose last 8 bytes coincidentally equal the EOS sequence must not
        // pass for a terminator (critique F1).
        match classify_termination(&bytes[pos..end])
            .map_err(|e| anyhow!("E6: segment {ordinal}: {e}"))?
        {
            Termination::Eos => {}
            Termination::LegacyEos => bail!(
                "E10: segment {ordinal} ends with the legacy (pre-Arrow-0.15) 4-byte \
                 end-of-stream marker, not the 8-byte marker the format requires \
                 (§2.2.1) — likely a legacy-format IPC stream; rewrite it with a \
                 modern Arrow writer"
            ),
            Termination::BareEof => bail!(
                "E10: segment {ordinal} is not terminated by the end-of-stream marker \
                 — truncation suspected (producer crash before close?)"
            ),
        }
        pos = end;
        out.push(RawSegment { ordinal, schema, batches });
    }
    Ok(out)
}

/// How the byte range a `StreamReader` consumed terminates (§2.2.1).
enum Termination {
    /// A genuine 8-byte end-of-stream frame (continuation sentinel + zero
    /// metadata length) exactly at the reader's stop position.
    Eos,
    /// A pre-Arrow-0.15 4-byte EOS frame (zero length, no continuation prefix)
    /// at the stop position — a well-formed *legacy* stream, which the format
    /// nevertheless rejects (the 8-byte marker is normative).
    LegacyEos,
    /// The range ends at a message boundary with no EOS frame: bare-EOF
    /// truncation (producer crash before close).
    BareEof,
}

/// Walk the IPC message frames of one consumed segment range and classify its
/// termination. Each frame is a 4-byte continuation sentinel + 4-byte metadata
/// length (legacy: length only), the flatbuffer metadata, then
/// `Message.bodyLength()` body bytes — so the walk lands exactly on every
/// message boundary the reader saw, and only a real EOS *frame* at the stop
/// position counts as termination (critique F1).
fn classify_termination(seg: &[u8]) -> Result<Termination> {
    const CONT: [u8; 4] = [0xff; 4];
    let mut pos = 0usize;
    loop {
        if pos + 4 > seg.len() {
            return Ok(Termination::BareEof);
        }
        let word: [u8; 4] = seg[pos..pos + 4].try_into().unwrap();
        let (meta_len, header) = if word == CONT {
            if pos + 8 > seg.len() {
                return Ok(Termination::BareEof);
            }
            (i32::from_le_bytes(seg[pos + 4..pos + 8].try_into().unwrap()), 8usize)
        } else {
            // Pre-0.15 framing: the length arrives without a continuation
            // sentinel (arrow-rs reads both; the format accepts only modern).
            (i32::from_le_bytes(word), 4usize)
        };
        if meta_len == 0 {
            // An EOS frame — genuine only when it sits exactly at the stop
            // position (an EOS the reader read past cannot terminate anything).
            return Ok(if pos + header == seg.len() {
                if header == 8 { Termination::Eos } else { Termination::LegacyEos }
            } else {
                Termination::BareEof
            });
        }
        let meta_len = usize::try_from(meta_len)
            .map_err(|_| anyhow!("bad metadata length {meta_len} in IPC frame at byte {pos}"))?;
        let meta_end = pos
            .checked_add(header + meta_len)
            .filter(|&e| e <= seg.len())
            .ok_or_else(|| anyhow!("IPC frame at byte {pos} overruns the segment"))?;
        let msg = arrow::ipc::root_as_message(&seg[pos + header..meta_end])
            .map_err(|e| anyhow!("invalid IPC message flatbuffer at byte {pos}: {e}"))?;
        let body_len = usize::try_from(msg.bodyLength())
            .map_err(|_| anyhow!("negative bodyLength in IPC message at byte {pos}"))?;
        pos = meta_end
            .checked_add(body_len)
            .filter(|&e| e <= seg.len())
            .ok_or_else(|| anyhow!("IPC message body at byte {pos} overruns the segment"))?;
    }
}

// ---------------------------------------------------------------------------
// Segment schema metadata (§3, §4)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Form {
    Long,
    Dense,
}

struct SegMeta {
    class: String,
    parameter: String,
    alternative: Option<String>,
    form: Form,
    version: (u32, u32),
    segment_count: u64,
}

fn parse_meta(ordinal: usize, schema: &Schema) -> Result<SegMeta> {
    let md = schema.metadata();

    // E2: format id.
    match md.get("spinal:format").map(String::as_str) {
        Some("spinal-db-bulk") => {}
        Some(other) => bail!("E2: segment {ordinal}: spinal:format is {other:?}, expected \"spinal-db-bulk\""),
        None => bail!("E2: segment {ordinal}: spinal:format metadata key is missing"),
    }

    // E3: version (MAJOR.MINOR; reader supports major 1, any minor — §3.2).
    let ver = md
        .get("spinal:format_version")
        .ok_or_else(|| anyhow!("E3: segment {ordinal}: spinal:format_version is missing"))?;
    let version = parse_version(ver)
        .map_err(|e| anyhow!("E3: segment {ordinal}: bad spinal:format_version {ver:?}: {e}"))?;
    if version.0 != 1 {
        bail!(
            "E3: segment {ordinal}: format version {ver} has major {} but this reader \
             supports major 1 only — no fallback parsing is attempted (§3.2)",
            version.0
        );
    }

    // Required keys (E4) + segment_count value sanity (E10).
    let count_raw = md
        .get("spinal:segment_count")
        .ok_or_else(|| anyhow!("E4: segment {ordinal}: spinal:segment_count is missing"))?;
    let segment_count: u64 = match count_raw.parse() {
        Ok(n) if n >= 1 => n,
        _ => bail!(
            "E10: segment {ordinal}: spinal:segment_count {count_raw:?} is not a positive integer"
        ),
    };
    let class = md
        .get("spinal:class")
        .ok_or_else(|| anyhow!("E4: segment {ordinal}: spinal:class is missing"))?
        .clone();
    let parameter = md
        .get("spinal:parameter")
        .ok_or_else(|| anyhow!("E4: segment {ordinal}: spinal:parameter is missing"))?
        .clone();
    let form = match md.get("spinal:form").map(String::as_str) {
        Some("long") => Form::Long,
        Some("dense") => Form::Dense,
        Some(other) => bail!(
            "E4: segment {ordinal} ({class}:{parameter}): unknown spinal:form {other:?} \
             (this reader supports long | dense; a newer minor version may add forms — §3.2)"
        ),
        None => bail!("E4: segment {ordinal} ({class}:{parameter}): spinal:form is missing"),
    };
    let alternative = md.get("spinal:alternative").cloned();

    // E5 (warning, not error): unknown spinal:-namespaced keys; keys outside
    // the namespace are foreign tools' business and ignored silently.
    for key in md.keys() {
        if key.starts_with("spinal:") && !KNOWN_SCHEMA_KEYS.contains(&key.as_str()) {
            println!("warning: segment {ordinal}: ignoring unknown metadata key {key:?} (E5)");
        }
    }
    for field in schema.fields() {
        for key in field.metadata().keys() {
            if key.starts_with("spinal:") && !KNOWN_FIELD_KEYS.contains(&key.as_str()) {
                println!(
                    "warning: segment {ordinal}: ignoring unknown metadata key {key:?} \
                     on field {:?} (E5)",
                    field.name()
                );
            }
        }
    }

    Ok(SegMeta { class, parameter, alternative, form, version, segment_count })
}

/// `MAJOR.MINOR`, decimal, no leading zeros (§3.1).
fn parse_version(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        bail!("expected MAJOR.MINOR");
    }
    let mut nums = [0u32; 2];
    for (i, p) in parts.iter().enumerate() {
        if p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()) {
            bail!("non-decimal component {p:?}");
        }
        if p.len() > 1 && p.starts_with('0') {
            bail!("leading zero in component {p:?}");
        }
        nums[i] = p.parse()?;
    }
    Ok((nums[0], nums[1]))
}

/// §4.3: metadata if present, else the CLI flag; both present ⇒ must match
/// case-insensitively; neither ⇒ E4.
fn effective_alternative(ordinal: usize, meta: &SegMeta, args: &ImportArgs) -> Result<String> {
    match (&meta.alternative, &args.alternative) {
        (Some(m), Some(f)) => {
            // Unicode fold, matching DuckDB lower() — alternative names have no
            // ASCII charset CHECK (critique F3).
            if m.to_lowercase() != f.to_lowercase() {
                bail!(
                    "E4: segment {ordinal} ({}:{}): spinal:alternative {m:?} conflicts with \
                     --alternative {f:?} (no precedence — a conflict is always a mistake, §4.3)",
                    meta.class, meta.parameter
                );
            }
            Ok(m.clone())
        }
        (Some(m), None) => Ok(m.clone()),
        (None, Some(f)) => Ok(f.clone()),
        (None, None) => bail!(
            "E4: segment {ordinal} ({}:{}): no alternative — neither spinal:alternative \
             metadata nor --alternative was given",
            meta.class, meta.parameter
        ),
    }
}

/// `--class` / `--parameter` are assertions, not selectors (OPEN-Q3-6b): a
/// non-matching segment is an error, never skipped.
fn assert_flags(ordinal: usize, meta: &SegMeta, args: &ImportArgs) -> Result<()> {
    if let Some(c) = &args.class {
        if !c.eq_ignore_ascii_case(&meta.class) {
            bail!(
                "E4: segment {ordinal} targets class {:?} but --class {c:?} was asserted",
                meta.class
            );
        }
    }
    if let Some(p) = &args.parameter {
        if !p.eq_ignore_ascii_case(&meta.parameter) {
            bail!(
                "E4: segment {ordinal} targets parameter {:?} but --parameter {p:?} was asserted",
                meta.parameter
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Payload normalization (§5 long, §6 dense) — structural only, never
// interpretive: dictionary decode, s/ms→µs upcast, dense→long expansion.
// ---------------------------------------------------------------------------

fn normalize_segment(raw: &RawSegment, meta: &SegMeta) -> Result<Vec<RecordBatch>> {
    let ordinal = raw.ordinal;
    let tag = format!("segment {ordinal} ({}:{})", meta.class, meta.parameter);

    // A segment whose batches total zero rows is an error (§2.3): no zero-row
    // write, no metadata-only auto-create.
    let total_rows: usize = raw.batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows == 0 {
        bail!("E6: {tag}: zero rows across {} record batch(es) — nothing to import", raw.batches.len());
    }

    check_name_tripwires(&tag, &raw.schema)?;

    match meta.form {
        Form::Long => raw.batches.iter().map(|b| normalize_long_batch(&tag, b)).collect(),
        Form::Dense => raw.batches.iter().map(|b| expand_dense_batch(&tag, b)).collect(),
    }
}

/// §5.1.1: a column literally named `entity`/`value` (case-insensitively) at a
/// position other than its slot is E6 (the SHOULD-names are tripwires whenever
/// present); both names absent entirely draws a warning.
fn check_name_tripwires(tag: &str, schema: &Schema) -> Result<()> {
    let ncols = schema.fields().len();
    let mut saw_entity = false;
    let mut saw_value = false;
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case("entity") {
            saw_entity = true;
            if i != 0 {
                bail!(
                    "E6: {tag}: column named {:?} at position {i}, but the entity column \
                     must be position 0 (§5.1.1 name tripwire)",
                    field.name()
                );
            }
        }
        if field.name().eq_ignore_ascii_case("value") {
            saw_value = true;
            if i != ncols - 1 {
                bail!(
                    "E6: {tag}: column named {:?} at position {i}, but the value column \
                     must be the last (§5.1.1 name tripwire)",
                    field.name()
                );
            }
        }
    }
    if !saw_entity && !saw_value {
        println!(
            "warning: {tag}: no column named 'entity' or 'value' — columns are \
             interpreted positionally (§5.1.1)"
        );
    }
    Ok(())
}

/// Decode a dictionary-encoded column to its plain value type (§2.4); reject
/// run-end encoding (forbidden in 1.0, MUST-reject).
fn decode_encodings(tag: &str, field: &Field, col: &ArrayRef) -> Result<(Field, ArrayRef)> {
    match field.data_type() {
        DataType::RunEndEncoded(_, _) => bail!(
            "E6: {tag}: column {:?} is run-end encoded — forbidden in format 1.0 (§2.4)",
            field.name()
        ),
        DataType::Dictionary(_, value_type) => {
            let plain = cast(col, value_type)
                .map_err(|e| anyhow!("E6: {tag}: cannot decode dictionary column {:?}: {e}", field.name()))?;
            Ok((field.clone().with_data_type((**value_type).clone()), plain))
        }
        _ => Ok((field.clone(), col.clone())),
    }
}

/// The µs target type for an accepted wire timestamp (§5.2.1): `Second` and
/// `Millisecond` upcast losslessly (integer multiply on the same instant);
/// `Nanosecond` is rejected with the fix named; tz is preserved (§5.2.2).
fn microsecond_target(tag: &str, field: &Field) -> Result<Option<DataType>> {
    match field.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => bail!(
            "E6: {tag}: column {:?} is timestamp[ns]; nanosecond truncation is lossy and \
             never silent — cast to microseconds producer-side \
             (pandas: .astype(\"datetime64[us]\")) (§5.2.1)",
            field.name()
        ),
        DataType::Timestamp(unit, tz) => {
            if *unit == TimeUnit::Microsecond {
                Ok(None) // already µs
            } else {
                Ok(Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())))
            }
        }
        _ => Ok(None),
    }
}

/// Normalize one long-form batch (§5): dictionary decode, type gate for dims
/// (§5.2) and value (§5.3), s/ms→µs upcast, label-domain presence (§7.2).
/// Field metadata is preserved throughout. A NaN VALUE is a legal stored value
/// (§5.3.1) — the core canonicalizes it; only NaN axis/coordinate floats are
/// rejected, and no dimension column here is float.
fn normalize_long_batch(tag: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let ncols = batch.num_columns();
    if ncols < 2 {
        bail!("E6: {tag}: long form needs at least (entity, value) columns, got {ncols}");
    }
    let rank = ncols - 2;
    if rank > 4 {
        bail!("E6: {tag}: rank {rank} exceeds the cap of 4");
    }

    let mut fields: Vec<Field> = Vec::with_capacity(ncols);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let (field, col) =
            decode_encodings(tag, batch.schema().field(i), batch.column(i))?;
        let (field, col) = if i == 0 {
            (field, col) // entity column: the core validates Utf8 + nulls
        } else {
            // Dim or value column: gate the type, upcast timestamps.
            let is_value = i == ncols - 1;
            let ok = match field.data_type() {
                DataType::Timestamp(_, _) => true, // unit gated below
                DataType::Int64 => true,
                DataType::Utf8 => true,
                DataType::Float64 | DataType::Boolean if is_value => true,
                other => bail!(
                    "E6: {tag}: {} column {:?} has unsupported Arrow type {other:?} \
                     ({})",
                    if is_value { "value" } else { "dimension" },
                    field.name(),
                    if is_value { "§5.3" } else { "§5.2" }
                ),
            };
            debug_assert!(ok);
            if let Some(target) = microsecond_target(tag, &field)? {
                let up = cast(&col, &target)
                    .map_err(|e| anyhow!("E6: {tag}: timestamp upcast on {:?}: {e}", field.name()))?;
                (field.with_data_type(target), up)
            } else {
                (field, col)
            }
        };
        // §7.2: a label dimension without spinal:label_domain is E6.
        if i > 0 && i < ncols - 1 && field.data_type() == &DataType::Utf8
            && !field.metadata().contains_key("spinal:label_domain")
        {
            bail!(
                "E6: {tag}: label dimension {:?} has no spinal:label_domain field \
                 metadata (required on every label dimension, §7.2)",
                field.name()
            );
        }
        fields.push(field);
        cols.push(col);
    }

    // §5.3.1: a NaN VALUE is accepted — it is a real stored value the core
    // canonicalizes (to one quiet NaN), not absence (absence is a NULL cell).
    // Only NaN in an axis/coordinate float is illegal, and no dimension column
    // here is float, so nothing is rejected on the NaN axis at this boundary.

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, cols).map_err(|e| anyhow!("E6: {tag}: {e}"))
}

// ---------------------------------------------------------------------------
// Dense form (§6): one row = one entity's complete value; the importer
// expands to logical-long via the core's expand_dense_row (the write-direction
// inverse of reconstruct — same row-major law, one implementation).
// ---------------------------------------------------------------------------

/// One dense coordinate, typed. `Ts` is always µs (upcast during extraction).
#[derive(Clone, PartialEq, Eq, Hash)]
enum Coord {
    Ts(i64),
    Int(i64),
    Label(String),
}

impl Coord {
    fn describe(&self) -> String {
        match self {
            Coord::Ts(v) => format!("timestamp {v}µs"),
            Coord::Int(v) => v.to_string(),
            Coord::Label(s) => format!("{s:?}"),
        }
    }
}

/// A dense dimension's declared child kind (post-gate), with the µs factor
/// for timestamp upcast and the preserved tz.
enum DimKind {
    Ts { factor: i64, tz: Option<Arc<str>> },
    Int,
    Label,
}

fn expand_dense_batch(tag: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let ncols = batch.num_columns();
    if ncols < 3 {
        bail!(
            "E6: {tag}: a rank-0 dense segment is meaningless — use long form (§6.1); \
             got {ncols} column(s)"
        );
    }
    let rank = ncols - 2;
    if rank > 4 {
        bail!("E6: {tag}: rank {rank} exceeds the cap of 4");
    }

    // Decode top-level encodings, then gate the shapes.
    let mut fields: Vec<Field> = Vec::with_capacity(ncols);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let (f, c) = decode_encodings(tag, batch.schema().field(i), batch.column(i))?;
        fields.push(f);
        cols.push(c);
    }

    let entity = cols[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            anyhow!("E6: {tag}: dense entity column must be Utf8, got {:?}", fields[0].data_type())
        })?;

    // Dimension columns: List<T> with T per §5.2 (ns rejected, s/ms upcast);
    // label dims must carry spinal:label_domain on the OUTER field (§7.3).
    let mut dim_kinds: Vec<DimKind> = Vec::with_capacity(rank);
    for d in 0..rank {
        let field = &fields[d + 1];
        let DataType::List(child) = field.data_type() else {
            bail!(
                "E6: {tag}: dense dimension {:?} must be List<...>, got {:?} (§6.1)",
                field.name(),
                field.data_type()
            );
        };
        let kind = match child.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => bail!(
                "E6: {tag}: dimension {:?} is List<timestamp[ns]>; cast to microseconds \
                 producer-side (§5.2.1)",
                field.name()
            ),
            DataType::Timestamp(unit, tz) => DimKind::Ts {
                factor: match unit {
                    TimeUnit::Second => 1_000_000,
                    TimeUnit::Millisecond => 1_000,
                    TimeUnit::Microsecond => 1,
                    TimeUnit::Nanosecond => unreachable!(),
                },
                tz: tz.clone(),
            },
            DataType::Int64 => DimKind::Int,
            DataType::Utf8 => {
                if !field.metadata().contains_key("spinal:label_domain") {
                    bail!(
                        "E6: {tag}: label dimension {:?} has no spinal:label_domain field \
                         metadata (required, §7.2/§7.3)",
                        field.name()
                    );
                }
                DimKind::Label
            }
            other => bail!(
                "E6: {tag}: dense dimension {:?} has unsupported element type {other:?} (§5.2)",
                field.name()
            ),
        };
        dim_kinds.push(kind);
    }

    // Value column: List<Float64> only in 1.0 (§6.5).
    let value_field = &fields[ncols - 1];
    match value_field.data_type() {
        DataType::List(child) if child.data_type() == &DataType::Float64 => {}
        other => bail!(
            "E6: {tag}: dense value column must be List<Float64> in format 1.0 (§6.5), \
             got {other:?}"
        ),
    }
    let value_list = cols[ncols - 1].as_any().downcast_ref::<ListArray>().unwrap();

    // Expand row by row (one row = one entity, §6.1).
    let nrows = batch.num_rows();
    let mut seen_entities: HashSet<String> = HashSet::new();
    let mut out_entity: Vec<String> = Vec::new();
    let mut out_dims: Vec<Vec<Coord>> = vec![Vec::new(); rank];
    let mut out_values: Vec<f64> = Vec::new();

    for row in 0..nrows {
        if entity.is_null(row) {
            bail!("E6: {tag}: null entity in dense row {row} (nulls are forbidden everywhere in dense form, §6.4)");
        }
        let ent = entity.value(row).to_string();
        // §6.4: each entity at most one row per segment (folded like the core).
        if !seen_entities.insert(ent.to_ascii_lowercase()) {
            bail!("E6: {tag}: entity {ent:?} appears in more than one dense row (§6.4)");
        }

        // Per-dimension coordinate lists.
        let mut dim_lists: Vec<Vec<Coord>> = Vec::with_capacity(rank);
        for d in 0..rank {
            let list = cols[d + 1].as_any().downcast_ref::<ListArray>().unwrap();
            if list.is_null(row) {
                bail!(
                    "E6: {tag}: null {:?} dim list in dense row {row} (entity {ent:?}) (§6.4)",
                    fields[d + 1].name()
                );
            }
            let coords = extract_coords(tag, fields[d + 1].name(), &dim_kinds[d], &list.value(row), row, &ent)?;
            // A zero-length dim list would expand the whole row to zero long
            // tuples — the entity would vanish without error, violating §9.3's
            // no-silent-drop rule (critique F2). The spec never legalizes an
            // empty list; reject it as the E8 shape-law class.
            if coords.is_empty() {
                bail!(
                    "E8: {tag}: empty {:?} dim list in dense row {row} (entity {ent:?}) — \
                     an empty dim list would expand the entity to nothing; absence is \
                     expressed in long form, never by an empty dense axis (§6.2, §9.3)",
                    fields[d + 1].name()
                );
            }
            // §6.2: entries within each dim list must be distinct (E8).
            let mut seen: HashSet<&Coord> = HashSet::new();
            for c in &coords {
                if !seen.insert(c) {
                    bail!(
                        "E8: {tag}: duplicate coordinate {} in dimension {:?} of dense row \
                         {row} (entity {ent:?}) — a duplicate would expand to duplicate \
                         index tuples (§6.2)",
                        c.describe(),
                        fields[d + 1].name()
                    );
                }
            }
            dim_lists.push(coords);
        }

        // The flat payload; nulls are forbidden (a null cell would be absence,
        // which dense form cannot express — §6.4). A NaN cell, by contrast, is a
        // real stored value the core canonicalizes (§5.3.1): dense stays a
        // complete grid, so a NaN keeps the entity on the array tier.
        if value_list.is_null(row) {
            bail!("E6: {tag}: null value list in dense row {row} (entity {ent:?}) (§6.4)");
        }
        let vals_arr = value_list.value(row);
        let vals = vals_arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let mut payload: Vec<f64> = Vec::with_capacity(vals.len());
        for k in 0..vals.len() {
            if vals.is_null(k) {
                bail!(
                    "E6: {tag}: null payload cell {k} in dense row {row} (entity {ent:?}) — \
                     a null cell has no meaning in dense form; use long (§6.4)"
                );
            }
            payload.push(vals.value(k));
        }

        // Row-major expansion via the core helper (§6.3) — the same law as
        // reconstruct, by construction.
        let expanded = expand_dense_row(&dim_lists, &payload).map_err(|e| {
            anyhow!("E8: {tag}: dense row {row} (entity {ent:?}): {e} (§6.2 row-major law)")
        })?;
        for (coords, v) in expanded {
            out_entity.push(ent.clone());
            for (d, c) in coords.into_iter().enumerate() {
                out_dims[d].push(c);
            }
            out_values.push(v);
        }
    }

    // Assemble the logical-long batch: entity, dims (µs timestamps with tz
    // preserved / Int64 / Utf8 with the outer field's metadata, incl. the
    // label domain), Float64 value.
    let mut long_fields: Vec<Field> = Vec::with_capacity(ncols);
    let mut long_cols: Vec<ArrayRef> = Vec::with_capacity(ncols);
    long_fields.push(Field::new(fields[0].name(), DataType::Utf8, false));
    long_cols.push(Arc::new(StringArray::from(out_entity)) as ArrayRef);
    for (d, kind) in dim_kinds.iter().enumerate() {
        let name = fields[d + 1].name();
        let (dt, col): (DataType, ArrayRef) = match kind {
            DimKind::Ts { tz, .. } => {
                let vals: Vec<i64> = out_dims[d]
                    .iter()
                    .map(|c| match c {
                        Coord::Ts(v) => *v,
                        _ => unreachable!(),
                    })
                    .collect();
                let mut arr = TimestampMicrosecondArray::from(vals);
                if let Some(tz) = tz {
                    arr = arr.with_timezone(Arc::clone(tz));
                }
                (DataType::Timestamp(TimeUnit::Microsecond, tz.clone()), Arc::new(arr) as ArrayRef)
            }
            DimKind::Int => {
                let vals: Vec<i64> = out_dims[d]
                    .iter()
                    .map(|c| match c {
                        Coord::Int(v) => *v,
                        _ => unreachable!(),
                    })
                    .collect();
                (DataType::Int64, Arc::new(Int64Array::from(vals)) as ArrayRef)
            }
            DimKind::Label => {
                let vals: Vec<String> = out_dims[d]
                    .iter()
                    .map(|c| match c {
                        Coord::Label(s) => s.clone(),
                        _ => unreachable!(),
                    })
                    .collect();
                (DataType::Utf8, Arc::new(StringArray::from(vals)) as ArrayRef)
            }
        };
        long_fields.push(
            Field::new(name, dt, false).with_metadata(fields[d + 1].metadata().clone()),
        );
        long_cols.push(col);
    }
    long_fields.push(Field::new(fields[ncols - 1].name(), DataType::Float64, false));
    long_cols.push(Arc::new(Float64Array::from(out_values)) as ArrayRef);

    RecordBatch::try_new(Arc::new(Schema::new(long_fields)), long_cols)
        .map_err(|e| anyhow!("E6: {tag}: assembling expanded batch: {e}"))
}

/// Extract one dense dim list into typed coordinates, applying the µs upcast
/// factor and rejecting null slots (§6.4).
fn extract_coords(
    tag: &str,
    dim_name: &str,
    kind: &DimKind,
    arr: &ArrayRef,
    row: usize,
    ent: &str,
) -> Result<Vec<Coord>> {
    let n = arr.len();
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        if arr.is_null(k) {
            bail!(
                "E6: {tag}: null coordinate {k} in {dim_name:?} dim list of dense row {row} \
                 (entity {ent:?}) (§6.4)"
            );
        }
        let coord = match kind {
            DimKind::Ts { factor, .. } => {
                let raw = match *factor {
                    1_000_000 => arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(k),
                    1_000 => arr.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(k),
                    _ => arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(k),
                };
                // Checked upcast (critique F10): an overflowing ×factor would
                // wrap in release and store a garbage instant silently.
                let up = raw.checked_mul(*factor).ok_or_else(|| {
                    anyhow!(
                        "E6: {tag}: timestamp coordinate {raw} (upcast ×{factor} to µs) in \
                         {dim_name:?} dim list of dense row {row} (entity {ent:?}) overflows \
                         the microsecond timestamp range (§5.2.1)"
                    )
                })?;
                Coord::Ts(up)
            }
            DimKind::Int => Coord::Int(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(k)),
            DimKind::Label => {
                Coord::Label(arr.as_any().downcast_ref::<StringArray>().unwrap().value(k).to_string())
            }
        };
        out.push(coord);
    }
    Ok(out)
}
