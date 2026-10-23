//! The shared whole-file rebuild machinery (moved out of `compaction`
//! unchanged): the base-table catalog maps ([`TABLE_ORDER`],
//! [`GENERATED_COLUMNS`], [`CLUSTER_KEYS`]), real-column discovery and the
//! catalog drift check, the read-only source ATTACH, the sequence restore, and
//! the temp-file durability helpers.
//!
//! Two callers share it: [`crate::compaction`] (a verbatim repack of the same
//! file) and [`crate::extract`] (a history-free copy into a new file). One set
//! of maps, so a schema edit touches one module, not two. Compaction's own
//! temp naming, orphan sweep and baseline sidecar stay in `compaction`: their
//! prefixes are part of its on-disk contract.

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;

use crate::compaction::SCHEMA_VERSION;
use crate::error::DbError;

/// Base tables in a safe insertion order (§2.3). The ONLY engine-enforced FKs
/// are `→ commit(id)` and `→ axis(id)` (verified across `001–065`), so
/// **`commit` first, then `axis`, then the rest in any order** satisfies every
/// referential constraint. `value_array_hist` carries no FK by design.
pub(crate) const TABLE_ORDER: &[&str] = &[
    // FK targets first.
    "commit",
    "axis",
    // References commit only (`at_commit`, `horizon_commit`); append-only
    // erasure trace (erasure_compaction_plan §1.1) — copied verbatim like
    // every base table (§4.2: erased rows are simply not there, so compaction
    // stays copy-verbatim-and-self-check).
    "erasure_marker",
    // The rest — no remaining engine FKs to order among.
    "metadata",
    "entity_metadata",
    "parameter_value_metadata",
    "run",
    "alternative",
    "scenario",
    "scenario_alternative",
    "entity_alternative",
    "membership",
    "axis_datetime",
    "axis_period",
    "axis_label",
    "axis_integer",
    "axis_float",
    "axis_time_pattern",
    "display_mode",
    "entity_class_display_mode",
    "entity_class",
    "class_set",
    "class_set_member",
    "entity_class_dimension",
    "entity",
    "entity_element",
    "entity_location",
    "units_of_measure",
    "parameter_value_list",
    "list_value",
    "parameter_definition",
    "entity_parameter_unit",
    "parameter_type",
    "parameter_type_dimension",
    "value_scalar",
    "value_long",
    "value_array",
    "value_array_hist",
    "definition_value_scalar",
    "definition_value_long",
    "definition_value_array",
    "semantic_annotation",
];

/// The authoritative per-table `GENERATED ALWAYS AS … VIRTUAL` column set
/// (design §2.2, verified table-by-table against the DDL). These recompute from
/// the base columns and CANNOT be inserted into, so they are excluded from both
/// the copy and the self-check hash. Tables absent from this list have no
/// generated columns.
///
/// **R3 (honest drift guard):** `information_schema.columns.is_generated` is
/// NULL for VIRTUAL columns on the bundled engine, so the runtime cross-check
/// ([`check_catalog`]) can only confirm these names STILL EXIST and that the set
/// of base tables is unchanged — it cannot confirm a listed column is still
/// *generated*, nor detect a NEW generated column added within an existing
/// table. Keyed to [`SCHEMA_VERSION`]; retire this map once a bundled DuckDB
/// reliably populates the standard flag.
pub(crate) const GENERATED_COLUMNS: &[(&str, &[&str])] = &[
    ("metadata", &["cur_id", "cur_name", "cur_val"]),
    ("entity_metadata", &["cur_e", "cur_m"]),
    (
        "parameter_value_metadata",
        &["cur_e", "cur_p", "cur_a", "cur_m"],
    ),
    ("display_mode", &["cur_id", "cur_name"]),
    ("entity_class_display_mode", &["cur_dm", "cur_ec"]),
    ("alternative", &["cur_id", "cur_name"]),
    ("scenario", &["cur_id", "cur_name"]),
    ("scenario_alternative", &["cur_s", "cur_a", "cur_r"]),
    ("entity_alternative", &["cur_e", "cur_a"]),
    ("membership", &["cur_g", "cur_m", "cur_a"]),
    ("value_scalar", &["ce", "cp", "ca"]),
    ("value_long", &["ce", "cp", "ca", "c1", "c2", "c3", "c4"]),
    ("definition_value_scalar", &["cp", "cs"]),
    (
        "definition_value_long",
        &["cp", "cs", "c1", "c2", "c3", "c4"],
    ),
    ("definition_value_array", &["cp", "cs"]),
    ("entity_class", &["cur_id", "cur_name"]),
    ("class_set", &["cur_id", "cur_name"]),
    ("class_set_member", &["cur_s", "cur_c"]),
    ("entity_class_dimension", &["cur_c", "cur_p"]),
    ("entity", &["cur_id", "cur_cls", "cur_name"]),
    ("entity_element", &["cur_e", "cur_p"]),
    ("entity_location", &["cur_e"]),
    ("units_of_measure", &["cur_id", "cur_name"]),
    ("parameter_value_list", &["cur_id", "cur_name"]),
    ("list_value", &["cur_id", "cur_l", "cur_o"]),
    ("parameter_definition", &["cur_id", "cur_cls", "cur_name"]),
    ("entity_parameter_unit", &["cur_e", "cur_p"]),
    ("parameter_type", &["cur_id"]),
    ("parameter_type_dimension", &["cur_t", "cur_p"]),
    ("semantic_annotation", &["cur_id"]),
];

/// Generated columns for one table (empty slice if none).
fn generated_for(table: &str) -> &'static [&'static str] {
    GENERATED_COLUMNS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, c)| *c)
        .unwrap_or(&[])
}

/// Per-table clustering key columns for the rebuild copy's `ORDER BY`
/// (erasure_compaction_plan §4.1, decision D-E4: clustering is ALWAYS-ON).
/// The rule, applied uniformly by [`cluster_order`]:
///
///   - table has a `valid_to_commit` column (from/to tiers and all narrow
///     versioned tables; also `value_array_hist`, whose liveness expression is
///     constant-false) ⇒ `ORDER BY (valid_to_commit IS NULL) DESC, <keys>,
///     valid_from_commit` — **current-first**, then key locality, then history
///     order;
///   - otherwise (append-only tables, axis children, the all-live
///     `value_array`) ⇒ `ORDER BY <keys>` — natural/key order, locality only.
///
/// **Mechanism, stated correctly** (critique M2, empirical on DuckDB 1.5.5;
/// the compaction tests re-pass on 2.0.0, and 2.0.0 does not change list
/// reads or list-segment skipping — the liveness-skipping claim itself was not
/// re-measured on 2.0.0):
/// the current-first win is **cheap contiguous validity-vector filtering**
/// (~2× faster current-state scans on a 2 M-row/50%-dead fixture), NOT
/// row-group skipping — 1.5.5 does not use null-count segment stats to
/// eliminate row groups on `valid_to_commit IS NULL`; both layouts scan every
/// row. Min/max zone maps DO prune on the sortable key columns (verified), so
/// the key part buys genuine skipping for keyed reads. Insertion order
/// verifiably persists through CHECKPOINT and reopen. The B3 self-check is
/// order-independent (`count` + `bit_xor(hash(...))`), so clustering needs
/// zero self-check changes; determinism costs no flag surface, and the sort
/// is bounded by compaction already being a whole-file rewrite.
///
/// Every listed column is validated against the live catalog at repack time
/// ([`cluster_order`] hard-errors on a missing column), so this map cannot
/// silently drift from the DDL. Every TABLE_ORDER entry must appear here.
pub(crate) const CLUSTER_KEYS: &[(&str, &[&str])] = &[
    // Append-only: id / natural key (verbatim order).
    ("commit", &["id"]),
    ("axis", &["id"]),
    ("erasure_marker", &["id"]),
    ("run", &["id"]),
    ("axis_datetime", &["axis_id", "pos"]),
    ("axis_period", &["axis_id", "pos"]),
    ("axis_label", &["axis_id", "pos"]),
    ("axis_integer", &["axis_id", "pos"]),
    ("axis_float", &["axis_id", "pos"]),
    ("axis_time_pattern", &["axis_id", "pos", "term_no"]),
    // All-live array tier: key triple (locality only — no validity column).
    (
        "value_array",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    // History twin: key triple (+ valid_from via the uniform versioned rule).
    (
        "value_array_hist",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    // From/to value tiers: key triple (long adds its position tuple).
    (
        "value_scalar",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    (
        "value_long",
        &[
            "entity_id",
            "parameter_definition_id",
            "alternative_id",
            "i1_pos",
            "i2_pos",
            "i3_pos",
            "i4_pos",
        ],
    ),
    (
        "definition_value_scalar",
        &["parameter_definition_id", "slot"],
    ),
    (
        "definition_value_long",
        &[
            "parameter_definition_id",
            "slot",
            "i1_pos",
            "i2_pos",
            "i3_pos",
            "i4_pos",
        ],
    ),
    (
        "definition_value_array",
        &["parameter_definition_id", "slot"],
    ),
    // Narrow versioned tables: their identity/key columns.
    ("metadata", &["id"]),
    ("entity_metadata", &["entity_id", "metadata_id"]),
    (
        "parameter_value_metadata",
        &[
            "entity_id",
            "parameter_definition_id",
            "alternative_id",
            "metadata_id",
        ],
    ),
    ("alternative", &["id"]),
    ("scenario", &["id"]),
    ("scenario_alternative", &["scenario_id", "alternative_id"]),
    ("entity_alternative", &["entity_id", "alternative_id"]),
    (
        "membership",
        &["group_entity_id", "member_entity_id", "alternative_id"],
    ),
    ("display_mode", &["id"]),
    (
        "entity_class_display_mode",
        &["display_mode_id", "entity_class_id"],
    ),
    ("entity_class", &["id"]),
    ("class_set", &["id"]),
    ("class_set_member", &["class_set_id", "member_class_id"]),
    ("entity_class_dimension", &["entity_class_id", "position"]),
    ("entity", &["id"]),
    ("entity_element", &["entity_id", "position"]),
    ("entity_location", &["entity_id"]),
    ("units_of_measure", &["id"]),
    ("parameter_value_list", &["id"]),
    ("list_value", &["id"]),
    ("parameter_definition", &["id"]),
    (
        "entity_parameter_unit",
        &["entity_id", "parameter_definition_id"],
    ),
    ("parameter_type", &["id"]),
    (
        "parameter_type_dimension",
        &["parameter_type_id", "position"],
    ),
    ("semantic_annotation", &["id"]),
];

/// The `ORDER BY` clause for one table's rebuild copy (see [`CLUSTER_KEYS`]).
/// Hard-errors on a table missing from the map or a key column absent from the
/// live catalog (`real_cols` — the same drift discipline as [`check_catalog`]).
pub(crate) fn cluster_order(table: &str, real_cols: &[String]) -> Result<String, DbError> {
    let keys = CLUSTER_KEYS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, k)| *k)
        .ok_or_else(|| {
            DbError::Api(format!(
                "compaction catalog drift: table {table:?} has no CLUSTER_KEYS entry \
                 (a schema edit added a table; update rebuild::CLUSTER_KEYS)"
            ))
        })?;
    for k in keys {
        if !real_cols.iter().any(|c| c == k) {
            return Err(DbError::Api(format!(
                "compaction catalog drift: cluster key column {k:?} of table {table:?} \
                 is gone (update rebuild::CLUSTER_KEYS)"
            )));
        }
    }
    let mut parts: Vec<String> = Vec::with_capacity(keys.len() + 2);
    let versioned = real_cols.iter().any(|c| c == "valid_to_commit");
    if versioned {
        // Current-first (D-E4): live rows form one contiguous prefix.
        parts.push("(\"valid_to_commit\" IS NULL) DESC".to_string());
    }
    parts.extend(keys.iter().map(|k| q(k)));
    if versioned {
        parts.push(q("valid_from_commit"));
    }
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

/// Quote a SQL identifier (double-quote, embedded `"` doubled). Every table and
/// column name flows through this so reserved words (`commit`, `values`,
/// `timestamp`, `user`, …) are always safe.
pub(crate) fn q(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Confirm the freshly-init'd temp seeded [`SCHEMA_VERSION`] (guards the const
/// against DDL drift). Read before B1 wipes `metadata`.
pub(crate) fn assert_temp_schema_version(conn: &Connection) -> Result<(), DbError> {
    let v: String = conn.query_row(
        "SELECT value FROM metadata \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
        [],
        |r| r.get(0),
    )?;
    if v != SCHEMA_VERSION {
        return Err(DbError::Api(format!(
            "internal: fresh temp seeded schema_version {v:?} != compaction::SCHEMA_VERSION \
             {SCHEMA_VERSION:?} — the const has drifted from the DDL"
        )));
    }
    Ok(())
}

/// Per-table ordered real (insertable) columns: catalog columns minus the
/// [`GENERATED_COLUMNS`] set, in `ordinal_position` order. Ordered so the copy's
/// INSERT list and SELECT list line up positionally.
pub(crate) fn discover_real_columns(
    conn: &Connection,
) -> Result<Vec<(String, Vec<String>)>, DbError> {
    let mut out = Vec::with_capacity(TABLE_ORDER.len());
    for table in TABLE_ORDER {
        let mut stmt = conn.prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )?;
        let names: Vec<String> = stmt
            .query_map([table], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        let gen = generated_for(table);
        let real: Vec<String> = names
            .into_iter()
            .filter(|c| !gen.contains(&c.as_str()))
            .collect();
        out.push((table.to_string(), real));
    }
    Ok(out)
}

/// R3 cross-check against the live temp catalog: (a) the set of base tables is
/// exactly [`TABLE_ORDER`]; (b) every column named in [`GENERATED_COLUMNS`]
/// still exists on its table. Hard-error on any divergence.
pub(crate) fn check_catalog(
    conn: &Connection,
    real_cols: &[(String, Vec<String>)],
) -> Result<(), DbError> {
    // (a) base-table set.
    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
    )?;
    let mut found: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    found.sort();
    let mut expected: Vec<String> = TABLE_ORDER.iter().map(|t| t.to_string()).collect();
    expected.sort();
    if found != expected {
        return Err(DbError::Api(format!(
            "compaction catalog drift: base tables {found:?} != known set {expected:?} \
             (a schema edit added/removed a table; update rebuild::TABLE_ORDER + \
             GENERATED_COLUMNS)"
        )));
    }
    // (b) each mapped generated column still exists on its table. Rebuild the
    // full column list per table (real + generated) from the catalog to check
    // presence.
    for (table, gens) in GENERATED_COLUMNS {
        let mut cstmt = conn
            .prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?")?;
        let cols: Vec<String> = cstmt
            .query_map([table], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        for g in *gens {
            if !cols.iter().any(|c| c == g) {
                return Err(DbError::Api(format!(
                    "compaction catalog drift: generated column {g:?} of table {table:?} is gone \
                     (update rebuild::GENERATED_COLUMNS)"
                )));
            }
        }
    }
    // Also fail if any table produced an empty real-column list (impossible for
    // the real schema; catches a wholesale-generated map mistake).
    for (table, real) in real_cols {
        if real.is_empty() {
            return Err(DbError::Api(format!(
                "compaction: table {table:?} has no real columns to copy \
                 (GENERATED_COLUMNS over-claims)"
            )));
        }
    }
    Ok(())
}

/// ATTACH the source read-only on the temp connection, mapping a busy file lock
/// to [`DbError::Busy`] (same UX as a busy commit).
pub(crate) fn attach_source_ro(conn: &Connection, src_path: &Path) -> Result<(), DbError> {
    // Single-quote-escape the path for the SQL literal.
    let lit = src_path.display().to_string().replace('\'', "''");
    let sql = format!("ATTACH '{lit}' AS src (READ_ONLY)");
    conn.execute_batch(&sql).map_err(|e| {
        if e.to_string().contains("Could not set lock") {
            DbError::Busy { attempts: 1 }
        } else {
            DbError::Duckdb(e)
        }
    })
}

/// Restore each fresh sequence to the source's state (B2, confirmed on bundled
/// 1.5.5; re-verified on 2.0.0). The fresh sequences sit at their DDL start after `init_schema`; left
/// there, the next auto-created id would be 1 and collide with copied ids /
/// violate a `one_current_*` index.
///
/// **Mechanism (B2 — locked):** `CREATE OR REPLACE SEQUENCE … START WITH n`
/// alone FAILS (dependency error — the column DEFAULT `nextval('seq')` depends
/// on it) and `ALTER SEQUENCE … RESTART` is unimplemented. The working dance is
/// per sequence: DROP the column DEFAULT → `CREATE OR REPLACE … START WITH n` →
/// re-add the DEFAULT.
///
/// `n = last_value + increment_by` so the NEXT `nextval` equals it (mirrors the
/// source's last handed-out id; DuckDB `nextval` is non-transactional so
/// mirroring `last_value` — not `max(id)+1` — is the non-destructive bar). A
/// source sequence that is unused (`last_value` NULL) is left at its DDL start.
///
/// **The whole sequence spec is re-emitted** (F10 fix): `INCREMENT BY`,
/// `MINVALUE`, `MAXVALUE` and `CYCLE` are read from the source and written back
/// with the new start. A bare `START WITH n` silently resets the increment to
/// 1, which for `seq_value_array` (stride 2) would let new `array_id`s fall next
/// to copied ones — dense id sets that DuckDB rewrites into range filters, so
/// payload reads lose the index scan. `n = last_value + increment_by` keeps the
/// ids' parity.
///
/// **B2b:** `duckdb_sequences()` returns rows for BOTH databases under ATTACH,
/// so the source state read is filtered `WHERE database_name = 'src'`.
pub(crate) fn restore_sequences(conn: &Connection) -> Result<(), DbError> {
    // (table, column, sequence) for every column whose DEFAULT is nextval('…').
    // Read from the temp's own catalog (main db).
    let mut stmt = conn.prepare(
        "SELECT table_name, column_name, column_default FROM information_schema.columns \
         WHERE column_default LIKE 'nextval(%'",
    )?;
    let mapping: Vec<(String, String, String)> = stmt
        .query_map([], |r| {
            let t: String = r.get(0)?;
            let c: String = r.get(1)?;
            let d: String = r.get(2)?;
            Ok((t, c, parse_seq_name(&d)))
        })?
        .collect::<Result<_, _>>()?;

    for (table, column, seq) in mapping {
        // Source state for THIS sequence (B2b: filter by database_name).
        let row: Option<SeqState> = conn
            .query_row(
                "SELECT last_value, increment_by, min_value, max_value, cycle \
                 FROM duckdb_sequences() \
                 WHERE database_name = 'src' AND sequence_name = ?",
                [&seq],
                |r| {
                    Ok(SeqState {
                        last: r.get(0)?,
                        inc: r.get(1)?,
                        min: r.get(2)?,
                        max: r.get(3)?,
                        cycle: r.get(4)?,
                    })
                },
            )
            .ok();
        let Some(SeqState {
            last: Some(last),
            inc,
            min,
            max,
            cycle,
        }) = row
        else {
            // Unused (NULL last_value) or missing ⇒ leave the fresh sequence at
            // its DDL start (matches an unused source sequence).
            continue;
        };
        let n = last + inc;
        let cycle = if cycle { "CYCLE" } else { "NO CYCLE" };
        // The confirmed drop-default → replace → re-add-default dance, with
        // the full sequence spec (see the F10 note above).
        conn.execute_batch(&format!(
            "ALTER TABLE {t} ALTER COLUMN {c} DROP DEFAULT; \
             CREATE OR REPLACE SEQUENCE {s} START WITH {n} INCREMENT BY {inc} \
               MINVALUE {min} MAXVALUE {max} {cycle}; \
             ALTER TABLE {t} ALTER COLUMN {c} SET DEFAULT nextval('{seq}');",
            t = q(&table),
            c = q(&column),
            s = q(&seq),
            seq = seq.replace('\'', "''"),
        ))?;
    }
    Ok(())
}

/// One source sequence's state, as `duckdb_sequences()` reports it.
struct SeqState {
    last: Option<i64>,
    inc: i64,
    min: i64,
    max: i64,
    cycle: bool,
}

/// Extract `seq` from a `nextval('seq')` DEFAULT expression.
fn parse_seq_name(default_expr: &str) -> String {
    default_expr
        .split_once('\'')
        .and_then(|(_, rest)| rest.split_once('\''))
        .map(|(name, _)| name.to_string())
        .unwrap_or_default()
}

/// Remove a temp file and its WAL sidecar (abort cleanup).
pub(crate) fn remove_temp(temp_path: &Path) {
    let _ = fs::remove_file(temp_path);
    let mut wal = temp_path.as_os_str().to_os_string();
    wal.push(".wal");
    let _ = fs::remove_file(PathBuf::from(wal));
}

/// fsync a file's bytes to disk (durability before the rename, §3).
pub(crate) fn fsync_file(path: &Path) -> Result<(), DbError> {
    let f = fs::File::open(path).map_err(|source| DbError::SchemaDir {
        path: path.display().to_string(),
        source,
    })?;
    f.sync_all().map_err(|source| DbError::SchemaDir {
        path: path.display().to_string(),
        source,
    })
}

/// fsync a directory entry (POSIX durability step for the rename). Best-effort:
/// on non-Unix a directory handle cannot be fsync'd — the atomic-replace API
/// carries the durability guarantee there (design risk #2).
#[cfg(unix)]
pub(crate) fn fsync_dir(dir: &Path) {
    if let Ok(f) = fs::File::open(dir) {
        let _ = f.sync_all();
    }
}

#[cfg(not(unix))]
pub(crate) fn fsync_dir(_dir: &Path) {}
