//! Metadata: the pair pool, sources, licenses, roles and the two link tables
//! (`schema/001_meta.sql`). This module is the one writer of
//! `metadata_pair`, `source`, `license`, `source_license`, `metadata_role`,
//! `object_metadata_link` and `value_metadata_link` (one writer per
//! table); `crate::property` owns `database_property`.
//!
//! Value links belong to a **value version**: a link row is valid while the
//! value version current at its `valid_from_commit` is. Every new version of
//! a value with links closes them, and a *keep* reopens them at the same
//! commit (`version + 1`), so link spans tile with value versions. The
//! write path's keep / set / clear statements are the metadata channel of
//! `api::commit`; the lifecycle helpers here serve the other
//! value writers and closers:
//!   - a value delete closes the value's links (`close_value_links`);
//!   - a template `map_values` rewrite keeps them (`keep_value_links_where`),
//!     its drop arm closes them (`close_orphaned_value_links`);
//!   - a template `merge_parameter` carries them to the filled target slots
//!     and carries the source definition's own links (`carry_merged_links`);
//!   - object deletes close links through the registry legs (links never
//!     block a delete).

use duckdb::{params, Connection};

use crate::error::DbError;

/// One key triple `(entity, parameter definition, alternative)`.
pub(crate) type Triple = (i64, i64, i64);

/// The rows of a value-link triple set: a `FROM` source with columns
/// `e, p, a`. `Lists` binds three `BIGINT[]` lists; `Query` is a SELECT of
/// three columns (no binds).
pub(crate) enum TripleSet<'a> {
    Lists(&'a [Triple]),
    Query(&'a str),
}

impl TripleSet<'_> {
    fn source_sql(&self) -> String {
        match self {
            TripleSet::Lists(_) => "(SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                 unnest(?::BIGINT[]) AS a) k"
                .to_string(),
            TripleSet::Query(q) => format!("(SELECT * FROM ({q}) q(e, p, a)) k"),
        }
    }

    fn binds(&self) -> Vec<duckdb::types::Value> {
        match self {
            TripleSet::Lists(ts) => {
                let list = |f: fn(&Triple) -> i64| {
                    duckdb::types::Value::List(
                        ts.iter()
                            .map(|t| duckdb::types::Value::BigInt(f(t)))
                            .collect(),
                    )
                };
                vec![list(|t| t.0), list(|t| t.1), list(|t| t.2)]
            }
            TripleSet::Query(_) => Vec::new(),
        }
    }
}

/// The match of a `value_metadata_link` row against the triple set `k`.
const K_MATCH: &str = "value_metadata_link.entity_id = k.e \
     AND value_metadata_link.parameter_definition_id = k.p \
     AND value_metadata_link.alternative_id = k.a";

/// Does the file hold any live value link? (One index-free probe; a
/// link-free file skips every link statement.)
pub(crate) fn has_value_links(conn: &Connection) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM value_metadata_link WHERE valid_to_commit IS NULL)",
        [],
        |r| r.get(0),
    )?)
}

/// Close the live links of `set`'s triples at commit `c` (same-commit
/// guard: a link this commit created is never closed). Returns rows closed.
pub(crate) fn close_links(
    conn: &Connection,
    c: i64,
    set: &TripleSet<'_>,
) -> Result<usize, DbError> {
    let mut binds: Vec<duckdb::types::Value> = vec![c.into()];
    binds.extend(set.binds());
    binds.push(c.into());
    Ok(conn.execute(
        &format!(
            "UPDATE value_metadata_link SET valid_to_commit = ? FROM {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit IS NULL \
               AND value_metadata_link.valid_from_commit < ?",
            set.source_sql()
        ),
        duckdb::params_from_iter(binds.iter()),
    )?)
}

/// Reopen at `c` (`version + 1`) the links of `set`'s triples that were
/// closed at `c`: the second half of a *keep*. Returns rows inserted.
pub(crate) fn reopen_links(
    conn: &Connection,
    c: i64,
    set: &TripleSet<'_>,
) -> Result<usize, DbError> {
    let mut binds: Vec<duckdb::types::Value> = vec![c.into()];
    binds.extend(set.binds());
    binds.push(c.into());
    Ok(conn.execute(
        &format!(
            "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, \
               alternative_id, target_kind, target_id, role_id, valid_from_commit, version) \
             SELECT value_metadata_link.entity_id, value_metadata_link.parameter_definition_id, \
                    value_metadata_link.alternative_id, target_kind, target_id, role_id, ?, \
                    version + 1 \
             FROM value_metadata_link, {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit = ? \
               AND NOT EXISTS (SELECT 1 FROM value_metadata_link x \
                 WHERE x.valid_to_commit IS NULL \
                   AND x.entity_id = value_metadata_link.entity_id \
                   AND x.parameter_definition_id = value_metadata_link.parameter_definition_id \
                   AND x.alternative_id = value_metadata_link.alternative_id \
                   AND x.target_kind = value_metadata_link.target_kind \
                   AND x.target_id = value_metadata_link.target_id \
                   AND coalesce(x.role_id, 0) = coalesce(value_metadata_link.role_id, 0))",
            set.source_sql()
        ),
        duckdb::params_from_iter(binds.iter()),
    )?)
}

/// Close the links of the closed `triples`, chunked (the unit tests' direct
/// closer; the write path closes links set-based). Skips every statement on
/// a link-free file. Returns rows closed.
#[cfg(test)]
pub(crate) fn close_value_links(
    conn: &Connection,
    c: i64,
    triples: &[Triple],
) -> Result<usize, DbError> {
    if triples.is_empty() || !has_value_links(conn)? {
        return Ok(0);
    }
    let mut n = 0;
    for chunk in triples.chunks(crate::history::LONG_INSERT_CHUNK) {
        n += close_links(conn, c, &TripleSet::Lists(chunk))?;
    }
    Ok(n)
}

/// *Keep* across a new value version written outside the write path (a
/// template rewrite, OD3): close and reopen at `c` the links of the triples
/// `triple_sql` selects (three columns). Returns the links kept.
pub(crate) fn keep_value_links_where(
    conn: &Connection,
    c: i64,
    triple_sql: &str,
) -> Result<usize, DbError> {
    if !has_value_links(conn)? {
        return Ok(0);
    }
    let set = TripleSet::Query(triple_sql);
    close_links(conn, c, &set)?;
    reopen_links(conn, c, &set)
}

/// Close the live links of definition `pd_id` whose value is no longer live
/// in any tier (a template drop arm closed it at `c`). Returns rows closed.
pub(crate) fn close_orphaned_value_links(
    conn: &Connection,
    c: i64,
    pd_id: i64,
) -> Result<usize, DbError> {
    if !has_value_links(conn)? {
        return Ok(0);
    }
    let q = format!(
        "SELECT l.entity_id, l.parameter_definition_id, l.alternative_id \
         FROM value_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.parameter_definition_id = {pd_id} \
           AND NOT EXISTS (SELECT 1 FROM value_keys v WHERE v.entity_id = l.entity_id \
                 AND v.parameter_definition_id = l.parameter_definition_id \
                 AND v.alternative_id = l.alternative_id)"
    );
    close_links(conn, c, &TripleSet::Query(&q))
}

/// What a `merge_parameter` carried ([`carry_merged_links`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Carried {
    /// Value links copied to the target's filled slots.
    pub(crate) value_links: usize,
    /// The source definition's own object links copied to the target.
    pub(crate) object_links: usize,
}

/// `merge_parameter` (template reconcile): before the source definition
/// closes, copy (a) the value links of every source value the merge carried
/// into an empty target slot (`filled`: the `(entity, alternative)` pairs
/// the merge's INSERT returned) onto that target value, and (b) the source definition's own object links
/// onto the target definition, anti-joined on the target's live edges. The
/// source's links then close with its cascade. "Copy carries" (owner ruling
/// 2026-11-17).
pub(crate) fn carry_merged_links(
    conn: &Connection,
    c: i64,
    src: i64,
    tgt: i64,
    filled: &[(i64, i64)],
) -> Result<Carried, DbError> {
    use duckdb::types::Value;
    let list = |f: fn(&(i64, i64)) -> i64| {
        Value::List(filled.iter().map(|p| Value::BigInt(f(p))).collect())
    };
    let (es, alts) = (list(|p| p.0), list(|p| p.1));
    let value_links = if filled.is_empty() {
        0
    } else {
        conn.execute(
            "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, alternative_id, \
           target_kind, target_id, role_id, valid_from_commit, version) \
         SELECT l.entity_id, ?1, l.alternative_id, l.target_kind, l.target_id, l.role_id, ?2, \
                coalesce((SELECT max(x.version) FROM value_metadata_link x \
                   WHERE x.entity_id = l.entity_id AND x.parameter_definition_id = ?1 \
                     AND x.alternative_id = l.alternative_id AND x.target_kind = l.target_kind \
                     AND x.target_id = l.target_id \
                     AND coalesce(x.role_id, 0) = coalesce(l.role_id, 0)), 0) + 1 \
         FROM value_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.parameter_definition_id = ?3 \
           AND (l.entity_id, l.alternative_id) IN \
                 (SELECT unnest(?4::BIGINT[]), unnest(?5::BIGINT[])) \
           AND NOT EXISTS (SELECT 1 FROM value_metadata_link t \
                 WHERE t.valid_to_commit IS NULL AND t.entity_id = l.entity_id \
                   AND t.parameter_definition_id = ?1 AND t.alternative_id = l.alternative_id \
                   AND t.target_kind = l.target_kind AND t.target_id = l.target_id \
                   AND coalesce(t.role_id, 0) = coalesce(l.role_id, 0))",
            params![tgt, c, src, es, alts],
        )?
    };
    let object_links = conn.execute(
        "INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id, \
           role_id, valid_from_commit, version) \
         SELECT 'parameter_definition', ?1, l.target_kind, l.target_id, l.role_id, ?2, \
                coalesce((SELECT max(x.version) FROM object_metadata_link x \
                   WHERE x.object_type = 'parameter_definition' AND x.object_id = ?1 \
                     AND x.target_kind = l.target_kind AND x.target_id = l.target_id \
                     AND coalesce(x.role_id, 0) = coalesce(l.role_id, 0)), 0) + 1 \
         FROM object_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.object_type = 'parameter_definition' \
           AND l.object_id = ?3 \
           AND NOT EXISTS (SELECT 1 FROM object_metadata_link t \
                 WHERE t.valid_to_commit IS NULL AND t.object_type = 'parameter_definition' \
                   AND t.object_id = ?1 AND t.target_kind = l.target_kind \
                   AND t.target_id = l.target_id \
                   AND coalesce(t.role_id, 0) = coalesce(l.role_id, 0))",
        params![tgt, c, src],
    )?;
    Ok(Carried {
        value_links,
        object_links,
    })
}

/// Close the object links whose subject is one of the value lists `ids`
/// (a SELECT of ids) at `c` (the value-list close door, which runs outside
/// the object-delete registry). Returns rows closed.
pub(crate) fn close_object_links_of(
    conn: &Connection,
    c: i64,
    object_type: &str,
    ids_sql: &str,
) -> Result<usize, DbError> {
    Ok(conn.execute(
        &format!(
            "UPDATE object_metadata_link SET valid_to_commit = ? \
             WHERE object_type = ? AND object_id IN ({ids_sql}) \
               AND valid_to_commit IS NULL AND valid_from_commit < ?"
        ),
        params![c, object_type, c],
    )?)
}

/// Fail when any of `keys` has a live value link: the raw per-key writers
/// (`history::write_value`, `history::write_long_values`) cannot state keep
/// / set / clear, so they refuse an annotated value (use
/// `api::commit`).
pub(crate) fn refuse_linked(conn: &Connection, keys: &[Triple]) -> Result<(), DbError> {
    if keys.is_empty() || !has_value_links(conn)? {
        return Ok(());
    }
    let linked: Vec<Triple> = live_value_links(conn, keys)?.into_keys().collect();
    if !linked.is_empty() {
        let count = linked.len();
        let sample = triple_names(conn, &linked).unwrap_or_default();
        return Err(DbError::ValueMetadataUnstated {
            message: format!(
                "{count} value(s) carry metadata links: a new version must state keep, set or \
                 clear — write through commit with a value_metadata statement \
                 (the raw per-key writers cannot state one)"
            ),
            count,
            sample,
        });
    }
    Ok(())
}

// ===========================================================================
// The metadata channel of `api::commit` (`ChangeSet::metadata`)
// ===========================================================================
//
// Runs LAST in the call (after the purges), under the call's commit id:
//   M0 fresh resolution (closure locals; never the segment phase's caches),
//   M1 pool creates and revisions in item order, then the one-item-per-object
//      check on resolved ids and the resolution of the pool deletes,
//   M2 value statements and value-edge items,
//   M3 object links,
//   M4 pool deletes (RESTRICT judged on the post-call state).

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::api::{
    find_alternative, find_entity, find_entity_class, find_parameter, find_scenario,
    find_value_list, lookup_unit,
};
use crate::text::{nfc, NameFolds};
use crate::types::{
    fold, Action, Blocker, CoveredWrites, LicenseFields, LinkSpec, MetadataChange, MetadataEffect,
    MetadataKind, MetadataOutcome, PairRef, Ref, RoleSel, Scope, SourceFields, Subject, Target,
    ValueAddr, ValueLinkCounts, Warning, WarningKind, WriteReport,
};

/// A link edge: `(target_kind, target_id, role_id)`.
type Edge = (&'static str, i64, Option<i64>);

/// How many unstated values the refusal names.
const UNSTATED_SAMPLE: usize = 10;

// ---------------------------------------------------------------------------
// Prepare (pure, before the transaction)
// ---------------------------------------------------------------------------

fn ident(what: &str, s: &str) -> Result<(), DbError> {
    if s.is_empty() || s.trim() != s || s.chars().any(char::is_control) {
        return Err(DbError::Api(format!(
            "metadata: {what} {s:?} must be non-empty, without leading or trailing whitespace \
             or control characters"
        )));
    }
    Ok(())
}

fn nfc_s(s: &mut String) {
    let n = nfc(s).into_owned();
    *s = n;
}

fn nfc_ref(r: &mut Ref) {
    if let Ref::Name(n) = r {
        nfc_s(n);
    }
}

fn nfc_pair(p: &mut PairRef) {
    if let PairRef::Pair { name, .. } = p {
        nfc_s(name);
    }
}

fn nfc_target(t: &mut Target) {
    match t {
        Target::Source(r) | Target::License(r) => nfc_ref(r),
        Target::Pair(p) => nfc_pair(p),
    }
}

fn nfc_role(r: &mut RoleSel) {
    if let RoleSel::Named(n) = r {
        nfc_s(n);
    }
}

fn nfc_subject(s: &mut Subject) {
    match s {
        Subject::Dataset | Subject::Run(_) => {}
        Subject::Entity { class, name } | Subject::Parameter { class, name } => {
            nfc_s(class);
            nfc_s(name);
        }
        Subject::Class(n)
        | Subject::Alternative(n)
        | Subject::Scenario(n)
        | Subject::ValueList(n)
        | Subject::Unit(n) => nfc_s(n),
        Subject::Source(r) | Subject::License(r) => nfc_ref(r),
    }
}

fn nfc_value(v: &mut ValueAddr) {
    nfc_s(&mut v.class);
    nfc_s(&mut v.parameter);
    nfc_s(&mut v.alternative);
    nfc_s(&mut v.entity);
}

/// Names of the metadata items in NFC (identifiers only: pair values,
/// titles, paths, citations, descriptions and policies are data, verbatim).
pub(crate) fn nfc_items(items: &mut [MetadataChange]) {
    for it in items {
        match it {
            MetadataChange::PutSource {
                source: r,
                new_name: rename,
                ..
            }
            | MetadataChange::PutLicense {
                license: r,
                new_name: rename,
                ..
            }
            | MetadataChange::PutRole {
                role: r,
                new_name: rename,
                ..
            } => {
                nfc_ref(r);
                if let Some(n) = rename {
                    nfc_s(n);
                }
            }
            MetadataChange::SetSourceLicenses { source, licenses } => {
                nfc_ref(source);
                licenses.iter_mut().for_each(nfc_ref);
            }
            MetadataChange::CreatePair { name, .. } => nfc_s(name),
            MetadataChange::RevisePair { pair, .. } | MetadataChange::DeletePair { pair, .. } => {
                nfc_pair(pair)
            }
            MetadataChange::RenamePairKey {
                current: from,
                new_name: to,
            } => {
                nfc_s(from);
                nfc_s(to);
            }
            MetadataChange::SetProperty { key, .. } => nfc_s(key),
            MetadataChange::DeleteSource { source: r, .. }
            | MetadataChange::DeleteLicense { license: r, .. }
            | MetadataChange::DeleteRole { role: r } => nfc_ref(r),
            MetadataChange::Link { subject, link } => {
                nfc_subject(subject);
                nfc_target(&mut link.target);
                nfc_role(&mut link.role);
            }
            MetadataChange::Unlink {
                subject,
                target,
                role,
            } => {
                nfc_subject(subject);
                nfc_target(target);
                nfc_role(role);
            }
            MetadataChange::LinkValue { value, link } => {
                nfc_value(value);
                nfc_target(&mut link.target);
                nfc_role(&mut link.role);
            }
            MetadataChange::UnlinkValue {
                value,
                target,
                role,
            } => {
                nfc_value(value);
                nfc_target(target);
                nfc_role(role);
            }
            MetadataChange::ValueMetadata { scope, action } => {
                match scope {
                    Scope::Save => {}
                    Scope::Target {
                        class,
                        parameter,
                        alternative,
                    } => {
                        nfc_s(class);
                        nfc_s(parameter);
                        nfc_s(alternative);
                    }
                    Scope::Value(v) => nfc_value(v),
                }
                if let Action::Set(ls) = action {
                    for l in ls {
                        nfc_target(&mut l.target);
                        nfc_role(&mut l.role);
                    }
                }
            }
        }
    }
}

fn check_ref(what: &str, r: &Ref) -> Result<(), DbError> {
    match r {
        Ref::Name(n) => ident(what, n),
        Ref::Id(_) => Ok(()),
    }
}

fn check_pair(p: &PairRef) -> Result<(), DbError> {
    match p {
        PairRef::Pair { name, .. } => ident("pair key", name),
        PairRef::Id(_) => Ok(()),
    }
}

fn check_link(l: &LinkSpec, value: bool) -> Result<(), DbError> {
    match &l.target {
        Target::Source(r) => check_ref("source", r)?,
        Target::License(r) => {
            if value {
                return Err(DbError::Api(
                    "metadata: a value links to a source or a pair, not a license \
                     (values reach licenses through their sources)"
                        .into(),
                ));
            }
            check_ref("license", r)?
        }
        Target::Pair(p) => check_pair(p)?,
    }
    if let RoleSel::Named(n) = &l.role {
        ident("role", n)?;
    }
    Ok(())
}

fn check_date(what: &str, d: &Option<String>) -> Result<(), DbError> {
    if let Some(d) = d {
        let b = d.as_bytes();
        let ok = b.len() == 10
            && b[4] == b'-'
            && b[7] == b'-'
            && b.iter()
                .enumerate()
                .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
        if !ok {
            return Err(DbError::Api(format!(
                "metadata: source {what} {d:?} is not an ISO date (YYYY-MM-DD)"
            )));
        }
    }
    Ok(())
}

/// A fold key of an object an item names, for the one-item-per-object rule.
fn ref_key(kind: &str, r: &Ref) -> String {
    match r {
        Ref::Id(i) => format!("{kind}#{i}"),
        Ref::Name(n) => format!("{kind}:{}", fold(n)),
    }
}

fn pair_key(p: &PairRef) -> String {
    match p {
        PairRef::Id(i) => format!("pair#{i}"),
        PairRef::Pair { name, value } => format!("pair:{}\u{1f}{value}", fold(name)),
    }
}

fn value_key(v: &ValueAddr) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        fold(&v.class),
        fold(&v.parameter),
        fold(&v.alternative),
        fold(&v.entity)
    )
}

fn target_key(t: &Target) -> String {
    match t {
        Target::Source(r) => ref_key("source", r),
        Target::License(r) => ref_key("license", r),
        Target::Pair(p) => pair_key(p),
    }
}

fn role_key(r: &RoleSel) -> String {
    match r {
        RoleSel::NoRole => String::new(),
        RoleSel::Named(n) => fold(n),
    }
}

fn subject_key(s: &Subject) -> String {
    match s {
        Subject::Dataset => "dataset".into(),
        Subject::Entity { class, name } => format!("entity:{}\u{1f}{}", fold(class), fold(name)),
        Subject::Class(n) => format!("class:{}", fold(n)),
        Subject::Parameter { class, name } => {
            format!("parameter:{}\u{1f}{}", fold(class), fold(name))
        }
        Subject::Alternative(n) => format!("alternative:{}", fold(n)),
        Subject::Scenario(n) => format!("scenario:{}", fold(n)),
        Subject::Run(i) => format!("run#{i}"),
        Subject::Source(r) => ref_key("source", r),
        Subject::License(r) => ref_key("license", r),
        Subject::ValueList(n) => format!("list:{}", fold(n)),
        Subject::Unit(n) => format!("unit:{}", fold(n)),
    }
}

/// The pure checks of the channel (before any SQL): identifiers, dates,
/// JSON policies, tool-key syntax; one statement per scope, one item per
/// edge, no edge item next to a value-scope `Set`, and one revise / rename
/// / delete item per object (by name; repeated on resolved ids in the
/// transaction).
pub(crate) fn prepare(items: &[MetadataChange]) -> Result<(), DbError> {
    let mut scopes: HashSet<String> = HashSet::new();
    let mut edges: HashSet<String> = HashSet::new();
    let mut value_sets: HashSet<String> = HashSet::new();
    let mut value_edges: HashSet<String> = HashSet::new();
    // Objects a revise / rename / delete item names, and every name of every
    // item (kind:fold), to refuse a second item on a touched object.
    let mut touched: HashMap<String, usize> = HashMap::new();
    let mut named: Vec<(usize, String)> = Vec::new();
    // Put items by name may create (links may then name the object), so
    // they are checked against each other and against deletes only; a Put
    // that revises is caught on resolved ids in the transaction.
    let mut puts: HashMap<String, usize> = HashMap::new();
    for (i, it) in items.iter().enumerate() {
        let mut put_once = |k: String| -> Result<(), DbError> {
            if let Some(j) = puts.insert(k, i) {
                return Err(DbError::Api(format!(
                    "metadata items {j} and {i} both revise, rename or delete the same object; \
                     one item per object per call"
                )));
            }
            Ok(())
        };
        let err = |e: DbError| match e {
            DbError::Api(m) => DbError::Api(format!("metadata item {i}: {m}")),
            other => other,
        };
        let mut touch = |k: String| -> Result<(), DbError> {
            if let Some(j) = touched.insert(k.clone(), i) {
                return Err(DbError::Api(format!(
                    "metadata items {j} and {i} both revise, rename or delete the same object; \
                     one item per object per call"
                )));
            }
            Ok(())
        };
        match it {
            MetadataChange::PutSource {
                source,
                new_name: rename,
                fields,
            } => {
                check_ref("source", source).map_err(err)?;
                if let Some(n) = rename {
                    ident("source name", n).map_err(err)?;
                    touch(format!("source:{}", fold(n)))?;
                }
                check_date("issued", &fields.issued).map_err(err)?;
                check_date("retrieved", &fields.retrieved).map_err(err)?;
                put_once(ref_key("source", source))?;
            }
            MetadataChange::PutLicense {
                license,
                new_name: rename,
                fields,
            } => {
                check_ref("license", license).map_err(err)?;
                if let Some(n) = rename {
                    ident("license name", n).map_err(err)?;
                    touch(format!("license:{}", fold(n)))?;
                }
                if let Some(p) = &fields.policy {
                    serde_json::from_str::<serde_json::Value>(p).map_err(|e| {
                        DbError::Api(format!(
                            "metadata item {i}: the license policy is not valid JSON ({e})"
                        ))
                    })?;
                }
                put_once(ref_key("license", license))?;
            }
            MetadataChange::SetSourceLicenses { source, licenses } => {
                check_ref("source", source).map_err(err)?;
                for l in licenses {
                    check_ref("license", l).map_err(err)?;
                    named.push((i, ref_key("license", l)));
                }
                touch(format!("licenses-of:{}", ref_key("source", source)))?;
                named.push((i, ref_key("source", source)));
            }
            MetadataChange::CreatePair { name, .. } => {
                ident("pair key", name).map_err(err)?;
            }
            MetadataChange::RevisePair { pair, .. } => {
                check_pair(pair).map_err(err)?;
                touch(pair_key(pair))?;
            }
            MetadataChange::RenamePairKey {
                current: from,
                new_name: to,
            } => {
                ident("pair key", from).map_err(err)?;
                ident("pair key", to).map_err(err)?;
                touch(format!("pairkey:{}", fold(from)))?;
            }
            MetadataChange::PutRole {
                role,
                new_name: rename,
                ..
            } => {
                check_ref("role", role).map_err(err)?;
                if let Some(n) = rename {
                    ident("role name", n).map_err(err)?;
                    touch(format!("role:{}", fold(n)))?;
                }
                put_once(ref_key("role", role))?;
            }
            MetadataChange::SetProperty { key, .. } => {
                crate::property::validate_user_key(key).map_err(err)?;
                touch(format!("property:{}", fold(key)))?;
            }
            MetadataChange::DeleteSource { source, .. } => {
                check_ref("source", source).map_err(err)?;
                touch(ref_key("source", source))?;
            }
            MetadataChange::DeleteLicense { license, .. } => {
                check_ref("license", license).map_err(err)?;
                touch(ref_key("license", license))?;
            }
            MetadataChange::DeletePair { pair, .. } => {
                check_pair(pair).map_err(err)?;
                touch(pair_key(pair))?;
            }
            MetadataChange::DeleteRole { role } => {
                check_ref("role", role).map_err(err)?;
                touch(ref_key("role", role))?;
            }
            MetadataChange::Link { subject, link } => {
                check_link(link, false).map_err(err)?;
                let k = format!(
                    "{}|{}|{}",
                    subject_key(subject),
                    target_key(&link.target),
                    role_key(&link.role)
                );
                if !edges.insert(k) {
                    return Err(DbError::Api(format!(
                        "metadata item {i}: a second item on the same link"
                    )));
                }
                named.push((i, subject_key(subject)));
                named.push((i, target_key(&link.target)));
                if let RoleSel::Named(n) = &link.role {
                    named.push((i, format!("role:{}", fold(n))));
                }
            }
            MetadataChange::Unlink {
                subject,
                target,
                role,
            } => {
                check_link(
                    &LinkSpec {
                        target: target.clone(),
                        role: role.clone(),
                    },
                    false,
                )
                .map_err(err)?;
                let k = format!(
                    "{}|{}|{}",
                    subject_key(subject),
                    target_key(target),
                    role_key(role)
                );
                if !edges.insert(k) {
                    return Err(DbError::Api(format!(
                        "metadata item {i}: a second item on the same link"
                    )));
                }
            }
            MetadataChange::LinkValue { value, link } => {
                check_link(link, true).map_err(err)?;
                let k = format!(
                    "{}|{}|{}",
                    value_key(value),
                    target_key(&link.target),
                    role_key(&link.role)
                );
                if !edges.insert(k) {
                    return Err(DbError::Api(format!(
                        "metadata item {i}: a second item on the same value link"
                    )));
                }
                value_edges.insert(value_key(value));
                named.push((i, target_key(&link.target)));
                if let RoleSel::Named(n) = &link.role {
                    named.push((i, format!("role:{}", fold(n))));
                }
            }
            MetadataChange::UnlinkValue {
                value,
                target,
                role,
            } => {
                check_link(
                    &LinkSpec {
                        target: target.clone(),
                        role: role.clone(),
                    },
                    true,
                )
                .map_err(err)?;
                let k = format!(
                    "{}|{}|{}",
                    value_key(value),
                    target_key(target),
                    role_key(role)
                );
                if !edges.insert(k) {
                    return Err(DbError::Api(format!(
                        "metadata item {i}: a second item on the same value link"
                    )));
                }
                value_edges.insert(value_key(value));
            }
            MetadataChange::ValueMetadata { scope, action } => {
                if let Action::Set(ls) = action {
                    let mut seen = HashSet::new();
                    for l in ls {
                        check_link(l, true).map_err(err)?;
                        if !seen.insert((target_key(&l.target), role_key(&l.role))) {
                            return Err(DbError::Api(format!(
                                "metadata item {i}: a link is listed twice in one set"
                            )));
                        }
                        named.push((i, target_key(&l.target)));
                        if let RoleSel::Named(n) = &l.role {
                            named.push((i, format!("role:{}", fold(n))));
                        }
                    }
                }
                let k = match scope {
                    Scope::Save => "save".to_string(),
                    Scope::Target {
                        class,
                        parameter,
                        alternative,
                    } => {
                        ident("class", class).map_err(err)?;
                        ident("parameter", parameter).map_err(err)?;
                        ident("alternative", alternative).map_err(err)?;
                        format!(
                            "target:{}\u{1f}{}\u{1f}{}",
                            fold(class),
                            fold(parameter),
                            fold(alternative)
                        )
                    }
                    Scope::Value(v) => {
                        if matches!(action, Action::Set(_)) {
                            value_sets.insert(value_key(v));
                        }
                        format!("value:{}", value_key(v))
                    }
                };
                if !scopes.insert(k) {
                    return Err(DbError::Api(format!(
                        "metadata item {i}: a second keep / set / clear statement for the same \
                         scope"
                    )));
                }
            }
        }
    }
    for (k, j) in &puts {
        if let Some(i) = touched.get(k) {
            return Err(DbError::Api(format!(
                "metadata items {j} and {i} both revise, rename or delete the same object; \
                 one item per object per call"
            )));
        }
    }
    if let Some(v) = value_sets.intersection(&value_edges).next() {
        return Err(DbError::Api(format!(
            "metadata: a value has both a value-scope set statement and link / unlink items \
             ({}); a set is the exact link list",
            v.replace('\u{1f}', "/")
        )));
    }
    for (i, k) in &named {
        if let Some(j) = touched.get(k) {
            if j != i {
                return Err(DbError::Api(format!(
                    "metadata item {i} names an object that item {j} revises, renames or \
                     deletes; one item per object per call (links may name an object created \
                     in the same call)"
                )));
            }
        }
    }
    Ok(())
}

/// Does the channel hold a save- or target-scope `Set` (which annotates new
/// values too, so the write path records every changed value)?
pub(crate) fn has_bulk_set(items: &[MetadataChange]) -> bool {
    items.iter().any(|it| {
        matches!(
            it,
            MetadataChange::ValueMetadata {
                scope: Scope::Save | Scope::Target { .. },
                action: Action::Set(_),
            }
        )
    })
}

/// The `(parameter definition, alternative)` targets a target-scope `Set`
/// names, resolved lookup-only; `None` when every target must be tracked
/// (a save-scope `Set`, or a target this call has yet to create).
pub(crate) fn set_targets(
    conn: &Connection,
    items: &[MetadataChange],
) -> Result<Option<HashSet<(i64, i64)>>, DbError> {
    let mut out = HashSet::new();
    for it in items {
        let MetadataChange::ValueMetadata {
            scope,
            action: Action::Set(_),
        } = it
        else {
            continue;
        };
        match scope {
            Scope::Save => return Ok(None),
            Scope::Target {
                class,
                parameter,
                alternative,
            } => {
                let Some(c) = find_entity_class(conn, class)? else {
                    return Ok(None);
                };
                let Some(p) = find_parameter(conn, c.id, parameter)? else {
                    return Ok(None);
                };
                let Some(a) = find_alternative(conn, alternative)? else {
                    return Ok(None);
                };
                out.insert((p.id, a.id));
            }
            Scope::Value(_) => {}
        }
    }
    Ok(Some(out))
}

/// The `(parameter definition, alternative)` pairs of the live value links,
/// read once per write attempt: the write path records the changed values
/// of only these targets (or of every target when a bulk `Set` asks).
pub(crate) fn linked_targets(conn: &Connection) -> Result<HashSet<(i64, i64)>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT parameter_definition_id, alternative_id FROM value_metadata_link \
         WHERE valid_to_commit IS NULL GROUP BY ALL",
    )?;
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<HashSet<_>, _>>()?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// The transaction phase
// ---------------------------------------------------------------------------

/// What the rest of the call tells the metadata phase.
pub(crate) struct ChannelCall<'a> {
    pub(crate) items: &'a [MetadataChange],
    /// The values this call gave a new version (recorded by the write path
    /// for the linked targets, or for every target under a bulk `Set`).
    pub(crate) changed: &'a [Triple],
    pub(crate) covered_writes: CoveredWrites,
    /// The live value links existed at the start of the attempt.
    pub(crate) had_links: bool,
}

/// Resolution state of one attempt (closure locals: a conflict retry
/// rebuilds it).
struct Ctx<'a> {
    conn: &'a Connection,
    c: i64,
    folds: NameFolds,
    covered: CoveredWrites,
}

fn opt_row<T>(r: Result<T, duckdb::Error>) -> Result<Option<T>, DbError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// A pool table's live row by id or by name: `(id, stored name)`.
fn find_named(
    cx: &mut Ctx<'_>,
    table: &'static str,
    r: &Ref,
) -> Result<Option<(i64, String)>, DbError> {
    let row = match r {
        Ref::Id(i) => opt_row(cx.conn.query_row(
            &format!("SELECT id, name FROM {table} WHERE cur_id = ?"),
            params![i],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ))?,
        Ref::Name(n) => {
            let row: Option<(i64, String)> = opt_row(cx.conn.query_row(
                &format!("SELECT id, name FROM {table} WHERE cur_name = lower(?)"),
                params![n],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ))?;
            if let Some((id, stored)) = &row {
                cx.folds.note(table, n, stored, *id);
            }
            row
        }
    };
    Ok(row)
}

fn require_named(
    cx: &mut Ctx<'_>,
    table: &'static str,
    what: &str,
    r: &Ref,
) -> Result<i64, DbError> {
    match find_named(cx, table, r)? {
        Some((id, _)) => Ok(id),
        None => Err(DbError::Api(match r {
            Ref::Id(i) => format!("metadata: no {what} with id {i}"),
            Ref::Name(n) => format!("metadata: no {what} named {n:?}"),
        })),
    }
}

fn find_pair(cx: &mut Ctx<'_>, p: &PairRef) -> Result<Option<(i64, String, String)>, DbError> {
    match p {
        PairRef::Id(i) => opt_row(cx.conn.query_row(
            "SELECT id, name, value FROM metadata_pair WHERE cur_id = ?",
            params![i],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )),
        PairRef::Pair { name, value } => {
            let row: Option<(i64, String, String)> = opt_row(cx.conn.query_row(
                "SELECT id, name, value FROM metadata_pair \
                 WHERE cur_name = lower(?) AND cur_val = ?",
                params![name, value],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ))?;
            if let Some((id, stored, _)) = &row {
                cx.folds.note("metadata_pair", name, stored, *id);
            }
            Ok(row)
        }
    }
}

fn require_pair(cx: &mut Ctx<'_>, p: &PairRef) -> Result<i64, DbError> {
    find_pair(cx, p)?.map(|r| r.0).ok_or_else(|| {
        DbError::Api(match p {
            PairRef::Id(i) => format!("metadata: no pair with id {i}"),
            PairRef::Pair { name, value } => format!("metadata: no pair {name:?} = {value:?}"),
        })
    })
}

fn resolve_target(cx: &mut Ctx<'_>, t: &Target) -> Result<(&'static str, i64), DbError> {
    Ok(match t {
        Target::Source(r) => ("source", require_named(cx, "source", "source", r)?),
        Target::License(r) => ("license", require_named(cx, "license", "license", r)?),
        Target::Pair(p) => ("pair", require_pair(cx, p)?),
    })
}

fn resolve_role(cx: &mut Ctx<'_>, r: &RoleSel) -> Result<Option<i64>, DbError> {
    match r {
        RoleSel::NoRole => Ok(None),
        RoleSel::Named(n) => match find_named(cx, "metadata_role", &Ref::Name(n.clone()))? {
            Some((id, _)) => Ok(Some(id)),
            None => Err(DbError::Api(format!(
                "metadata: no role named {n:?}; roles are created on purpose with PutRole \
                 (recommended: author, publisher, maintainer, contributor, primary_source, \
                 adjustment, derived_from, edition_of, part_of, declared)"
            ))),
        },
    }
}

fn resolve_edge(cx: &mut Ctx<'_>, t: &Target, r: &RoleSel) -> Result<Edge, DbError> {
    let (tk, tid) = resolve_target(cx, t)?;
    Ok((tk, tid, resolve_role(cx, r)?))
}

/// Was a live row of `table` matching `pred` (one `?` bound to `name`)
/// closed by this call (`valid_to_commit = c`) with no successor (a rename
/// or revision leaves a live row of the id: not a delete)?
fn closed_here(cx: &Ctx<'_>, table: &str, pred: &str, name: &str) -> Result<bool, DbError> {
    Ok(cx.conn.query_row(
        &format!(
            "SELECT EXISTS (SELECT 1 FROM {table} WHERE valid_to_commit = ? AND {pred} \
               AND NOT EXISTS (SELECT 1 FROM {table} x WHERE x.id = {table}.id \
                 AND x.valid_to_commit IS NULL))"
        ),
        params![cx.c, name],
        |r| r.get(0),
    )?)
}

/// A subject or value this call deleted: refused under `Refuse`, dropped
/// under `DeleteWins`.
enum Resolved<T> {
    Live(T),
    Covered,
}

fn covered_or<T>(cx: &Ctx<'_>, what: String) -> Result<Resolved<T>, DbError> {
    match cx.covered {
        CoveredWrites::DeleteWins => Ok(Resolved::Covered),
        _ => Err(DbError::Api(format!(
            "metadata: {what} is deleted by this call (use CoveredWrites::DeleteWins to drop \
             the item)"
        ))),
    }
}

fn unknown<T>(what: String) -> Result<Resolved<T>, DbError> {
    Err(DbError::Api(format!("metadata: no {what}")))
}

/// A lookup of `name` in `table` (narrowed by `pred`) missed: `Covered`
/// (via [`covered_or`], naming `deleted`) when an object delete or purge of
/// this call closed it, else the "no {unknown}" error. Used at every level a
/// subject or value resolves through (class, parameter, alternative,
/// entity), so a delete that cascades from any of them drops the item.
fn missing<T>(
    cx: &Ctx<'_>,
    table: &str,
    pred: &str,
    name: &str,
    deleted: String,
    unknown_what: String,
) -> Result<Resolved<T>, DbError> {
    if closed_here(cx, table, pred, name)? {
        covered_or(cx, deleted)
    } else {
        unknown(unknown_what)
    }
}

const BY_NAME: &str = "lower(name) = lower(?)";

fn resolve_subject(
    cx: &mut Ctx<'_>,
    s: &Subject,
) -> Result<Resolved<(&'static str, i64)>, DbError> {
    let named = |cx: &mut Ctx<'_>,
                 found: Option<crate::api::Found>,
                 table: &'static str,
                 ot: &'static str,
                 name: &str|
     -> Result<Resolved<(&'static str, i64)>, DbError> {
        match found {
            Some(f) => Ok(Resolved::Live((ot, f.noted(&mut cx.folds, table, name)))),
            None if closed_here(cx, table, "lower(name) = lower(?)", name)? => {
                covered_or(cx, format!("{ot} {name:?}"))
            }
            None => unknown(format!("{ot} named {name:?}")),
        }
    };
    match s {
        Subject::Dataset => Ok(Resolved::Live(("dataset", 0))),
        Subject::Class(n) => {
            let f = find_entity_class(cx.conn, n)?;
            named(cx, f, "entity_class", "entity_class", n)
        }
        Subject::Alternative(n) => {
            let f = find_alternative(cx.conn, n)?;
            named(cx, f, "alternative", "alternative", n)
        }
        Subject::Scenario(n) => {
            let f = find_scenario(cx.conn, n)?;
            named(cx, f, "scenario", "scenario", n)
        }
        Subject::ValueList(n) => {
            let f = find_value_list(cx.conn, n)?;
            named(cx, f, "parameter_value_list", "parameter_value_list", n)
        }
        Subject::Unit(n) => match lookup_unit(cx.conn, n)? {
            Some(id) => Ok(Resolved::Live(("units_of_measure", id))),
            None => unknown(format!("unit named {n:?}")),
        },
        Subject::Entity { class, name } => {
            let Some(cid) = find_entity_class(cx.conn, class)? else {
                return missing(
                    cx,
                    "entity_class",
                    BY_NAME,
                    class,
                    format!("entity_class {class:?}"),
                    format!("class named {class:?}"),
                );
            };
            let cid = cid.noted(&mut cx.folds, "entity_class", class);
            let f = find_entity(cx.conn, cid, name)?;
            match f {
                Some(f) => Ok(Resolved::Live((
                    "entity",
                    f.noted(&mut cx.folds, "entity", name),
                ))),
                None if closed_here(
                    cx,
                    "entity",
                    &format!("entity_class_id = {cid} AND lower(name) = lower(?)"),
                    name,
                )? =>
                {
                    covered_or(cx, format!("entity {class}.{name}"))
                }
                None => unknown(format!("entity {class}.{name}")),
            }
        }
        Subject::Parameter { class, name } => {
            let Some(cid) = find_entity_class(cx.conn, class)? else {
                return missing(
                    cx,
                    "entity_class",
                    BY_NAME,
                    class,
                    format!("entity_class {class:?}"),
                    format!("class named {class:?}"),
                );
            };
            let cid = cid.noted(&mut cx.folds, "entity_class", class);
            match find_parameter(cx.conn, cid, name)? {
                Some(f) => Ok(Resolved::Live((
                    "parameter_definition",
                    f.noted(&mut cx.folds, "parameter_definition", name),
                ))),
                None if closed_here(
                    cx,
                    "parameter_definition",
                    &format!("entity_class_id = {cid} AND lower(name) = lower(?)"),
                    name,
                )? =>
                {
                    covered_or(cx, format!("parameter {class}.{name}"))
                }
                None => unknown(format!("parameter {class}.{name}")),
            }
        }
        Subject::Run(id) => {
            let n: i64 =
                cx.conn
                    .query_row("SELECT count(*) FROM run WHERE id = ?", params![id], |r| {
                        r.get(0)
                    })?;
            if n == 0 {
                return unknown(format!("run with id {id}"));
            }
            Ok(Resolved::Live(("run", *id)))
        }
        Subject::Source(r) => Ok(Resolved::Live((
            "source",
            require_named(cx, "source", "source", r)?,
        ))),
        Subject::License(r) => Ok(Resolved::Live((
            "license",
            require_named(cx, "license", "license", r)?,
        ))),
    }
}

/// A value's triple, lookup-only; `Covered` when this call deleted it.
fn resolve_value(
    cx: &mut Ctx<'_>,
    v: &ValueAddr,
    changed: &HashSet<Triple>,
) -> Result<Resolved<Triple>, DbError> {
    let label = format!(
        "{}.{} / {} / {}",
        v.class, v.parameter, v.alternative, v.entity
    );
    let Some(cid) = find_entity_class(cx.conn, &v.class)? else {
        return missing(
            cx,
            "entity_class",
            BY_NAME,
            &v.class,
            format!("value {label} (its class {:?})", v.class),
            format!("value {label} (unknown class)"),
        );
    };
    let cid = cid.noted(&mut cx.folds, "entity_class", &v.class);
    let Some(pd) = find_parameter(cx.conn, cid, &v.parameter)? else {
        return missing(
            cx,
            "parameter_definition",
            &format!("entity_class_id = {cid} AND {BY_NAME}"),
            &v.parameter,
            format!("value {label} (its parameter {:?})", v.parameter),
            format!("value {label} (unknown parameter)"),
        );
    };
    let pd = pd.noted(&mut cx.folds, "parameter_definition", &v.parameter);
    let Some(alt) = find_alternative(cx.conn, &v.alternative)? else {
        return missing(
            cx,
            "alternative",
            BY_NAME,
            &v.alternative,
            format!("value {label} (its alternative {:?})", v.alternative),
            format!("value {label} (unknown alternative)"),
        );
    };
    let alt = alt.noted(&mut cx.folds, "alternative", &v.alternative);
    let ent = match find_entity(cx.conn, cid, &v.entity)? {
        Some(f) => f.noted(&mut cx.folds, "entity", &v.entity),
        None if closed_here(
            cx,
            "entity",
            &format!("entity_class_id = {cid} AND lower(name) = lower(?)"),
            &v.entity,
        )? =>
        {
            return covered_or(cx, format!("value {label}"))
        }
        None => return unknown(format!("value {label} (unknown entity)")),
    };
    let t = (ent, pd, alt);
    if changed.contains(&t) || value_live(cx.conn, t)? {
        return Ok(Resolved::Live(t));
    }
    let closed_now: bool = cx.conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM value_scalar WHERE entity_id = ?1 \
           AND parameter_definition_id = ?2 AND alternative_id = ?3 AND valid_to_commit = ?4) \
         OR EXISTS (SELECT 1 FROM value_long WHERE entity_id = ?1 \
           AND parameter_definition_id = ?2 AND alternative_id = ?3 AND valid_to_commit = ?4) \
         OR EXISTS (SELECT 1 FROM value_array_hist WHERE entity_id = ?1 \
           AND parameter_definition_id = ?2 AND alternative_id = ?3 AND valid_to_commit = ?4)",
        params![t.0, t.1, t.2, cx.c],
        |r| r.get(0),
    )?;
    if closed_now {
        return covered_or(cx, format!("value {label}"));
    }
    unknown(format!("live value {label}"))
}

fn value_live(conn: &Connection, t: Triple) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM value_keys WHERE entity_id = ? \
           AND parameter_definition_id = ? AND alternative_id = ?)",
        params![t.0, t.1, t.2],
        |r| r.get(0),
    )?)
}

fn outcome(index: usize, kind: MetadataKind, id: i64, effect: MetadataEffect) -> MetadataOutcome {
    MetadataOutcome {
        item_index: index,
        kind,
        id,
        effect,
        links_closed: 0,
        blockers: Vec::new(),
    }
}

fn close_row(conn: &Connection, c: i64, table: &str, id: i64) -> Result<usize, DbError> {
    Ok(conn.execute(
        &format!(
            "UPDATE {table} SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL \
             AND valid_from_commit < ?"
        ),
        params![c, id, c],
    )?)
}

fn check_free_name(
    cx: &Ctx<'_>,
    table: &str,
    what: &str,
    name: &str,
    id: i64,
) -> Result<(), DbError> {
    let other: Option<i64> = opt_row(cx.conn.query_row(
        &format!("SELECT id FROM {table} WHERE cur_name = lower(?) AND id <> ?"),
        params![name, id],
        |r| r.get(0),
    ))?;
    if other.is_some() {
        return Err(DbError::Api(format!(
            "metadata: a {what} named {name:?} already exists (names are case-insensitive)"
        )));
    }
    Ok(())
}

/// `PutSource`.
fn put_source(
    cx: &mut Ctx<'_>,
    r: &Ref,
    rename: &Option<String>,
    f: &SourceFields,
) -> Result<(i64, MetadataEffect), DbError> {
    let found = find_named(cx, "source", r)?;
    let Some((id, stored)) = found else {
        let Ref::Name(name) = r else {
            return Err(DbError::Api(format!("metadata: no source with id {r:?}")));
        };
        if rename.is_some() {
            return Err(DbError::Api(format!(
                "metadata: cannot rename the unknown source {name:?}"
            )));
        }
        let id: i64 = cx.conn.query_row(
            "INSERT INTO source (name, title, path, edition, publisher, issued, retrieved, \
               citation, description, valid_from_commit) \
             VALUES (?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?) RETURNING id",
            params![
                name,
                f.title,
                f.path,
                f.edition,
                f.publisher,
                f.issued,
                f.retrieved,
                f.citation,
                f.description,
                cx.c
            ],
            |r| r.get(0),
        )?;
        return Ok((id, MetadataEffect::Created));
    };
    let name = rename.clone().unwrap_or_else(|| stored.clone());
    if name != stored {
        check_free_name(cx, "source", "source", &name, id)?;
    }
    let cur: (SourceFields, i64) = cx.conn.query_row(
        "SELECT title, path, edition, publisher, CAST(issued AS VARCHAR), \
           CAST(retrieved AS VARCHAR), citation, description, version \
         FROM source WHERE cur_id = ?",
        params![id],
        |r| {
            Ok((
                SourceFields {
                    title: r.get(0)?,
                    path: r.get(1)?,
                    edition: r.get(2)?,
                    publisher: r.get(3)?,
                    issued: r.get(4)?,
                    retrieved: r.get(5)?,
                    citation: r.get(6)?,
                    description: r.get(7)?,
                },
                r.get(8)?,
            ))
        },
    )?;
    if cur.0 == *f && name == stored {
        return Ok((id, MetadataEffect::NoOp));
    }
    close_row(cx.conn, cx.c, "source", id)?;
    cx.conn.execute(
        "INSERT INTO source (id, name, title, path, edition, publisher, issued, retrieved, \
           citation, description, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?)",
        params![
            id,
            name,
            f.title,
            f.path,
            f.edition,
            f.publisher,
            f.issued,
            f.retrieved,
            f.citation,
            f.description,
            cx.c,
            cur.1 + 1
        ],
    )?;
    Ok((
        id,
        if name != stored {
            MetadataEffect::Renamed
        } else {
            MetadataEffect::Updated
        },
    ))
}

/// `PutLicense`.
fn put_license(
    cx: &mut Ctx<'_>,
    r: &Ref,
    rename: &Option<String>,
    f: &LicenseFields,
) -> Result<(i64, MetadataEffect), DbError> {
    let found = find_named(cx, "license", r)?;
    let Some((id, stored)) = found else {
        let Ref::Name(name) = r else {
            return Err(DbError::Api(format!("metadata: no license with id {r:?}")));
        };
        if rename.is_some() {
            return Err(DbError::Api(format!(
                "metadata: cannot rename the unknown license {name:?}"
            )));
        }
        let id: i64 = cx.conn.query_row(
            "INSERT INTO license (name, title, path, spdx, policy, valid_from_commit) \
             VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
            params![name, f.title, f.path, f.spdx, f.policy, cx.c],
            |r| r.get(0),
        )?;
        return Ok((id, MetadataEffect::Created));
    };
    let name = rename.clone().unwrap_or_else(|| stored.clone());
    if name != stored {
        check_free_name(cx, "license", "license", &name, id)?;
    }
    let cur: (LicenseFields, i64) = cx.conn.query_row(
        "SELECT title, path, spdx, policy, version FROM license WHERE cur_id = ?",
        params![id],
        |r| {
            Ok((
                LicenseFields {
                    title: r.get(0)?,
                    path: r.get(1)?,
                    spdx: r.get(2)?,
                    policy: r.get(3)?,
                },
                r.get(4)?,
            ))
        },
    )?;
    if cur.0 == *f && name == stored {
        return Ok((id, MetadataEffect::NoOp));
    }
    close_row(cx.conn, cx.c, "license", id)?;
    cx.conn.execute(
        "INSERT INTO license (id, name, title, path, spdx, policy, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![id, name, f.title, f.path, f.spdx, f.policy, cx.c, cur.1 + 1],
    )?;
    Ok((
        id,
        if name != stored {
            MetadataEffect::Renamed
        } else {
            MetadataEffect::Updated
        },
    ))
}

/// `PutRole`.
fn put_role(
    cx: &mut Ctx<'_>,
    r: &Ref,
    rename: &Option<String>,
    description: &Option<String>,
    iri: &Option<String>,
) -> Result<(i64, MetadataEffect), DbError> {
    let found = find_named(cx, "metadata_role", r)?;
    let Some((id, stored)) = found else {
        let Ref::Name(name) = r else {
            return Err(DbError::Api(format!("metadata: no role with id {r:?}")));
        };
        if rename.is_some() {
            return Err(DbError::Api(format!(
                "metadata: cannot rename the unknown role {name:?}"
            )));
        }
        let id: i64 = cx.conn.query_row(
            "INSERT INTO metadata_role (name, description, iri, valid_from_commit) \
             VALUES (?, ?, ?, ?) RETURNING id",
            params![name, description, iri, cx.c],
            |r| r.get(0),
        )?;
        return Ok((id, MetadataEffect::Created));
    };
    let name = rename.clone().unwrap_or_else(|| stored.clone());
    if name != stored {
        check_free_name(cx, "metadata_role", "role", &name, id)?;
    }
    let cur: (Option<String>, Option<String>, i64) = cx.conn.query_row(
        "SELECT description, iri, version FROM metadata_role WHERE cur_id = ?",
        params![id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if cur.0 == *description && cur.1 == *iri && name == stored {
        return Ok((id, MetadataEffect::NoOp));
    }
    close_row(cx.conn, cx.c, "metadata_role", id)?;
    cx.conn.execute(
        "INSERT INTO metadata_role (id, name, description, iri, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?)",
        params![id, name, description, iri, cx.c, cur.2 + 1],
    )?;
    Ok((
        id,
        if name != stored {
            MetadataEffect::Renamed
        } else {
            MetadataEffect::Updated
        },
    ))
}

/// `CreatePair`: the existing key spelling wins (`NameFolded`).
fn create_pair(
    cx: &mut Ctx<'_>,
    name: &str,
    value: &str,
) -> Result<(i64, MetadataEffect), DbError> {
    let stored: Option<(i64, String)> = opt_row(cx.conn.query_row(
        "SELECT id, name FROM metadata_pair WHERE cur_name = lower(?) ORDER BY id LIMIT 1",
        params![name],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ))?;
    let key = match &stored {
        Some((id, s)) => {
            cx.folds.note("metadata_pair", name, s, *id);
            s.clone()
        }
        None => name.to_string(),
    };
    if let Some((id, _, _)) = find_pair(
        cx,
        &PairRef::Pair {
            name: key.clone(),
            value: value.to_string(),
        },
    )? {
        return Ok((id, MetadataEffect::NoOp));
    }
    let version: i64 = 1;
    let id: i64 = cx.conn.query_row(
        "INSERT INTO metadata_pair (name, value, valid_from_commit, version) \
         VALUES (?, ?, ?, ?) RETURNING id",
        params![key, value, cx.c, version],
        |r| r.get(0),
    )?;
    Ok((id, MetadataEffect::Created))
}

fn revise_pair(
    cx: &mut Ctx<'_>,
    p: &PairRef,
    value: &str,
) -> Result<(i64, MetadataEffect), DbError> {
    let Some((id, name, cur)) = find_pair(cx, p)? else {
        require_pair(cx, p)?;
        unreachable!("require_pair errors on a missing pair");
    };
    if cur == value {
        return Ok((id, MetadataEffect::NoOp));
    }
    let clash: bool = cx.conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM metadata_pair WHERE cur_name = lower(?) AND cur_val = ?)",
        params![name, value],
        |r| r.get(0),
    )?;
    if clash {
        return Err(DbError::Api(format!(
            "metadata: the pair {name:?} = {value:?} already exists"
        )));
    }
    let version: i64 = cx.conn.query_row(
        "SELECT version FROM metadata_pair WHERE cur_id = ?",
        params![id],
        |r| r.get(0),
    )?;
    close_row(cx.conn, cx.c, "metadata_pair", id)?;
    cx.conn.execute(
        "INSERT INTO metadata_pair (id, name, value, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?)",
        params![id, name, value, cx.c, version + 1],
    )?;
    Ok((id, MetadataEffect::Updated))
}

/// `RenamePairKey`: every live pair of the key `current`, in one item.
/// Returns the pair ids renamed.
fn rename_pair_key(cx: &mut Ctx<'_>, from: &str, to: &str) -> Result<Vec<i64>, DbError> {
    let mut stmt = cx.conn.prepare(
        "SELECT id, name, value, version FROM metadata_pair WHERE cur_name = lower(?) ORDER BY id",
    )?;
    let pairs: Vec<(i64, String, String, i64)> = stmt
        .query_map(params![from], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    if pairs.iter().all(|p| p.1 == to) {
        return Ok(Vec::new());
    }
    if fold(from) != fold(to) {
        for (_, _, v, _) in &pairs {
            let clash: bool = cx.conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM metadata_pair WHERE cur_name = lower(?) \
                   AND cur_val = ?)",
                params![to, v],
                |r| r.get(0),
            )?;
            if clash {
                return Err(DbError::Api(format!(
                    "metadata: renaming the pair key {from:?} to {to:?} collides with the \
                     existing pair {to:?} = {v:?}"
                )));
            }
        }
    }
    let mut ids = Vec::new();
    for (id, _, v, version) in &pairs {
        close_row(cx.conn, cx.c, "metadata_pair", *id)?;
        cx.conn.execute(
            "INSERT INTO metadata_pair (id, name, value, valid_from_commit, version) \
             VALUES (?, ?, ?, ?, ?)",
            params![id, to, v, cx.c, version + 1],
        )?;
        ids.push(*id);
    }
    Ok(ids)
}

/// `SetSourceLicenses`: the exact set.
fn set_source_licenses(
    cx: &mut Ctx<'_>,
    source: i64,
    licenses: &[i64],
) -> Result<MetadataEffect, DbError> {
    let mut stmt = cx.conn.prepare(
        "SELECT license_id FROM source_license WHERE source_id = ? AND valid_to_commit IS NULL",
    )?;
    let cur: BTreeSet<i64> = stmt
        .query_map(params![source], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let want: BTreeSet<i64> = licenses.iter().copied().collect();
    if cur == want {
        return Ok(MetadataEffect::NoOp);
    }
    for l in cur.difference(&want) {
        cx.conn.execute(
            "UPDATE source_license SET valid_to_commit = ? WHERE source_id = ? AND license_id = ? \
             AND valid_to_commit IS NULL AND valid_from_commit < ?",
            params![cx.c, source, l, cx.c],
        )?;
    }
    for l in want.difference(&cur) {
        cx.conn.execute(
            "INSERT INTO source_license (source_id, license_id, valid_from_commit, version) \
             SELECT ?1, ?2, ?3, coalesce(max(version), 0) + 1 FROM source_license \
             WHERE source_id = ?1 AND license_id = ?2",
            params![source, l, cx.c],
        )?;
    }
    Ok(MetadataEffect::Updated)
}

/// Insert one object link (version continues the edge's history).
fn insert_object_link(cx: &Ctx<'_>, ot: &str, oid: i64, e: Edge) -> Result<(), DbError> {
    cx.conn.execute(
        "INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id, \
           role_id, valid_from_commit, version) \
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, coalesce(max(version), 0) + 1 FROM object_metadata_link \
         WHERE object_type = ?1 AND object_id = ?2 AND target_kind = ?3 AND target_id = ?4 \
           AND coalesce(role_id, 0) = coalesce(?5, 0)",
        params![ot, oid, e.0, e.1, e.2, cx.c],
    )?;
    Ok(())
}

fn object_link_live(cx: &Ctx<'_>, ot: &str, oid: i64, e: Edge) -> Result<bool, DbError> {
    Ok(cx.conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM object_metadata_link WHERE valid_to_commit IS NULL \
           AND object_type = ? AND object_id = ? AND target_kind = ? AND target_id = ? \
           AND coalesce(role_id, 0) = coalesce(?, 0))",
        params![ot, oid, e.0, e.1, e.2],
        |r| r.get(0),
    )?)
}

/// A resolved pool delete (M4), resolved before M2 so a keep never reopens
/// a link to a target this call deletes with cascade.
struct PoolDelete {
    index: usize,
    kind: MetadataKind,
    id: i64,
    cascade: bool,
}

/// The legs of a pool target's delete: (table, predicate, blocks?).
/// Identity legs close with the object; blocking legs restrict a delete
/// without cascade and close with one. The pool's counterpart of
/// `object_delete::object_legs`.
fn target_legs(kind: MetadataKind, id: i64) -> Vec<(&'static str, String, bool)> {
    let tk = match kind {
        MetadataKind::Source => "source",
        MetadataKind::License => "license",
        MetadataKind::Pair => "pair",
        _ => "",
    };
    let mut legs = Vec::new();
    match kind {
        MetadataKind::Role => {
            legs.push(("object_metadata_link", format!("role_id = {id}"), true));
            legs.push(("value_metadata_link", format!("role_id = {id}"), true));
            return legs;
        }
        MetadataKind::Source => {
            legs.push(("source_license", format!("source_id = {id}"), false));
            legs.push((
                "object_metadata_link",
                format!("object_type = 'source' AND object_id = {id}"),
                false,
            ));
            legs.push((
                "value_metadata_link",
                format!("target_kind = 'source' AND target_id = {id}"),
                true,
            ));
        }
        MetadataKind::License => {
            legs.push(("source_license", format!("license_id = {id}"), true));
            legs.push((
                "object_metadata_link",
                format!("object_type = 'license' AND object_id = {id}"),
                false,
            ));
        }
        MetadataKind::Pair => {
            legs.push((
                "value_metadata_link",
                format!("target_kind = 'pair' AND target_id = {id}"),
                true,
            ));
        }
        _ => {}
    }
    legs.push((
        "object_metadata_link",
        format!("target_kind = '{tk}' AND target_id = {id}"),
        true,
    ));
    legs
}

fn pool_table(kind: MetadataKind) -> &'static str {
    match kind {
        MetadataKind::Source => "source",
        MetadataKind::License => "license",
        MetadataKind::Pair => "metadata_pair",
        _ => "metadata_role",
    }
}

/// M4: one pool delete on the post-call state.
fn run_pool_delete(cx: &Ctx<'_>, d: &PoolDelete) -> Result<MetadataOutcome, DbError> {
    let legs = target_legs(d.kind, d.id);
    let mut blockers = Vec::new();
    for (table, pred, blocks) in &legs {
        if !blocks {
            continue;
        }
        let n: i64 = cx.conn.query_row(
            &format!("SELECT count(*) FROM {table} WHERE valid_to_commit IS NULL AND {pred}"),
            [],
            |r| r.get(0),
        )?;
        if n > 0 {
            let mut stmt = cx.conn.prepare(&format!(
                "SELECT {} FROM {table} WHERE valid_to_commit IS NULL AND {pred} LIMIT 10",
                match *table {
                    "source_license" => "'source=' || source_id",
                    "value_metadata_link" =>
                        "'entity=' || entity_id || ',pd=' || parameter_definition_id \
                         || ',alt=' || alternative_id",
                    _ => "object_type || '=' || object_id",
                }
            ))?;
            let sample = stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            blockers.push(Blocker {
                table: table.to_string(),
                column: pred.clone(),
                count: n as usize,
                sample,
            });
        }
    }
    let mut out = outcome(d.index, d.kind, d.id, MetadataEffect::Deleted);
    if !blockers.is_empty() && (!d.cascade || d.kind == MetadataKind::Role) {
        out.effect = MetadataEffect::Restricted;
        out.blockers = blockers;
        return Ok(out);
    }
    for (table, pred, _) in &legs {
        out.links_closed += cx.conn.execute(
            &format!(
                "UPDATE {table} SET valid_to_commit = ? WHERE valid_to_commit IS NULL \
                 AND valid_from_commit < ? AND {pred}"
            ),
            params![cx.c, cx.c],
        )?;
    }
    if close_row(cx.conn, cx.c, pool_table(d.kind), d.id)? == 0 {
        // Created by this call (the same-commit guard): nothing to delete.
        out.effect = MetadataEffect::NoOp;
    }
    Ok(out)
}

/// The live links of `triples`: triple → [(edge, version)].
fn live_value_links(
    conn: &Connection,
    triples: &[Triple],
) -> Result<HashMap<Triple, Vec<Edge>>, DbError> {
    let mut out: HashMap<Triple, Vec<Edge>> = HashMap::new();
    for chunk in triples.chunks(crate::history::LONG_INSERT_CHUNK) {
        let set = TripleSet::Lists(chunk);
        let mut stmt = conn.prepare(&format!(
            "SELECT value_metadata_link.entity_id, value_metadata_link.parameter_definition_id, \
               value_metadata_link.alternative_id, target_kind, target_id, role_id \
             FROM value_metadata_link, {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit IS NULL",
            set.source_sql()
        ))?;
        let rows = stmt.query_map(duckdb::params_from_iter(set.binds().iter()), |r| {
            Ok((
                (
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ),
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
            ))
        })?;
        for row in rows {
            let (t, tk, tid, role) = row?;
            let tk: &'static str = if tk == "source" { "source" } else { "pair" };
            out.entry(t).or_default().push((tk, tid, role));
        }
    }
    Ok(out)
}

/// Insert value links (set-based): one row per `(triple, edge)`, the
/// version continuing the edge's history.
fn insert_value_links(
    conn: &Connection,
    c: i64,
    rows: &[(Triple, Edge)],
) -> Result<usize, DbError> {
    use duckdb::types::Value;
    let mut n = 0;
    for chunk in rows.chunks(crate::history::LONG_INSERT_CHUNK) {
        let list =
            |f: &dyn Fn(&(Triple, Edge)) -> Value| Value::List(chunk.iter().map(f).collect());
        let binds = [
            list(&|r| Value::BigInt(r.0 .0)),
            list(&|r| Value::BigInt(r.0 .1)),
            list(&|r| Value::BigInt(r.0 .2)),
            list(&|r| Value::Text(r.1 .0.to_string())),
            list(&|r| Value::BigInt(r.1 .1)),
            list(&|r| r.1 .2.map(Value::BigInt).unwrap_or(Value::Null)),
            Value::BigInt(c),
        ];
        n += conn.execute(
            "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, alternative_id, \
               target_kind, target_id, role_id, valid_from_commit, version) \
             SELECT k.e, k.p, k.a, k.tk, k.t, k.r, ?7, \
                    coalesce((SELECT max(x.version) FROM value_metadata_link x \
                       WHERE x.entity_id = k.e AND x.parameter_definition_id = k.p \
                         AND x.alternative_id = k.a AND x.target_kind = k.tk \
                         AND x.target_id = k.t AND coalesce(x.role_id, 0) = coalesce(k.r, 0)), \
                             0) + 1 \
             FROM (SELECT unnest(?1::BIGINT[]) AS e, unnest(?2::BIGINT[]) AS p, \
                          unnest(?3::BIGINT[]) AS a, unnest(?4::VARCHAR[]) AS tk, \
                          unnest(?5::BIGINT[]) AS t, unnest(?6::BIGINT[]) AS r) k",
            duckdb::params_from_iter(binds.iter()),
        )?;
    }
    Ok(n)
}

/// Close specific value links (an edit of an unchanged value).
fn close_value_edges(conn: &Connection, c: i64, rows: &[(Triple, Edge)]) -> Result<usize, DbError> {
    use duckdb::types::Value;
    let mut n = 0;
    for chunk in rows.chunks(crate::history::LONG_INSERT_CHUNK) {
        let list =
            |f: &dyn Fn(&(Triple, Edge)) -> Value| Value::List(chunk.iter().map(f).collect());
        let binds = [
            Value::BigInt(c),
            list(&|r| Value::BigInt(r.0 .0)),
            list(&|r| Value::BigInt(r.0 .1)),
            list(&|r| Value::BigInt(r.0 .2)),
            list(&|r| Value::Text(r.1 .0.to_string())),
            list(&|r| Value::BigInt(r.1 .1)),
            list(&|r| Value::BigInt(r.1 .2.unwrap_or(0))),
            Value::BigInt(c),
        ];
        n += conn.execute(
            "UPDATE value_metadata_link SET valid_to_commit = ?1 \
             FROM (SELECT unnest(?2::BIGINT[]) AS e, unnest(?3::BIGINT[]) AS p, \
                          unnest(?4::BIGINT[]) AS a, unnest(?5::VARCHAR[]) AS tk, \
                          unnest(?6::BIGINT[]) AS t, unnest(?7::BIGINT[]) AS r) k \
             WHERE value_metadata_link.entity_id = k.e \
               AND value_metadata_link.parameter_definition_id = k.p \
               AND value_metadata_link.alternative_id = k.a \
               AND value_metadata_link.target_kind = k.tk \
               AND value_metadata_link.target_id = k.t \
               AND coalesce(value_metadata_link.role_id, 0) = k.r \
               AND value_metadata_link.valid_to_commit IS NULL \
               AND value_metadata_link.valid_from_commit < ?8",
            duckdb::params_from_iter(binds.iter()),
        )?;
    }
    Ok(n)
}

/// `"class.entity / parameter / alternative"` names of up to
/// [`UNSTATED_SAMPLE`] triples, for the refusal.
fn triple_names(conn: &Connection, ts: &[Triple]) -> Result<Vec<String>, DbError> {
    let mut out = Vec::new();
    for t in ts.iter().take(UNSTATED_SAMPLE) {
        let s: String = conn.query_row(
            "SELECT ec.name || '.' || e.name || ' / ' || pd.name || ' / ' || a.name \
             FROM entity e JOIN entity_class ec ON ec.id = e.entity_class_id \
                  AND ec.valid_to_commit IS NULL, parameter_definition pd, alternative a \
             WHERE e.id = ? AND e.valid_to_commit IS NULL AND pd.id = ? \
               AND pd.valid_to_commit IS NULL AND a.id = ? AND a.valid_to_commit IS NULL",
            params![t.0, t.1, t.2],
            |r| r.get(0),
        )?;
        out.push(s);
    }
    Ok(out)
}

/// The metadata phase of one call (M0–M4). Appends the outcomes to
/// `report.metadata` and the value-link counts to `report.value_links`.
pub(crate) fn apply_channel(
    conn: &Connection,
    c: i64,
    call: &ChannelCall<'_>,
    report: &mut WriteReport,
) -> Result<(), DbError> {
    let mut counts = ValueLinkCounts::default();
    if call.had_links {
        // Links the call's value deletes, object deletes and purges closed.
        counts.closed_by_delete = conn.query_row(
            "SELECT count(*) FROM value_metadata_link WHERE valid_to_commit = ?",
            params![c],
            |r| r.get::<_, i64>(0),
        )? as usize;
    }
    let mut cx = Ctx {
        conn,
        c,
        folds: NameFolds::default(),
        covered: call.covered_writes,
    };
    let items = call.items;
    let mut outs: Vec<Option<MetadataOutcome>> = vec![None; items.len()];
    // Objects revised / renamed / deleted (kind, id) by item.
    let mut touched: HashMap<(MetadataKind, i64), usize> = HashMap::new();
    // Objects an item of this call created (kind, id): links may name them,
    // but no other item may revise, rename or delete them (the row is this
    // commit's own, so a close-out would match nothing).
    let mut created: HashMap<(MetadataKind, i64), usize> = HashMap::new();
    // Sources and licenses a SetSourceLicenses item writes rows for.
    let mut licensed: HashMap<(MetadataKind, i64), usize> = HashMap::new();
    // Claim an existing object for item `i` before revising it: a second
    // item on it (an id and a name can alias) is refused, never a raw
    // unique-key error.
    fn claim(
        touched: &mut HashMap<(MetadataKind, i64), usize>,
        created: &HashMap<(MetadataKind, i64), usize>,
        key: (MetadataKind, i64),
        i: usize,
    ) -> Result<(), DbError> {
        refuse_created(created, key, i)?;
        match touched.insert(key, i) {
            Some(j) if j != i => Err(DbError::Api(format!(
                "metadata items {j} and {i} both revise, rename or delete the same object \
                 (resolved by id); one item per object per call"
            ))),
            _ => Ok(()),
        }
    }
    // A second item on an object an earlier item of this call created.
    fn refuse_created(
        created: &HashMap<(MetadataKind, i64), usize>,
        key: (MetadataKind, i64),
        i: usize,
    ) -> Result<(), DbError> {
        match created.get(&key) {
            Some(&j) if j != i => Err(DbError::Api(format!(
                "metadata item {i} revises, renames or deletes the object that item {j} \
                 creates (resolved by id); one item per object per call: create it in its \
                 final form, or change it in a later call"
            ))),
            _ => Ok(()),
        }
    }
    // ---- M1: pool creates and revisions, in item order.
    for (i, it) in items.iter().enumerate() {
        let o = match it {
            MetadataChange::PutSource {
                source,
                new_name: rename,
                fields,
            } => {
                if let Some((id, _)) = find_named(&mut cx, "source", source)? {
                    claim(&mut touched, &created, (MetadataKind::Source, id), i)?;
                }
                let (id, e) = put_source(&mut cx, source, rename, fields)?;
                if e == MetadataEffect::Created {
                    created.insert((MetadataKind::Source, id), i);
                } else {
                    touched.insert((MetadataKind::Source, id), i);
                }
                outcome(i, MetadataKind::Source, id, e)
            }
            MetadataChange::PutLicense {
                license,
                new_name: rename,
                fields,
            } => {
                if let Some((id, _)) = find_named(&mut cx, "license", license)? {
                    claim(&mut touched, &created, (MetadataKind::License, id), i)?;
                }
                let (id, e) = put_license(&mut cx, license, rename, fields)?;
                if e == MetadataEffect::Created {
                    created.insert((MetadataKind::License, id), i);
                } else {
                    touched.insert((MetadataKind::License, id), i);
                }
                outcome(i, MetadataKind::License, id, e)
            }
            MetadataChange::PutRole {
                role,
                new_name: rename,
                description,
                iri,
            } => {
                if let Some((id, _)) = find_named(&mut cx, "metadata_role", role)? {
                    claim(&mut touched, &created, (MetadataKind::Role, id), i)?;
                }
                let (id, e) = put_role(&mut cx, role, rename, description, iri)?;
                if e == MetadataEffect::Created {
                    created.insert((MetadataKind::Role, id), i);
                } else {
                    touched.insert((MetadataKind::Role, id), i);
                }
                outcome(i, MetadataKind::Role, id, e)
            }
            MetadataChange::CreatePair { name, value } => {
                let (id, e) = create_pair(&mut cx, name, value)?;
                if e == MetadataEffect::Created {
                    created.insert((MetadataKind::Pair, id), i);
                }
                outcome(i, MetadataKind::Pair, id, e)
            }
            MetadataChange::RevisePair { pair, value } => {
                if let Some((id, _, _)) = find_pair(&mut cx, pair)? {
                    claim(&mut touched, &created, (MetadataKind::Pair, id), i)?;
                }
                let (id, e) = revise_pair(&mut cx, pair, value)?;
                touched.insert((MetadataKind::Pair, id), i);
                outcome(i, MetadataKind::Pair, id, e)
            }
            MetadataChange::RenamePairKey {
                current: from,
                new_name: to,
            } => {
                let mut stmt =
                    conn.prepare("SELECT id FROM metadata_pair WHERE cur_name = lower(?)")?;
                let held: Vec<i64> = stmt
                    .query_map(params![from], |r| r.get(0))?
                    .collect::<Result<_, _>>()?;
                for id in held {
                    claim(&mut touched, &created, (MetadataKind::Pair, id), i)?;
                }
                let ids = rename_pair_key(&mut cx, from, to)?;
                for id in &ids {
                    touched.insert((MetadataKind::Pair, *id), i);
                }
                let e = if ids.is_empty() {
                    MetadataEffect::NoOp
                } else {
                    MetadataEffect::Renamed
                };
                outcome(i, MetadataKind::Pair, 0, e)
            }
            MetadataChange::SetProperty { key, value } => {
                use crate::property::PropertyEffect as P;
                let e = match crate::property::set_user(conn, c, key, value.as_deref())? {
                    P::Created => MetadataEffect::Created,
                    P::Updated => MetadataEffect::Updated,
                    P::Cleared => MetadataEffect::Cleared,
                    P::NoOp => MetadataEffect::NoOp,
                };
                outcome(i, MetadataKind::Property, 0, e)
            }
            MetadataChange::SetSourceLicenses { source, licenses } => {
                let sid = require_named(&mut cx, "source", "source", source)?;
                let mut lids = Vec::new();
                for l in licenses {
                    lids.push(require_named(&mut cx, "license", "license", l)?);
                }
                // A delete of this source or of a listed license in the
                // same call (by an aliasing id / name) would leave the new
                // rows live and dangling: refused below.
                // One SetSourceLicenses per source (resolved: an id and a
                // name can alias), else the two exact sets would merge.
                if let Some(j) = licensed.insert((MetadataKind::Source, sid), i) {
                    return Err(DbError::Api(format!(
                        "metadata items {j} and {i} both set the licenses of one source \
                         (resolved by id); one item per object per call"
                    )));
                }
                for l in &lids {
                    licensed.insert((MetadataKind::License, *l), i);
                }
                let e = set_source_licenses(&mut cx, sid, &lids)?;
                outcome(i, MetadataKind::SourceLicenses, sid, e)
            }
            _ => continue,
        };
        outs[i] = Some(o);
    }
    // ---- Resolve the pool deletes (before M2: a keep never reopens a link
    // to a target deleted with cascade).
    let mut deletes: Vec<PoolDelete> = Vec::new();
    for (i, it) in items.iter().enumerate() {
        let (kind, id, cascade) = match it {
            MetadataChange::DeleteSource { source, cascade } => (
                MetadataKind::Source,
                require_named(&mut cx, "source", "source", source)?,
                *cascade,
            ),
            MetadataChange::DeleteLicense { license, cascade } => (
                MetadataKind::License,
                require_named(&mut cx, "license", "license", license)?,
                *cascade,
            ),
            MetadataChange::DeletePair { pair, cascade } => {
                (MetadataKind::Pair, require_pair(&mut cx, pair)?, *cascade)
            }
            MetadataChange::DeleteRole { role } => (
                MetadataKind::Role,
                require_named(&mut cx, "metadata_role", "role", role)?,
                false,
            ),
            _ => continue,
        };
        if let Some(j) = licensed.get(&(kind, id)) {
            return Err(DbError::Api(format!(
                "metadata items {j} and {i}: item {i} deletes a source or license that item \
                 {j} sets licenses for (resolved by id); one item per object per call"
            )));
        }
        refuse_created(&created, (kind, id), i)?;
        if let Some(j) = touched.insert((kind, id), i) {
            return Err(DbError::Api(format!(
                "metadata items {j} and {i} both revise, rename or delete the same object \
                 (resolved by id); one item per object per call"
            )));
        }
        deletes.push(PoolDelete {
            index: i,
            kind,
            id,
            cascade,
        });
    }
    let doomed: HashSet<(&'static str, i64)> = deletes
        .iter()
        .filter(|d| d.cascade)
        .filter_map(|d| match d.kind {
            MetadataKind::Source => Some(("source", d.id)),
            MetadataKind::Pair => Some(("pair", d.id)),
            MetadataKind::License => Some(("license", d.id)),
            _ => None,
        })
        .collect();
    let kind_of = |tk: &str| match tk {
        "source" => MetadataKind::Source,
        "license" => MetadataKind::License,
        _ => MetadataKind::Pair,
    };
    // The one-item-per-object rule on resolved ids: a link item may not
    // name an object another item revises, renames or deletes.
    let check_touch = |i: usize, e: &Edge| -> Result<(), DbError> {
        let mut hits = vec![(kind_of(e.0), e.1)];
        if let Some(r) = e.2 {
            hits.push((MetadataKind::Role, r));
        }
        for h in hits {
            if let Some(j) = touched.get(&h) {
                if *j != i {
                    return Err(DbError::Api(format!(
                        "metadata item {i} names an object that item {j} revises, renames or \
                         deletes; one item per object per call"
                    )));
                }
            }
        }
        Ok(())
    };

    // ---- M2: value statements and value-edge items.
    let changed: HashSet<Triple> = call.changed.iter().copied().collect();
    let mut save: Option<(usize, &Action)> = None;
    let mut by_target: HashMap<(i64, i64), (usize, &Action)> = HashMap::new();
    let mut by_value: HashMap<Triple, (usize, &Action)> = HashMap::new();
    let mut adds: HashMap<Triple, Vec<(usize, Edge)>> = HashMap::new();
    let mut removes: HashMap<Triple, Vec<(usize, Edge)>> = HashMap::new();
    let mut set_edges: HashMap<usize, Vec<Edge>> = HashMap::new();
    for (i, it) in items.iter().enumerate() {
        match it {
            MetadataChange::ValueMetadata { scope, action } => {
                if let Action::Set(ls) = action {
                    let mut es = Vec::new();
                    for l in ls {
                        let e = resolve_edge(&mut cx, &l.target, &l.role)?;
                        check_touch(i, &e)?;
                        es.push(e);
                    }
                    set_edges.insert(i, es);
                }
                match scope {
                    Scope::Save => save = Some((i, action)),
                    Scope::Target {
                        class,
                        parameter,
                        alternative,
                    } => {
                        let label = format!("{class}.{parameter} / {alternative}");
                        let cid = find_entity_class(conn, class)?.ok_or_else(|| {
                            DbError::Api(format!("metadata item {i}: no target {label}"))
                        })?;
                        let cid = cid.noted(&mut cx.folds, "entity_class", class);
                        let pd = find_parameter(conn, cid, parameter)?.ok_or_else(|| {
                            DbError::Api(format!("metadata item {i}: no target {label}"))
                        })?;
                        let pd = pd.noted(&mut cx.folds, "parameter_definition", parameter);
                        let alt = find_alternative(conn, alternative)?.ok_or_else(|| {
                            DbError::Api(format!("metadata item {i}: no target {label}"))
                        })?;
                        let alt = alt.noted(&mut cx.folds, "alternative", alternative);
                        if by_target.insert((pd, alt), (i, action)).is_some() {
                            return Err(DbError::Api(format!(
                                "metadata item {i}: a second statement for the target {label}"
                            )));
                        }
                        outs[i] = Some(outcome(
                            i,
                            MetadataKind::ValueMetadata,
                            0,
                            MetadataEffect::NoOp,
                        ));
                    }
                    Scope::Value(v) => match resolve_value(&mut cx, v, &changed)? {
                        Resolved::Live(t) => {
                            if by_value.insert(t, (i, action)).is_some() {
                                return Err(DbError::Api(format!(
                                    "metadata item {i}: a second statement for the same value"
                                )));
                            }
                        }
                        Resolved::Covered => {
                            outs[i] = Some(outcome(
                                i,
                                MetadataKind::ValueMetadata,
                                0,
                                MetadataEffect::DroppedByDelete,
                            ));
                        }
                    },
                }
                if outs[i].is_none() {
                    outs[i] = Some(outcome(
                        i,
                        MetadataKind::ValueMetadata,
                        0,
                        MetadataEffect::NoOp,
                    ));
                }
            }
            MetadataChange::LinkValue { value, link } => {
                let e = resolve_edge(&mut cx, &link.target, &link.role)?;
                check_touch(i, &e)?;
                match resolve_value(&mut cx, value, &changed)? {
                    Resolved::Live(t) => {
                        adds.entry(t).or_default().push((i, e));
                        outs[i] =
                            Some(outcome(i, MetadataKind::ValueLink, 0, MetadataEffect::NoOp));
                    }
                    Resolved::Covered => {
                        outs[i] = Some(outcome(
                            i,
                            MetadataKind::ValueLink,
                            0,
                            MetadataEffect::DroppedByDelete,
                        ))
                    }
                }
            }
            MetadataChange::UnlinkValue {
                value,
                target,
                role,
            } => {
                let e = resolve_edge(&mut cx, target, role)?;
                match resolve_value(&mut cx, value, &changed)? {
                    Resolved::Live(t) => {
                        removes.entry(t).or_default().push((i, e));
                        outs[i] =
                            Some(outcome(i, MetadataKind::ValueLink, 0, MetadataEffect::NoOp));
                    }
                    Resolved::Covered => {
                        outs[i] = Some(outcome(i, MetadataKind::ValueLink, 0, MetadataEffect::NoOp))
                    }
                }
            }
            _ => {}
        }
    }
    for t in by_value.keys() {
        if matches!(by_value[t].1, Action::Set(_))
            && (adds.contains_key(t) || removes.contains_key(t))
        {
            return Err(DbError::Api(format!(
                "metadata item {}: a value-scope set statement and link / unlink items name the \
                 same value; a set is the exact link list",
                by_value[t].0
            )));
        }
    }
    // The values to look at: every changed value of a linked target (or of
    // any target under a bulk set), every value a statement or edge names.
    let mut candidates: Vec<Triple> = call.changed.to_vec();
    for t in by_value.keys().chain(adds.keys()).chain(removes.keys()) {
        if !changed.contains(t) {
            candidates.push(*t);
        }
    }
    let old = if call.had_links {
        live_value_links(conn, &candidates)?
    } else {
        HashMap::new()
    };
    let empty: Vec<Edge> = Vec::new();
    let mut unstated: Vec<Triple> = Vec::new();
    let mut close_whole: Vec<Triple> = Vec::new();
    let mut close_edges: Vec<(Triple, Edge)> = Vec::new();
    let mut inserts: Vec<(Triple, Edge)> = Vec::new();
    // Per item: did it change anything (for its outcome)?
    let mut item_hit: HashSet<usize> = HashSet::new();
    let mut seen: HashSet<Triple> = HashSet::new();
    for t in &candidates {
        if !seen.insert(*t) {
            continue;
        }
        let is_changed = changed.contains(t);
        let old_edges = old.get(t).unwrap_or(&empty);
        let stmt = by_value.get(t).copied().or_else(|| {
            if is_changed {
                by_target.get(&(t.1, t.2)).copied().or(save)
            } else {
                None
            }
        });
        if is_changed && !old_edges.is_empty() && stmt.is_none() {
            unstated.push(*t);
            continue;
        }
        let mut fin: Vec<Edge> = match stmt {
            Some((_, Action::Keep)) | None => old_edges.clone(),
            Some((_, Action::Clear)) => Vec::new(),
            Some((i, Action::Set(_))) => set_edges[&i].clone(),
        };
        // A keep never reopens a link to a target deleted with cascade.
        fin.retain(|e| !doomed.contains(&(e.0, e.1)));
        if let Some(a) = adds.get(t) {
            for (i, e) in a {
                if !fin.contains(e) {
                    fin.push(*e);
                    item_hit.insert(*i);
                }
            }
        }
        if let Some(r) = removes.get(t) {
            for (i, e) in r {
                if let Some(pos) = fin.iter().position(|x| x == e) {
                    fin.remove(pos);
                    item_hit.insert(*i);
                } else {
                    report.warnings.push(Warning {
                        kind: WarningKind::NothingToDelete { key_index: *i },
                        object_type: "value_metadata_link".into(),
                        name: String::new(),
                        id: 0,
                    });
                }
            }
        }
        let kept = fin.iter().filter(|e| old_edges.contains(e)).count();
        let added: Vec<Edge> = fin
            .iter()
            .filter(|e| !old_edges.contains(e))
            .copied()
            .collect();
        let removed: Vec<Edge> = old_edges
            .iter()
            .filter(|e| !fin.contains(e))
            .copied()
            .collect();
        if is_changed {
            // A new version: close every old link, insert the final set.
            if !old_edges.is_empty() {
                close_whole.push(*t);
            }
            inserts.extend(fin.iter().map(|e| (*t, *e)));
            counts.kept += kept;
        } else {
            close_edges.extend(removed.iter().map(|e| (*t, *e)));
            inserts.extend(added.iter().map(|e| (*t, *e)));
        }
        counts.set += added.len();
        counts.cleared += removed.len();
        if let Some((i, _)) = stmt {
            if !added.is_empty() || !removed.is_empty() || (is_changed && kept > 0) {
                item_hit.insert(i);
            }
        }
    }
    if !unstated.is_empty() {
        let count = unstated.len();
        let sample = triple_names(conn, &unstated)?;
        return Err(DbError::ValueMetadataUnstated {
            message: format!(
                "{count} changed value(s) carry metadata links and no keep / set / clear \
                 statement says what their new version keeps (e.g. {}): state it for the save \
                 (ValueMetadata {{ scope: Save, .. }}; the CLI's --value-metadata keep|clear; \
                 the skin's value_metadata=), per target (Scope::Target) or per value \
                 (Scope::Value); nothing was written",
                sample.join("; ")
            ),
            count,
            sample,
        });
    }
    for chunk in close_whole.chunks(crate::history::LONG_INSERT_CHUNK) {
        close_links(conn, c, &TripleSet::Lists(chunk))?;
    }
    close_value_edges(conn, c, &close_edges)?;
    insert_value_links(conn, c, &inserts)?;
    for (i, o) in outs.iter_mut().enumerate() {
        if let Some(o) = o {
            if item_hit.contains(&i) && o.effect == MetadataEffect::NoOp {
                o.effect = match &items[i] {
                    MetadataChange::UnlinkValue { .. } => MetadataEffect::Deleted,
                    MetadataChange::ValueMetadata {
                        action: Action::Clear,
                        ..
                    } => MetadataEffect::Cleared,
                    MetadataChange::ValueMetadata {
                        action: Action::Keep,
                        ..
                    } => MetadataEffect::NoOp,
                    _ => MetadataEffect::Created,
                };
            }
        }
    }
    if let Some((i, _)) = save {
        // The save default's outcome is a statement, never a write by itself.
        outs[i].get_or_insert(outcome(
            i,
            MetadataKind::ValueMetadata,
            0,
            MetadataEffect::NoOp,
        ));
    }

    // ---- M3: object links.
    for (i, it) in items.iter().enumerate() {
        match it {
            MetadataChange::Link { subject, link } => {
                let e = resolve_edge(&mut cx, &link.target, &link.role)?;
                check_touch(i, &e)?;
                let o = match resolve_subject(&mut cx, subject)? {
                    Resolved::Covered => {
                        outcome(i, MetadataKind::Link, 0, MetadataEffect::DroppedByDelete)
                    }
                    Resolved::Live((ot, oid)) => {
                        if ot == e.0 && oid == e.1 {
                            return Err(DbError::Api(format!(
                                "metadata item {i}: an object cannot link to itself"
                            )));
                        }
                        if let Some(j) = match ot {
                            "source" => touched.get(&(MetadataKind::Source, oid)),
                            "license" => touched.get(&(MetadataKind::License, oid)),
                            _ => None,
                        } {
                            if *j != i {
                                return Err(DbError::Api(format!(
                                    "metadata item {i} names an object that item {j} revises, \
                                     renames or deletes; one item per object per call"
                                )));
                            }
                        }
                        if object_link_live(&cx, ot, oid, e)? {
                            outcome(i, MetadataKind::Link, oid, MetadataEffect::NoOp)
                        } else {
                            insert_object_link(&cx, ot, oid, e)?;
                            outcome(i, MetadataKind::Link, oid, MetadataEffect::Created)
                        }
                    }
                };
                outs[i] = Some(o);
            }
            MetadataChange::Unlink {
                subject,
                target,
                role,
            } => {
                let e = resolve_edge(&mut cx, target, role)?;
                let o = match resolve_subject(&mut cx, subject)? {
                    Resolved::Covered => outcome(i, MetadataKind::Link, 0, MetadataEffect::NoOp),
                    Resolved::Live((ot, oid)) => {
                        let n = conn.execute(
                            "UPDATE object_metadata_link SET valid_to_commit = ? \
                             WHERE valid_to_commit IS NULL AND valid_from_commit < ? \
                               AND object_type = ? AND object_id = ? AND target_kind = ? \
                               AND target_id = ? AND coalesce(role_id, 0) = coalesce(?, 0)",
                            params![c, c, ot, oid, e.0, e.1, e.2],
                        )?;
                        if n == 0 {
                            report.warnings.push(Warning {
                                kind: WarningKind::NothingToDelete { key_index: i },
                                object_type: "object_metadata_link".into(),
                                name: String::new(),
                                id: oid,
                            });
                            outcome(i, MetadataKind::Link, oid, MetadataEffect::NoOp)
                        } else {
                            let mut o =
                                outcome(i, MetadataKind::Link, oid, MetadataEffect::Deleted);
                            o.links_closed = n;
                            o
                        }
                    }
                };
                outs[i] = Some(o);
            }
            _ => {}
        }
    }

    // ---- M4: pool deletes, on the post-call state.
    for d in &deletes {
        outs[d.index] = Some(run_pool_delete(&cx, d)?);
    }

    report.warnings.extend(cx.folds.warnings("metadata", ""));
    report.metadata.extend(outs.into_iter().flatten());
    report.value_links = counts;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh schema with commit 2 holding three scalar values of pd 10
    /// (entities 1..=3, alternative 1), each with one pair link, and one
    /// object link on pd 10. Returns the connection.
    fn fixture(dir: &std::path::Path) -> Connection {
        let db = dir.join("u.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO commit (user, message) VALUES ('t', 'v');
             INSERT INTO metadata_pair (name, value, valid_from_commit) VALUES ('n', 'x', 2);
             INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               SELECT e, 10, 1, 2, 1, TRUE, 'string', 'old' FROM range(1, 4) t(e);
             INSERT INTO value_metadata_link (entity_id, parameter_definition_id,
                 alternative_id, target_kind, target_id, valid_from_commit)
               SELECT e, 10, 1, 'pair', 1, 2 FROM range(1, 4) t(e);
             INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id,
                 valid_from_commit) VALUES ('parameter_definition', 10, 'pair', 1, 2);
             INSERT INTO commit (user, message) VALUES ('t', 'c3');",
        )
        .unwrap();
        conn
    }

    fn n(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn a_template_rewrite_keeps_and_a_drop_closes_the_links() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        // Rewrite entity 1 ('old' -> 'new') at commit 3: its link is kept.
        conn.execute_batch(
            "UPDATE value_scalar SET valid_to_commit = 3 WHERE entity_id = 1;
             INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               VALUES (1, 10, 1, 3, 2, TRUE, 'string', 'new');",
        )
        .unwrap();
        let kept = keep_value_links_where(
            &conn,
            3,
            "SELECT entity_id, parameter_definition_id, alternative_id FROM value_scalar \
             WHERE valid_from_commit = 3 AND valid_to_commit IS NULL",
        )
        .unwrap();
        assert_eq!(kept, 1);
        // A second keep at the same commit reopens nothing twice.
        assert_eq!(
            keep_value_links_where(
                &conn,
                3,
                "SELECT entity_id, parameter_definition_id, alternative_id FROM value_scalar \
                 WHERE valid_from_commit = 3 AND valid_to_commit IS NULL",
            )
            .unwrap(),
            0
        );
        assert_eq!(
            n(
                &conn,
                "SELECT version FROM value_metadata_link WHERE entity_id = 1 \
                      AND valid_to_commit IS NULL"
            ),
            2
        );
        // Drop entity 2's value: its link closes, entity 3's stays.
        conn.execute_batch("UPDATE value_scalar SET valid_to_commit = 3 WHERE entity_id = 2")
            .unwrap();
        assert_eq!(close_orphaned_value_links(&conn, 3, 10).unwrap(), 1);
        assert_eq!(
            n(
                &conn,
                "SELECT count(*) FROM value_metadata_link WHERE valid_to_commit IS NULL"
            ),
            2
        );
    }

    #[test]
    fn a_merge_carries_value_and_parameter_links() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        // pd 20 is the target; entity 1's value is carried into it at 3.
        conn.execute_batch(
            "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               VALUES (1, 20, 1, 3, 1, TRUE, 'string', 'old');",
        )
        .unwrap();
        let c = carry_merged_links(&conn, 3, 10, 20, &[(1, 1)]).unwrap();
        assert_eq!(
            c,
            Carried {
                value_links: 1,
                object_links: 1
            }
        );
        // Carrying again adds no second parameter link.
        assert_eq!(
            carry_merged_links(&conn, 3, 10, 20, &[(1, 1)])
                .unwrap()
                .object_links,
            0
        );
        // A target value written at 3 by another step (not carried by the
        // merge) gets no copied links.
        conn.execute_batch(
            "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               VALUES (2, 20, 1, 3, 1, TRUE, 'string', 'other');",
        )
        .unwrap();
        carry_merged_links(&conn, 3, 10, 20, &[(1, 1)]).unwrap();
        assert_eq!(
            n(
                &conn,
                "SELECT count(*) FROM value_metadata_link WHERE parameter_definition_id = 20 \
                      AND entity_id = 2"
            ),
            0
        );
        assert_eq!(
            n(
                &conn,
                "SELECT count(*) FROM value_metadata_link WHERE parameter_definition_id = 20"
            ),
            1
        );
    }

    #[test]
    fn a_value_delete_closes_links_and_skips_a_link_free_file() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        assert_eq!(
            close_value_links(&conn, 3, &[(1, 10, 1), (9, 9, 9)]).unwrap(),
            1
        );
        conn.execute_batch("DELETE FROM value_metadata_link")
            .unwrap();
        assert!(!has_value_links(&conn).unwrap());
        assert_eq!(close_value_links(&conn, 3, &[(2, 10, 1)]).unwrap(), 0);
    }
}
