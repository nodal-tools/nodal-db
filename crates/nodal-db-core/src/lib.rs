//! nodal-db-core — the writer library and the single canonicalizer of
//! nodal-db database files.
//!
//! A nodal-db file is a DuckDB database with a fixed schema: classes,
//! entities, parameters, alternatives and scenarios, parameter values in
//! typed tables (scalar, dense array, long), and append-only history with
//! as-of reads. This crate is the one writer: every value goes through
//! the same canonicalization pipeline ([`canon`], [`tier`], [`encoding`])
//! so the same data always produces the same stored bytes, whoever
//! writes it.
//!
//! The entry points:
//! - [`schema_init`] creates a database file from the embedded schema.
//! - [`api::commit`] commits one [`api::ChangeSet`] (value writes,
//!   structural changes, value and object deletes, purges, metadata)
//!   atomically; [`api::write_parameter`], [`api::delete_values`] and the
//!   [`verbs`] are convenience wrappers over it. Reads go through the SQL
//!   views (the read surface, [`read_surface`]) or
//!   [`api::read_parameter`].
//! - [`template`] loads, plans, applies and exports templates.
//! - [`compaction`], [`extract`], [`erasure`] and [`purge`] maintain a file.
//!
//! Connections are per operation: every call opens the file, works and
//! closes it, so a long-lived reader never blocks a writer ([`conn`]).
//!
//! User documentation: <https://nodal-tools.github.io/nodal-db/>.

/// Re-export of the one `duckdb` (and thus the one `duckdb::arrow`) this
/// library is built against, so downstream crates (the CLI, tests) can name
/// the exact same types the public API uses: one arrow version in the whole
/// dependency tree.
pub use duckdb;

/// Minimum bundled DuckDB version the schema semantics are verified on.
/// Mirrors `schema/build.py`'s `MINIMUM_DUCKDB`; asserted when a connection
/// opens.
///
/// 2.0 because the project writes DuckDB's native 2.0 storage format
/// (`storage_version` tag `v2.0.0+`; readers need DuckDB >= 2.0) and the
/// schema is verified only on 2.0. Native files are what make an array
/// replace commit cheap: the 1.5 format logs the whole list column to the
/// WAL on every write. The tests in `tests/storage_format.rs` guard that.
pub const MINIMUM_DUCKDB: (u32, u32) = (2, 0);

/// The database format version this build writes and accepts (see
/// [`compaction::SCHEMA_VERSION`], where it is defined).
pub use compaction::SCHEMA_VERSION;

// Each module's documentation is its own `//!` header.

pub mod types;

pub mod text;

/// The name fold's data (`types::fold`): Unicode 15.1 simple lowercase, the
/// table of DuckDB's `lower()`. Generated; public for the table and guard
/// tests.
#[doc(hidden)]
pub mod fold_table;

pub mod encoding;

pub mod canon;

pub mod tier;

pub mod reconstruct;

pub mod error;

pub mod schema_init;

pub mod conn;

pub mod read_surface;

pub mod axis;

pub mod history;

pub mod api;

pub mod object_delete;

#[doc(hidden)]
pub mod entity_tuple;

#[doc(hidden)]
pub mod entity_rename_set;

pub mod purge;

pub mod compaction;

/// The shared whole-file rebuild machinery (catalog maps, real-column
/// discovery, read-only source ATTACH, sequence restore, durability
/// helpers) used by [`compaction`] and the history-free extract.
pub(crate) mod rebuild;

pub mod extract;

pub mod erasure;

pub mod verbs;

pub mod property;

pub mod metadata;

pub mod structural;

pub mod template;
