# Reference

This page lists the public surface in one place: the Python methods, the
exceptions, the CLI commands, the change items a save can carry and the
fields of the reports that come back. [Using nodal-db](usage.md) explains
how to use them; the Python docstrings (`help(nodal_db.Database.commit)`)
and the Rust API docs give every detail.

## Python package

`import nodal_db`. A `Database` handle stores only the path; every method
opens a connection, does its work and closes it again.

### Module functions and constants

| Name | What it does |
|---|---|
| `nodal_db.init(path, *, default_alternative=None)` | Create a new database file with the embedded schema. Refuses a path that exists. Returns a `Database`. |
| `nodal_db.open(path)` | A `Database` handle for an existing file (checks that the file exists). |
| `nodal_db.normalize_identifier(s)` | `s` in NFC, as nodal-db stores names and labels. |
| `nodal_db.fold_name(s)` | The name fold nodal-db compares names with (DuckDB's `lower()`). |
| `nodal_db.UNICODE_VERSION` | The Unicode version of the normalization tables, `"16.0.0"`. |
| `nodal_db.__version__` | The package version. |

### Writing

Every write takes `user` (required, non-empty) and most take `message`.
Writes that take `expect_commit` commit only when the head commit is
exactly that id, else they raise `Stale` with nothing written.

| Method | What it does |
|---|---|
| `commit(*, segments, structural, deletes, object_deletes, purges, metadata, user, message, retain_content, expect_commit, covered_writes, value_metadata)` | The one save: every channel in one atomic commit. All channels are optional; at least one must be non-empty. |
| `write_parameter(class_name, parameter, alternative, batch, *, user, message, domains, retain_content, expect_commit, value_metadata)` | Write one parameter's values for one alternative. |
| `commit_structural(changes, *, user, message, expect_commit)` | Structural changes only. |
| `delete_values(keys, *, user, message, expect_commit)` | Delete values by `(class, parameter, alternative, entity)` tuples. |
| `purge(requests, *, user, message, retain_content, expect_commit)` | Purges only. |
| `template_apply(template_path, *, user, message, programs, expect_commit, allow_retarget, allow_downgrade, recreate_removed)` | Apply a template (migration chain, then the additive diff). |
| `ensure_alternative(name, *, description, user)` | Create an alternative if absent; returns its id. |
| `ensure_class(name, dims=[], *, user)` | Create a class (0-D or N-D) if absent; returns its id. |
| `ensure_parameter(class_name, name, *, types, value_list, description, user)` | Create a parameter definition if absent; returns its id. |
| `ensure_entity(class_name, name=None, elements=[], *, user)` | Create an entity (0-D by name, N-D by elements) if absent; returns its id. |
| `set_scenario_alternatives(scenario, alternatives, *, user)` | Replace a scenario's alternative ranking. |
| `set_entity_activation(class_name, entity, alternative, active, *, user)` | Set (`True`/`False`) or clear (`None`) an entity's activation in an alternative. |
| `set_class_active_by_default(class_name, active_by_default, *, user)` | Set a class's default activation. |
| `set_class_refinement(class_name, refinement, *, user)` | Mark or unmark a two-dimensional class as a refinement class. |
| `drop_value_list(name, *, user)` | Drop a value list that no definition binds. |
| `stamp_template_identity(name, version, *, allow_overwrite=False, user)` | Record `template_name` and `template_version`; a different existing stamp needs `allow_overwrite=True`. |
| `erase_alternative(alternative, *, user, note, expect_present)` | Physically remove one alternative's data, with an erasure marker. |
| `extract(dest, *, user, as_of, note)` | Write a history-free copy into a new file. |
| `compact(*, if_needed, growth, max_commits)` | Rewrite the file densely packed. |
| `refresh_read_surface(*, allow_downgrade=False)` | Replace an older read surface with this build's; `allow_downgrade=True` also replaces a newer one (development only). |

The `ensure_*` and `set_*` methods make no commit when nothing changes.

`retain_content` decides what the versions a call replaces or closes
keep: `None` (the default) follows each parameter's own
`retain_content` setting, `False` keeps no content (each replaced
version stays in history only as a content-less stub), and `True` keeps
the content even where the parameter's setting says not to. New
versions are written either way, and values equal to the stored ones
are not rewritten.

### Reading and previews

| Method | What it returns |
|---|---|
| `query(sql, *, trusted=False)` | A `pyarrow.RecordBatchReader` for one read-only query. |
| `read_parameter(class_name, parameter, alternative)` | One parameter in one alternative, as a `pyarrow.RecordBatch`. |
| `read_values(class_name, parameter, entities, alternatives=None)` | A few values; `None` reads every alternative. |
| `head_commit()` | The newest commit id (0 on an empty log). |
| `db_identity()` | The database's identity UUID. |
| `database_property(key)` / `database_properties()` | One / every current database property. |
| `read_surface_status()` | The file's read-surface status (`current`, `missing`, `older`, `drift`, `newer`). |
| `preview_purge(requests)` | What purge requests would close (upper bounds). |
| `preview_object_deletes(keys)` | What object deletes would close or be blocked by. |
| `preview_erase_alternative(alternative, *, expect_present)` | Row counts and a size estimate of an erase. |
| `template_plan(template_path, *, recreate_removed=False)` | The plan a `template_apply` would run, as a dict. |
| `template_export(*, json=False)` | The database's template document as a string: YAML, or JSON with `json=True`. |
| `path` | The file path of the handle (a read-only property). |

### Exceptions

Every nodal-db error derives from `NodalDbError`. Catch `Busy` and
`Conflict` to retry; catch `ApiError` for everything the caller can fix.

```text
NodalDbError
├── Busy                    the file is locked by another process; retry shortly
├── Conflict                a write-write conflict outlasted the retry budget; retry
├── ApiError                a batch, name or argument the core cannot accept
│   ├── Stale               expect_commit did not match the head (expected, found)
│   ├── SchemaMismatch      the file has another schema version (found, expected; strings)
│   ├── CoveredWrite        a write and an object delete covering it in one call
│   ├── WriteAndDeleteSameKey  a write and a delete of the same value in one call
│   ├── UndeclaredValueType a batch matches none of the declared value types
│   ├── ValueMetadataUnstated  a changed value with metadata links needs a statement
│   ├── ReadSurfaceStale    a read needs a view the file's older read surface lacks
│   ├── TemplateStampedRename  a class/parameter rename on a template-stamped file
│   └── TemplateStampedPurge   a vocabulary purge on a template-stamped file
├── VersionTooOld           the bundled DuckDB is older than the verified line
├── SchemaError             the schema directory could not be read
├── DatabaseError           a DuckDB error, passed through
├── ShadowedBuiltin         the file defines a macro named like a DuckDB built-in
├── UnreplayableWal         compaction found a leftover WAL it cannot replay
├── CanonError              a value could not be canonicalized or encoded
├── ArrowError              an Arrow import or compute error
└── InternalError           damaged stored data or a failed internal check
```

`ReadSurfaceStaleWarning` (a `UserWarning`) is issued once per process
and file when a file's read surface is older than this build's.
`nodal_db.open()` on a missing file raises `ApiError`. Each exception's
docstring lists its attributes.

## Change items

A save (`commit`, or `api::commit` with a `ChangeSet` in Rust) carries
six channels, applied in this order: structural changes, value writes
(`segments`), value deletes, object deletes, purges, metadata. Items are
dicts in Python; the Rust types are in `nodal_db_core::types`.

### Segments, deletes and object deletes

| Channel | Item |
|---|---|
| `segments` | `{"class", "parameter", "alternative", "batch", "domains"?}`; `batch` is an Arrow batch or table, or a list of them (one per value type) |
| `deletes` | `(class, parameter, alternative, entity)` |
| `object_deletes` | `{"kind": "entity" \| "parameter" \| "alternative" \| "class" \| "scenario", "class"?, "name" \| "elements", "cascade": bool}` |

### Structural changes (`structural`, keyed on `"op"`)

| `op` | Keys (`?` = optional) |
|---|---|
| `create_alternative` | `name`, `description`? |
| `rename_alternative` | `current`, `new_name`, `description`? |
| `create_scenario` | `name`, `description`? |
| `rename_scenario` | `current`, `new_name`, `description`? |
| `set_scenario_alternatives` | `scenario`, `alternatives` (ranked list) |
| `set_entity_activation` | `class`, `entity`, `alternative`, `active` (`True`/`False`; `None` clears) |
| `create_class` | `name`, `dims` (class names), `description`? |
| `rename_class` | `current`, `new_name`, `description`?, `allow_on_template`? |
| `create_parameter` | `class`, `name`, `types`? (list of type dicts, as `ensure_parameter`), `value_list`?, `description`? |
| `rename_parameter` | `class`, `current`, `new_name`, `description`?, `allow_on_template`? |
| `create_entity` | `class`, `name`? and/or `elements`?, `description`? |
| `rename_entity` | `class`, `current`, `new_name`, `description`? |
| `set_membership` | `group` and `member` (each `(class, name)` or `{"class", "name"}`), `alternative`?, `included`? (`None` clears) |

A rename with `new_name == current` and a `description` revises only the
description; `description: ""` clears it.

### Purges (`purges`)

`{"scope", <filters>, "allow_outside_scope"?, "allow_on_template"?}`.
Scopes and their filters: `values` (`class`, `parameter`, `alternative`),
`entities` (`class`), `activations` (`class`, `alternative`),
`memberships` (`group_class`), `alternatives` (`except`), `scenarios`,
`classes` (`except`), `parameters` (`class`), `value_lists` (`except`).

### Metadata (`metadata`, keyed on `"op"`)

| `op` | Keys |
|---|---|
| `put_source` | `source`, `new_name`?, `fields`? (`title`, `issued`, …) |
| `put_license` | `license`, `new_name`?, `fields`? (`title`, `path`, `spdx`, `policy`) |
| `set_source_licenses` | `source`, `licenses` |
| `create_pair` | `name`, `value` |
| `revise_pair` | `pair` (`[name, value]`), `value` |
| `rename_pair_key` | `current`, `new_name` |
| `put_role` | `role`, `new_name`?, `description`?, `iri`? |
| `set_property` | `key`, `value` (`None` clears) |
| `delete_source` / `delete_license` / `delete_pair` | `source` / `license` / `pair`, `cascade`? |
| `delete_role` | `role` |
| `link` / `unlink` | `subject`, `target`, `role`? |
| `link_value` / `unlink_value` | `value` (`{"class", "parameter", "alternative", "entity"}`), `target`, `role`? |
| `value_metadata` | `scope` (`"save"`, `{class, parameter, alternative}` or `{…, entity}`), `action` (`"keep"`, `"clear"` or `{"set": [target, …]}`) |

Targets: `{"source": ref}`, `{"license": ref}`, `{"pair": [name, value]}`
or `{"id": n, "kind": "source" | "license" | "pair"}`; a `ref` is a name
or an id. Subjects: `{"dataset": True}`, `{"entity": [class, name]}`,
`{"class": name}`, `{"parameter": [class, name]}`, `{"alternative": name}`,
`{"scenario": name}`, `{"run": id}`, `{"source": ref}`,
`{"license": ref}`, `{"value_list": name}`, `{"unit": name}`.

Clearing differs by channel: a description is cleared with
`description: ""`, a property with `set_property` and `value: None`, and
a `put_*` revises a record to exactly the fields given (fields left out
are cleared).

The rule for omitted fields: in the rename and `set_*` items a field
that is `None` or left out means "leave as is"; it means "clear" only
where the tables above say so (`active`, `included`, a property's
`value`). A `put_*` item is a full replace: the fields it leaves out are
cleared.

## Reports

Every save returns a report (a dict in Python, `types::WriteReport` in
Rust):

| Field | Meaning |
|---|---|
| `commit_id` | the new commit, or 0 when nothing changed (no commit was made) |
| `values_written`, `values_unchanged` | values written (or deleted); values equal to the stored ones and skipped |
| `rows_written`, `rows_closed` | physical rows inserted; rows closed out |
| `axes_created`, `axes_reused` | axis rows created; existing axes reused |
| `tiers` | the tiers written (`scalar`, `array`, `long`) |
| `warnings` | see [Warnings](#warnings) |
| `structural`, `object_deletes`, `purges`, `metadata` | one outcome per item of that channel |
| `value_links` | `kept`, `set`, `cleared`, `closed_by_delete`, `carried` |
| `read_surface_warning` | why the read-surface refresh before the write failed, else `None` |

### Index fields

Each outcome and warning names the item it is about by its position in
the call's list for that channel:

| Field | Position in |
|---|---|
| `change_index` | `structural` (structural outcomes, `TemplateStampedRename` items) |
| `segment_index` | `segments` (`dropped_by_delete` warnings, `UndeclaredValueType`) |
| `key_index` | `deletes` or `object_deletes` (object-delete outcomes, delete warnings, `CoveredWrite`, `WriteAndDeleteSameKey`) |
| `request_index` | `purges` (purge outcomes, `purge_large` warnings, `TemplateStampedPurge` items) |
| `item_index` | `metadata` (metadata outcomes) |

### Outcomes and effect tags

| Outcome | Fields |
|---|---|
| structural | `change_index`, `kind`, `id`, `effect` |
| object delete | `key_index`, `kind`, `status`, then `closed_out` (`deleted`) or `blockers` (`restricted`) |
| purge | `request_index`, `closed_out`, `objects`, `spared`, `unbound` |
| metadata | `item_index`, `kind`, `id`, `effect`, `links_closed`, `blockers` |

Structural `kind`: `alternative`, `scenario`, `scenario_ranking`,
`activation`, `membership`, `entity`, `class`, `parameter`. Metadata
`kind`: `source`, `license`, `source_licenses`, `pair`, `role`,
`property`, `link`, `value_link`, `value_metadata`.

| `effect` | Meaning |
|---|---|
| `created` | a new object was created |
| `renamed` | an object got a new name (same id) |
| `updated` | an existing object changed (a description, a ranking, a flag, fields) |
| `no_op` | it was already so; nothing was written |
| `cleared` | a setting was removed (an activation, a membership, a property) |
| `deleted` | a metadata record or link was deleted |
| `restricted` | a delete without `cascade` was blocked by links (`blockers`) |
| `dropped_by_delete` | the item's subject was deleted by the same call (`covered_writes="delete_wins"`) |

Structural outcomes use `created`, `renamed`, `updated`, `no_op` and
`cleared`; metadata outcomes use all eight. An object-delete `status` is
`deleted`, `restricted` or `not_found`.

### Warnings

Every warning has `kind`, `object_type`, `name` and `id`, plus the extra
keys of its kind. `object_type` names what the warning is about: `class`,
`parameter`, `alternative`, `entity`, `scenario`, `parameter_type`,
`value_list`, or a group (`value`, `column`, `segment`,
`structural`, `purge`, `value_metadata_link`, `object_metadata_link`).

| `kind` | Extra keys | When |
|---|---|---|
| `auto_created` | — | a write named a class, parameter, alternative, entity or parameter type that did not exist, and it was created; `id` is the new row |
| `timezone_normalized` | `column`, `tz` | a time-zone-aware timestamp column was stored as its UTC instant; the zone was dropped |
| `unknown_referent` | `key_index`, `level` | a value delete named something that does not exist; `level` is `class`, `parameter`, `alternative` or `entity`; the key was skipped |
| `nothing_to_delete` | `key_index` | a delete found no live value or link |
| `dropped_by_delete` | `segment_index`, `by`, `key_index` | a write was dropped because a delete of the same call covers it; `by` is `deletes` or `object_deletes` |
| `text_normalized` | `what`, `count`, `written`, `stored` | names or labels were normalized to NFC; `what` is `name` or `label` |
| `name_folded` | `what`, `count`, `written`, `stored`, `spellings` | a name resolved case-insensitively to an object stored with another spelling |
| `purge_large` | `request_index`, `cells`, `threshold` | a purge's scope is large (one long transaction); not a refusal |

## CLI

`nodal-db --help` and `nodal-db <command> --help` list every option. The
global options `--memory-limit SIZE` and `--threads N` cap DuckDB for
every database a command opens.

| Command | What it does |
|---|---|
| `init DB [--default-alternative NAME] [--template FILE] [--user NAME]` | Create a new database; optionally create an alternative and load a template (`--user` is the template load's author) |
| `import FILE --db DB [--alternative] [--class] [--parameter] [--user] [--message] [--retain-content \| --no-retain-content] [--value-metadata keep\|clear]` | Import an Arrow-IPC bulk file in one commit; `--class` / `--parameter` assert that every segment targets them; `--no-retain-content` keeps replaced versions only as content-less stubs, `--retain-content` keeps their content even where the parameter's setting says not to (neither: the parameter's setting) |
| `purge DB --scope SCOPE [filters] [--preview] …` | Close out everything in one scope |
| `erase-alternative DB NAME [--preview] [--note] [--expect-present NAME] [--user]` | Physically remove one alternative's data |
| `extract SRC DEST [--as-of N] [--note] [--user]` | Write a history-free copy into a new file |
| `maintain compact DB [--if-needed [--growth F] [--max-commits N]]` | Rewrite the file densely packed |
| `maintain refresh-views DB [--check \| --allow-downgrade]` | Replace an older read surface; `--check` only reports; `--allow-downgrade` also replaces a newer one (development only) |
| `template load FILE --db DB [--user] [--message]` | Load a template into a new database |
| `template plan FILE --db DB [--recreate-removed]` | Print what an apply would do, and the commit id to pin it to |
| `template apply FILE --db DB [--user] [--message] [--expect-commit N] [--recreate-removed]` | Apply a template; `--expect-commit` refuses unless the database is at the planned commit |
| `template export --db DB [--out FILE] [--json]` | Write the database's template (YAML, or JSON) |

Exit codes: 0 success, 1 error, 2 usage error (bad arguments), 75 busy
(retry later); `maintain refresh-views --check` exits 0 up to date, 3
stale or 4 newer. Details are in
[Using nodal-db](usage.md#cli-exit-codes).
