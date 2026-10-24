"""The generic Spine-DB -> nodal-db importer.

Mechanics
---------
1. Read the whole source through spinedb_api (``upgrade=False`` PINNED — the
   importer never upgrades its input; the oracle leg owns upgrading).
2. Determine the schema version (profile hook; FAIL if indeterminable — the
   identity stamp is non-optional) and normalize it to a canonical integer
   string ("40.0" -> "40").
3. Synthesize a BOOTSTRAP TEMPLATE carrying the source vocabulary as found —
   value lists, classes (dims, ``active_by_default``), parameter definitions
   with OBSERVED-SHAPE declared type sets (the union of shapes actually
   observed in stored values; value-less definitions typeless), value-list
   bindings, descriptions, and literal scalar defaults — and ``template_apply``
   it onto a fresh nodal DB. Loading vocabulary through the one template
   loader keeps invariant 2 intact (no second vocabulary-write path) and
   stamps ``template_name``/``template_version`` in the same stroke.
4. Write sections through the skin, one commit (group) per section:
   alternatives -> scenarios -> entities -> activation (ALL classes' rows,
   as found — no class allowlist) -> values (bind-by-
   construction: every batch instantiates the member its observed shape
   declared) -> the final, non-optional identity stamp (an idempotent
   verification no-op, since the bootstrap template already stamped).
5. Emit a LOUD residue report: unconsumed constructs, unflattenable values,
   ambiguous member picks, and the `parameter_type_list` drift table
   ("declared X, observed Y — chain will repair"). Zero silent drops.

Allowlist posture: allowlist OFF for migration-range imports — vocabulary imports as
found. The closed-allowlist mode exists for curated non-migration imports
and is out of scope here.
"""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

#: Names are free-text (was #2373 charset [A-Za-z0-9_]+; relaxed for
#: Spine interop). The importer enforces only a hygiene floor,
#: matching the Rust writer's canonical predicate (types.rs::charset_ok):
#: a name must be non-empty, equal to its trimmed form (no leading/trailing
#: whitespace), and free of ASCII control characters (bytes < 0x20 or 0x7f).
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _name_ok(name: str) -> bool:
    return bool(name) and name == name.strip() and _CONTROL_RE.search(name) is None

#: The spinedb_api open contract, pinned by test: never create, never upgrade.
OPEN_KWARGS = {"create": False, "upgrade": False}


class ImportError_(Exception):
    """A loud importer failure (bad version, illegal name, hostile value)."""


def normalize_version(raw) -> str:
    """Normalize a source version to the canonical integer string
    ("40.0" -> "40")."""
    if raw is None:
        raise ImportError_(
            "cannot determine the source database's schema version — the "
            "identity stamp is non-optional; refusing to import unstamped"
        )
    s = str(raw).strip()
    try:
        f = float(s)
    except ValueError:
        raise ImportError_(f"source schema version {raw!r} is not numeric")
    if f != int(f):
        raise ImportError_(f"source schema version {raw!r} is not an integer")
    return str(int(f))


# ---------------------------------------------------------------------------
# Value flattening (flat tables, label kinds, Spine Array -> integer-position
# axis)
# ---------------------------------------------------------------------------


@dataclass
class _LeafGroup:
    kinds: tuple  # index kinds, () for scalar
    index_names: tuple  # raw Spine index names (may be empty strings)
    leaf_kind: str  # float | string | bool | date_time
    rows: list  # [(idx..., leaf)]


def _leaf(value):
    """Classify one leaf value -> (kind, converted) or None (unrepresentable).

    Numeric leaves normalize to float (Spine's own scalar number type is
    float)."""
    import spinedb_api

    if value is None:
        return None
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("float", float(value))
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, spinedb_api.DateTime):
        return ("date_time", value.value)
    return None


def _index_of(idx):
    """One Spine map index -> (kind, converted)."""
    import spinedb_api

    if isinstance(idx, spinedb_api.DateTime):
        return ("datetime", idx.value)
    if isinstance(idx, bool):
        return ("label", str(idx))
    if isinstance(idx, (int, float)):
        return ("label", str(idx))
    return ("label", str(idx))


def flatten_value(parsed, residue: list, where: str) -> list[_LeafGroup]:
    """Flatten one parsed Spine value into leaf groups (one group per
    distinct (index kinds, leaf kind) signature — ragged maps are legal in
    Spine and land as several groups, i.e. several observed members)."""
    import spinedb_api

    groups: dict[tuple, _LeafGroup] = {}

    def add(kinds, names, idx_tuple, leaf_value):
        lk = _leaf(leaf_value)
        if lk is None:
            residue.append(f"{where}: unrepresentable leaf {type(leaf_value).__name__} dropped")
            return
        leaf_kind, conv = lk
        key = (tuple(kinds), leaf_kind)
        g = groups.get(key)
        if g is None:
            g = groups[key] = _LeafGroup(tuple(kinds), tuple(names), leaf_kind, [])
        g.rows.append((*idx_tuple, conv))

    def walk(v, kinds, names, idx_tuple):
        if isinstance(v, spinedb_api.Map):
            iname = getattr(v, "index_name", "") or ""
            for i, leaf in zip(v.indexes, v.values):
                ik, iconv = _index_of(i)
                walk(leaf, kinds + [ik], names + [iname], idx_tuple + (iconv,))
        elif isinstance(v, spinedb_api.Array):
            iname = getattr(v, "index_name", "") or "i"
            for pos, leaf in enumerate(v.values):
                add(kinds + ["integer"], names + [iname], idx_tuple + (pos,), leaf)
        elif isinstance(v, spinedb_api.TimeSeriesFixedResolution) or isinstance(
            v, spinedb_api.TimeSeriesVariableResolution
        ):
            for ts, leaf in zip(v.indexes, v.values):
                # numpy datetime64[s] -> python datetime (pyarrow refuses to
                # cast a datetime64[s] scalar straight into timestamp[us]).
                ts = ts.astype("datetime64[us]").item() if hasattr(ts, "astype") else ts
                add(kinds + ["datetime"], names + ["time"], idx_tuple + (ts,), float(leaf))
        elif isinstance(v, spinedb_api.TimePattern):
            residue.append(f"{where}: time_pattern value not flattenable (long-input door closed)")
        else:
            add(kinds, names, idx_tuple, v)

    walk(parsed, [], [], ())
    return list(groups.values())


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class ImportReport:
    source: str = ""
    target: str = ""
    template_name: str = ""
    template_version: str = ""
    counts: dict = field(default_factory=dict)
    #: "declared {..}, observed {..} — chain will repair" rows.
    drift_table: list = field(default_factory=list)
    #: loud residue: unconsumed constructs / unflattenable values / ambiguous
    #: member picks — counted and named, zero silent drops.
    residue: list = field(default_factory=list)
    #: warnings surfaced by the write path (auto-creates here mean an
    #: importer bug — everything is pre-declared).
    write_warnings: list = field(default_factory=list)
    #: ``(channel, commit_id)`` per write call. ``commit_id`` is 0 when the
    #: call changed nothing (every item already in place, every value equal
    #: to the stored one): the core then makes no commit. Nothing reads these
    #: ids back; 0 is not an error.
    commits: list = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"import {self.source} -> {self.target} "
            f"[{self.template_name} @ {self.template_version}]",
            "counts: " + ", ".join(f"{k}={v}" for k, v in sorted(self.counts.items())),
            f"drift rows: {len(self.drift_table)}",
            f"residue items: {len(self.residue)}",
        ]
        for r in self.residue[:50]:
            lines.append(f"  residue: {r}")
        for d in self.drift_table[:50]:
            lines.append(f"  drift: {d}")
        return "\n".join(lines)


def _check_name(kind: str, name: str):
    if not _name_ok(name or ""):
        raise ImportError_(
            f"{kind} name {name!r} must be non-empty, trimmed, and free of "
            "control characters — refusing to import silently mangled identity"
        )


_ARROW_IDX = {
    "label": pa.string(),
    "integer": pa.int64(),
    "datetime": pa.timestamp("us"),
}
_ARROW_LEAF = {
    "float": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
    "date_time": pa.timestamp("us"),
}


# ---------------------------------------------------------------------------
# The importer
# ---------------------------------------------------------------------------


class SpineImporter:
    def __init__(self, spine_path, nodal_path, profile, *, user="spine-import",
                 version: str | None = None):
        self.spine_path = str(spine_path)
        self.nodal_path = str(nodal_path)
        self.profile = profile
        self.user = user
        self.version_override = version
        self.report = ImportReport(source=self.spine_path, target=self.nodal_path)

    # -- source reading ----------------------------------------------------

    def _url(self):
        p = self.spine_path
        if p.startswith("sqlite://"):
            return p
        return "sqlite:///" + p

    def run(self) -> ImportReport:
        from spinedb_api import DatabaseMapping, from_database

        import nodal_db

        rep = self.report
        prof = self.profile

        with DatabaseMapping(self._url(), **OPEN_KWARGS) as src:
            # ---- version (before anything is written) --------------------
            raw_version = (
                self.version_override
                if self.version_override is not None
                else prof.version_of(src)
            )
            version = normalize_version(raw_version)
            name = prof.template_name
            if not name:
                raise ImportError_("profile declares no template name")
            rep.template_name, rep.template_version = name, version

            # ---- read everything (per-operation read of the source) ------
            def ext(items):
                return [i.extended() for i in items]

            classes = ext(src.find_entity_classes())
            defs = ext(src.find_parameter_definitions())
            value_lists = ext(src.find_parameter_value_lists())
            list_values = ext(src.find_list_values())
            alternatives = ext(src.find_alternatives())
            scenarios = ext(src.find_scenarios())
            scen_alts = ext(src.find_scenario_alternatives())
            entities = ext(src.find_entities())
            ent_alts = ext(src.find_entity_alternatives())
            values = ext(src.find_parameter_values())
            declared_types = ext(src.find_parameter_types())

            # nodal-db name resolution is case-insensitive; Spine's is not.
            # Case-colliding names cannot import faithfully — refuse loudly.
            def check_fold(kind, names):
                seen: dict[str, str] = {}
                for n in names:
                    if n.lower() in seen and seen[n.lower()] != n:
                        raise ImportError_(
                            f"case-colliding {kind} names {seen[n.lower()]!r} / "
                            f"{n!r}: nodal-db resolution is case-insensitive; "
                            "merge or rename in the source first"
                        )
                    seen[n.lower()] = n

            check_fold("alternative", [a["name"] for a in alternatives])
            check_fold("scenario", [s["name"] for s in scenarios])
            check_fold("entity_class", [c["name"] for c in classes])
            check_fold("value_list", [v["name"] for v in value_lists])
            by_cls: dict[str, list] = {}
            for d in defs:
                by_cls.setdefault(d["entity_class_name"], []).append(d["name"])
            for cls, names in by_cls.items():
                check_fold(f"parameter ({cls})", names)
            by_cls = {}
            for e in entities:
                by_cls.setdefault(e["entity_class_name"], []).append(e["name"])
            for cls, names in by_cls.items():
                check_fold(f"entity ({cls})", names)

            # unconsumed constructs — counted and named (loud residue)
            for probe, label in (
                ("find_metadata_items", "metadata item"),
                ("find_entity_groups", "entity_group row"),
                ("find_superclass_subclasses", "superclass_subclass row"),
            ):
                try:
                    n = len(list(getattr(src, probe)()))
                except Exception:
                    n = 0
                if n:
                    rep.residue.append(f"{n} {label}(s) present, not imported")

            # ---- flatten all values; observe shapes ----------------------
            # (class, param) -> observed member signatures
            observed: dict[tuple, dict] = {}
            # (class, param, alt, kinds, names, leaf) -> rows [(entity, *idx, v)]
            batches: dict[tuple, list] = {}
            null_values = 0
            for pv in values:
                cls = pv["entity_class_name"]
                par = pv["parameter_definition_name"]
                where = f"{cls}.{par}[{pv['entity_byname']}@{pv['alternative_name']}]"
                try:
                    parsed = from_database(pv["value"], pv["type"])
                except Exception as e:
                    rep.residue.append(f"{where}: unparseable value ({e})")
                    continue
                if parsed is None:
                    null_values += 1
                    continue
                groups = flatten_value(parsed, rep.residue, where)
                ent_name = pv.get("entity_name") or "__".join(pv["entity_byname"])
                for g in groups:
                    if not g.rows:
                        continue
                    names, note = prof.dim_names(cls, par, g.kinds, g.index_names, tuple(
                        r[0] for r in g.rows) if len(g.kinds) == 1 and g.kinds[0] == "label" else ())
                    if note:
                        rep.residue.append(f"ambiguous member pick: {note}")
                    sig = (tuple(g.kinds), tuple(names))
                    observed.setdefault((cls, par), {}).setdefault(sig, set()).add(g.leaf_kind)
                    bkey = (cls, par, pv["alternative_name"], sig, g.leaf_kind)
                    batches.setdefault(bkey, []).extend(
                        (ent_name, *row) for row in g.rows
                    )
            if null_values:
                rep.residue.append(f"{null_values} null parameter value(s) dropped")

            # ---- drift table (declared vs observed) ----------------------
            declared_by_def: dict[tuple, set] = {}
            for t in declared_types:
                key = (t["entity_class_name"], t["parameter_definition_name"])
                declared_by_def.setdefault(key, set()).add(
                    (t.get("type") or t.get("value_type"), t.get("rank", 0))
                )
            for key, obs in sorted(observed.items()):
                decl = declared_by_def.get(key)
                obs_desc = sorted(
                    ("scalar:" + "/".join(sorted(leafs)))
                    if not sig[0]
                    else f"indexed[{','.join(sig[0])}]:{'/'.join(sorted(leafs))}"
                    for sig, leafs in obs.items()
                )
                if decl is not None:
                    decl_desc = sorted(f"{t}({r})" for t, r in decl)
                    mismatch = self._drifted(decl, obs)
                    if mismatch:
                        rep.drift_table.append(
                            f"{key[0]}.{key[1]}: declared {decl_desc}, observed "
                            f"{obs_desc} — chain will repair"
                        )
            rep.counts["parameter_type_list rows (drift metadata only)"] = len(declared_types)

            # ---- bootstrap template (vocabulary as found) ----------------
            lists_by_id_members: dict[str, list] = {}
            for lv in sorted(list_values, key=lambda x: x.get("index", x.get("id", 0)) or 0):
                lname = lv["parameter_value_list_name"]
                try:
                    member = from_database(lv["value"], lv["type"])
                except Exception as e:
                    rep.residue.append(f"value_list {lname}: unparseable member ({e})")
                    continue
                lists_by_id_members.setdefault(lname, []).append(member)

            tpl = self._bootstrap_template(
                name, version, classes, defs, value_lists, lists_by_id_members,
                observed, rep,
            )

            db = nodal_db.init(self.nodal_path)
            with tempfile.NamedTemporaryFile(
                "w", suffix=".yaml", prefix="spine_bootstrap_", delete=False
            ) as f:
                json.dump(tpl, f)  # JSON is valid YAML 1.2
                tpl_path = f.name
            outcome = db.template_apply(tpl_path, user=self.user,
                                        message="spine import: vocabulary (as found)")
            rep.commits.append(("vocabulary", outcome.get("commit_id")))
            rep.counts["classes"] = len(classes)
            rep.counts["parameter_definitions"] = len(defs)
            rep.counts["value_lists"] = len(value_lists)

            # ---- structural writes: ONE atomic batched commit -----------
            # The four kinds (alternatives, entities, scenarios, activation)
            # were formerly written one-per-own-commit-verb (~1800 connection
            # opens; the dominant bottleneck). They are the same shared
            # appliers the structural channel drives, so accumulating them as
            # StructuralChange dicts and sending them through ONE
            # commit_changes(structural=[…]) produces a logically identical DB
            # (same rows, same resolved state) — only the commit count differs.
            #
            # Ordering inside the vec (items apply sequentially in one txn; a
            # later item sees an earlier item's uncommitted inserts):
            #   1. create_alternative      (referenced by scenarios/activation)
            #   2. create_entity           (in the current sorted-by-ndims
            #                               order, so an N-D entity's element
            #                               entities precede it in the same txn)
            #   3. set_scenario_alternatives (references alternatives)
            #   4. set_entity_activation     (references entities + alternatives)

            # -- alternatives --
            alt_changes = []
            for a in alternatives:
                _check_name("alternative", a["name"])
                alt_changes.append({
                    "op": "create_alternative",
                    "name": a["name"],
                    "description": a.get("description"),
                })
            rep.counts["alternatives"] = len(alternatives)

            # -- entities (0-D first, then by dimension count) --
            ndims = {c["name"]: len(c.get("dimension_name_list") or ()) for c in classes}
            entity_changes = []
            for e in sorted(entities, key=lambda x: ndims.get(x["entity_class_name"], 0)):
                _check_name("entity", e["name"])
                entity_changes.append({
                    "op": "create_entity",
                    "class": e["entity_class_name"],
                    "name": e["name"],
                    "elements": list(e.get("element_name_list") or ()),
                })
                if e.get("description"):
                    rep.residue.append(
                        f"entity {e['entity_class_name']}/{e['name']}: description not carried"
                    )
            rep.counts["entities"] = len(entities)

            # -- scenarios --
            order_by_scen: dict[str, list] = {}
            for sa in sorted(scen_alts, key=lambda x: x["rank"]):
                order_by_scen.setdefault(sa["scenario_name"], []).append(
                    sa["alternative_name"]
                )
            scenario_changes = []
            for s in scenarios:
                _check_name("scenario", s["name"])
                scenario_changes.append({
                    "op": "set_scenario_alternatives",
                    "scenario": s["name"],
                    "alternatives": order_by_scen.get(s["name"], []),
                })
                if s.get("description"):
                    rep.residue.append(
                        f"scenario {s['name']}: description not carried (no store slot)"
                    )
            rep.counts["scenarios"] = len(scenarios)

            # -- activation: ALL classes' rows, as found --
            activation_changes = []
            for ea in ent_alts:
                ent_name = ea.get("entity_name") or "__".join(ea["entity_byname"])
                activation_changes.append({
                    "op": "set_entity_activation",
                    "class": ea["entity_class_name"],
                    "entity": ent_name,
                    "alternative": ea["alternative_name"],
                    "active": bool(ea["active"]),
                })
            rep.counts["activation_rows"] = len(ent_alts)

            structural = (
                alt_changes + entity_changes + scenario_changes + activation_changes
            )
            repd = db.commit_changes(
                structural=structural, user=self.user,
                message="spine import: structural (alternatives, entities, "
                        "scenarios, activation)",
            )
            rep.commits.append(("structural", repd.get("commit_id")))
            for w in repd.get("warnings", []):
                rep.write_warnings.append(w)

            # ---- values --------------------------------------------------
            # A RAGGED value (one Spine value, several leaf-group shapes on
            # ONE entity) cannot land whole: a later write of the same
            # (class, param, alt, entity) replaces the earlier one. Keep the
            # largest group per entity, residue-report the rest — loudly.
            seen_entity_target: dict[tuple, tuple] = {}
            ordered = sorted(
                batches.items(), key=lambda kv: (-len(kv[1]), kv[0])
            )
            segments = []
            for (cls, par, alt, sig, leaf_kind), rows in ordered:
                kinds, names = sig
                kept = []
                for row in rows:
                    tkey = (cls, par, alt, row[0])
                    if tkey in seen_entity_target:
                        rep.residue.append(
                            f"{cls}.{par}[{row[0]}@{alt}]: ragged multi-shape "
                            f"value — group {kinds}/{leaf_kind} dropped "
                            "(largest group imported)"
                        )
                        continue
                    kept.append(row)
                if not kept:
                    continue
                for row in kept:
                    seen_entity_target[(cls, par, alt, row[0])] = sig
                cols = {"entity": pa.array([r[0] for r in kept], pa.string())}
                domains: dict[int, list] = {}
                for i, (dname, dkind) in enumerate(zip(names, kinds)):
                    cols[dname] = pa.array([r[1 + i] for r in kept], _ARROW_IDX[dkind])
                    if dkind == "label":
                        # ordered domain: first-appearance order (the source
                        # Map's transport order; ordinals are order-only
                        # presentation, excluded from the fingerprint)
                        seen = dict.fromkeys(r[1 + i] for r in kept)
                        domains[i + 1] = list(seen)
                cols["value"] = pa.array(
                    [r[-1] for r in kept], _ARROW_LEAF[leaf_kind]
                )
                seg = {
                    "class": cls, "parameter": par, "alternative": alt,
                    "batch": pa.record_batch(cols),
                }
                if domains:
                    seg["domains"] = domains
                segments.append(seg)
            # commit_changes refuses duplicate targets within one call —
            # partition same-target segments into sequential waves (distinct
            # entities per wave by construction above, so nothing clobbers).
            waves: list[list] = []
            wave_targets: list[set] = []
            for seg in segments:
                target = (seg["class"].lower(), seg["parameter"].lower(),
                          seg["alternative"].lower())
                placed = False
                for wave, targets in zip(waves, wave_targets):
                    if target not in targets and len(wave) < 300:
                        wave.append(seg)
                        targets.add(target)
                        placed = True
                        break
                if not placed:
                    waves.append([seg])
                    wave_targets.append({target})
            total_written = 0
            total_unchanged = 0
            for i, wave in enumerate(waves):
                repd = db.commit_changes(
                    segments=wave, user=self.user,
                    message=f"spine import: values ({i + 1}/{len(waves)})",
                )
                # values_written counts changed values only; values equal to
                # the stored ones are values_unchanged (and a wave with no
                # change makes no commit: commit_id 0).
                total_written += repd.get("values_written", 0)
                total_unchanged += repd.get("values_unchanged", 0)
                rep.commits.append(("values", repd.get("commit_id")))
                for w in repd.get("warnings", []):
                    rep.write_warnings.append(w)
            rep.counts["value_batches"] = len(segments)
            rep.counts["values_written"] = total_written
            if total_unchanged:
                rep.counts["values_unchanged"] = total_unchanged

            # ---- the final, non-optional identity stamp ------------------
            # The bootstrap template already stamped this identity; the verb
            # verifies it (an identical stamp is a commit-free no-op, a
            # DIFFERENT stamp would refuse loudly).
            db.stamp_template_identity(name, version, user=self.user)

        return rep

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _drifted(decl: set, obs: dict) -> bool:
        """Loose declared-vs-observed comparison for the drift table: a
        declared (type, rank) set covers an observation when every observed
        signature has a declared row of the same rank whose type family
        matches (str->string, float->float, *_map/array -> indexed)."""
        def norm(t, r):
            t = (t or "").lower()
            if t in ("str", "string"):
                return ("string", 0)
            if t in ("float", "int"):
                return ("float", 0)
            if t == "bool":
                return ("bool", 0)
            if t.endswith("_map") or t in ("map", "array", "time_series", "time_pattern"):
                rank = r or (int(t[0]) if t[:1].isdigit() else 1)
                return ("indexed", rank)
            return (t, r or 0)

        declared_norm = {norm(t, r) for (t, r) in decl}
        for (kinds, _names), leafs in obs.items():
            if kinds:
                if ("indexed", len(kinds)) not in declared_norm:
                    return True
            else:
                for leaf in leafs:
                    if (leaf if leaf != "date_time" else "date_time", 0) not in declared_norm and (
                        leaf, 0
                    ) not in declared_norm:
                        return True
        return False

    def _bootstrap_template(self, name, version, classes, defs, value_lists,
                            list_members, observed, rep):
        """Synthesize the vocabulary-as-found bootstrap template (dict —
        dumped as JSON, which the IR's YAML 1.2 parser reads natively)."""
        from spinedb_api import from_database

        for c in classes:
            _check_name("entity_class", c["name"])
        for d in defs:
            _check_name("parameter_definition", d["name"])

        vlists = []
        for vl in value_lists:
            _check_name("value_list", vl["name"])
            members = []
            for m in list_members.get(vl["name"], []):
                if isinstance(m, (str, float, int, bool)):
                    members.append({"value": m})
                else:
                    rep.residue.append(
                        f"value_list {vl['name']}: non-scalar member {m!r} dropped"
                    )
            if not members:
                rep.residue.append(f"value_list {vl['name']}: empty, not imported")
                continue
            vlists.append({"name": vl["name"], "members": members})
        vlist_names = {v["name"] for v in vlists}

        defs_by_class: dict[str, list] = {}
        for d in defs:
            defs_by_class.setdefault(d["entity_class_name"], []).append(d)

        out_classes = []
        ndims = {c["name"]: len(c.get("dimension_name_list") or ()) for c in classes}
        for c in sorted(classes, key=lambda x: (ndims[x["name"]], x["name"])):
            cls_out = {
                "name": c["name"],
                "active_by_default": bool(c.get("active_by_default", True)),
            }
            if c.get("description"):
                cls_out["description"] = c["description"]
            dim_list = list(c.get("dimension_name_list") or ())
            if dim_list:
                cls_out["dimensions"] = dim_list
            params = []
            for d in sorted(defs_by_class.get(c["name"], []), key=lambda x: x["name"]):
                p = {"name": d["name"]}
                if d.get("description"):
                    p["description"] = d["description"]
                obs = observed.get((c["name"], d["name"]), {})
                types = []
                for (kinds, names), leafs in sorted(obs.items()):
                    if not kinds:
                        for leaf in sorted(leafs):
                            types.append({"kind": leaf})
                    else:
                        types.append({
                            "kind": "indexed",
                            "dims": [
                                {"name": n, "kind": k} for n, k in zip(names, kinds)
                            ],
                        })
                lname = d.get("parameter_value_list_name")
                if lname and lname in vlist_names:
                    p["value_list"] = lname
                if types:
                    p["types"] = types
                # literal scalar default, as found; anything else -> drift
                try:
                    dv = from_database(d.get("default_value"), d.get("default_type"))
                except Exception:
                    dv = None
                    rep.residue.append(
                        f"{c['name']}.{d['name']}: unparseable default dropped"
                    )
                if dv is not None:
                    members_here = list_members.get(p.get("value_list"), None)
                    if members_here is not None and dv not in members_here:
                        rep.residue.append(
                            f"{c['name']}.{d['name']}: default {dv!r} is not a "
                            f"member of bound list {p['value_list']!r} — not "
                            "carried (drift)"
                        )
                        lit = None
                    else:
                        lit = self._default_literal(dv, types, p.get("value_list"))
                    if lit is not None:
                        p["default"] = lit
                    else:
                        rep.residue.append(
                            f"{c['name']}.{d['name']}: non-literal default "
                            f"{type(dv).__name__} not carried (drift; head "
                            "reconcile owns the end-state default)"
                        )
                params.append(p)
            if params:
                cls_out["parameters"] = params
            out_classes.append(cls_out)

        return {
            "template": {
                "name": name,
                "version": version,
                "description": "spine import bootstrap (vocabulary as found)",
                "role": getattr(self.profile, "role", "input"),
            },
            "value_lists": vlists,
            "classes": out_classes,
        }

    @staticmethod
    def _default_literal(dv, types, value_list):
        """A default literal, only when it is a scalar the declared set can
        carry (string default with a string/list-bound member, numeric with a
        float member, bool with bool; typeless definitions accept any scalar
        literal)."""
        scalar_kinds = {t["kind"] for t in types if t["kind"] != "indexed"}
        # A value-less definition imports TYPELESS, and the loader
        # (correctly) refuses a default on a typeless parameter — the default
        # goes to the drift report instead; the head reconcile owns the
        # end-state default. A list binding carries an inferred member, so a
        # list-bound default stays representable.
        typeless = not types
        if typeless and value_list is None:
            return None
        if isinstance(dv, bool):
            ok = typeless or "bool" in scalar_kinds
            return {"kind": "bool", "value": dv} if ok else None
        if isinstance(dv, (int, float)):
            ok = typeless or "float" in scalar_kinds
            return {"kind": "float", "value": float(dv)} if ok else None
        if isinstance(dv, str):
            ok = typeless or "string" in scalar_kinds or value_list is not None
            return {"kind": "string", "value": dv} if ok else None
        return None
