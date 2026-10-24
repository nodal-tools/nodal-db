-- 065_semantic.sql — polymorphic ontology crosswalk (fitness round 2b, T1.4).
-- Maps any schema object to external ontology terms (OEO, CIM, QUDT, SKOS,
-- SDMX codelists) by IRI, WITHOUT importing any foreign ontology structure —
-- the store stays relational; the mapping is a side table. This is the
-- express-never-assert stance applied to semantics: a stale external IRI is
-- surfaced by audit, never enforced. The OWL/RDF/JSON-LD *export* projection
-- built on this table is a separate deferred spike (2b §5.4) — NOT built here.
-- Versioned like every mutable table (from/to-commit + cur_* shadow).

CREATE TABLE semantic_annotation (
  id            BIGINT NOT NULL DEFAULT nextval('seq_semantic_annotation'),
  -- what kind of object is annotated. Extensible tag, NO CHECK enum
  -- (Decision 4): entity_class | parameter_definition | entity | unit |
  -- parameter_value_list | list_value | class_dimension | ... additive.
  object_type   VARCHAR NOT NULL,
  object_id     BIGINT NOT NULL,   -- stable id of the annotated object; FK-intent via audit (D1)
  object_subkey VARCHAR,           -- optional discriminator when object_id is not enough
                                   -- (e.g. a relationship role = entity_class_dimension position)
  -- SKOS/OWL predicate stating mapping strength. Extensible tag, no CHECK
  -- (Decision 4): exactMatch | closeMatch | broadMatch | owl:sameAs |
  -- owl:equivalentClass | rdf:type | qudt:hasQuantityKind | ...
  relation      VARCHAR NOT NULL DEFAULT 'exactMatch',
  term_iri      VARCHAR NOT NULL,  -- external term as full IRI or CURIE — the authority
  vocabulary    VARCHAR,           -- optional scheme hint (OEO|CIM16|QUDT|...); IRI is authority, not this
  note          VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_semantic_annotation ON semantic_annotation (cur_id);
