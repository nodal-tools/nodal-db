//! The per-segment label-domain index (plan T12,
//! `specs/unicode_text_rule_PLAN.md` §5): it must give every label the
//! ordinal the linear domain scan it replaced gave, so axis order and bytes
//! are unchanged.
//!
//! - A label repeated in a domain keeps its FIRST ordinal (a direct
//!   `canonicalize` caller may pass one; the write path refuses it first).
//! - Of two domains for one position, the first serves it.
//! - `index_domains` refuses a repeat in any domain, including a second
//!   domain for a covered position, and names the dimension.
//! - `canonicalize` and `canonicalize_indexed` agree on a large domain.

use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

use nodal_db_core::canon::{
    canonicalize, canonicalize_indexed, index_domains, CanonError, DomainIndex, LabelDomain,
};
use nodal_db_core::encoding::AxisBody;
use nodal_db_core::types::{DeclaredType, IndexKind, ValueKind};

fn rank1() -> DeclaredType {
    DeclaredType {
        positions: vec![(1, IndexKind::Label)],
    }
}

fn batch(labels: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("mode", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let values: Vec<f64> = (0..labels.len()).map(|i| i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn domain(position: u8, labels: &[&str]) -> LabelDomain {
    LabelDomain {
        position,
        labels: labels.iter().map(|s| s.to_string()).collect(),
    }
}

/// (ordinal, label) of the value's one label axis, in axis order.
fn ordinals(domains: &[LabelDomain], labels: &[&str]) -> Vec<(i32, String)> {
    let v = canonicalize(&batch(labels), &rank1(), ValueKind::Float, domains).unwrap();
    match &v.axes[0].body {
        AxisBody::Label { positions } => positions
            .iter()
            .map(|p| (p.ordinal, p.label.clone()))
            .collect(),
        other => panic!("not a label axis: {other:?}"),
    }
}

#[test]
fn repeated_label_keeps_its_first_ordinal() {
    let d = [domain(1, &["y", "x", "y", "z"])];
    assert_eq!(
        ordinals(&d, &["z", "y", "x"]),
        vec![(0, "y".into()), (1, "x".into()), (3, "z".into())]
    );
}

#[test]
fn first_domain_for_a_position_serves_it() {
    let d = [domain(1, &["a", "b"]), domain(1, &["b", "a", "c"])];
    assert_eq!(
        ordinals(&d, &["b", "a"]),
        vec![(0, "a".into()), (1, "b".into())]
    );
    // "c" is only in the ignored second domain.
    let err = canonicalize(&batch(&["c"]), &rank1(), ValueKind::Float, &d).unwrap_err();
    assert!(
        matches!(err, CanonError::LabelNotInDomain(ref l, 0) if l == "c"),
        "{err:?}"
    );
}

#[test]
fn missing_domain_is_still_reported() {
    let err = canonicalize(
        &batch(&["a"]),
        &rank1(),
        ValueKind::Float,
        &[domain(2, &["a"])],
    )
    .unwrap_err();
    assert!(matches!(err, CanonError::MissingDomain(1)), "{err:?}");
}

#[test]
fn index_domains_refuses_repeats_in_every_domain() {
    let name = |p: u8| Some(format!("dim{p}"));
    let err = index_domains(&[domain(1, &["a", "b", "a"])], name).unwrap_err();
    assert!(
        matches!(&err, CanonError::RepeatedDomainLabel { label, position: 1, dimension: Some(d) }
            if label == "a" && d == "dim1"),
        "{err:?}"
    );
    // A second domain for a covered position is never looked up, but its
    // repeat is refused all the same.
    let err = index_domains(&[domain(1, &["a"]), domain(1, &["b", "b"])], name).unwrap_err();
    assert!(
        matches!(&err, CanonError::RepeatedDomainLabel { label, position: 1, .. } if label == "b"),
        "{err:?}"
    );
    assert!(index_domains(&[domain(1, &["a", "b"]), domain(2, &["a"])], name).is_ok());
}

#[test]
fn indexed_and_direct_canonicalization_agree_on_a_large_domain() {
    let labels: Vec<String> = (0..8760).map(|i| format!("t{i:04}")).collect();
    let d = [LabelDomain {
        position: 1,
        labels: labels.clone(),
    }];
    let index = index_domains(&d, |_| None).unwrap();
    // Sparse (a few labels, out of order) and dense (all, reversed).
    let sparse: Vec<&str> = [8000, 3, 4500, 17]
        .iter()
        .map(|&i| labels[i].as_str())
        .collect();
    let dense: Vec<&str> = labels.iter().rev().map(|s| s.as_str()).collect();
    for rows in [sparse, dense] {
        let b = batch(&rows);
        let direct = canonicalize(&b, &rank1(), ValueKind::Float, &d).unwrap();
        let indexed = canonicalize_indexed(&b, &rank1(), ValueKind::Float, &index).unwrap();
        assert_eq!(direct, indexed);
        assert_eq!(
            direct,
            canonicalize_indexed(&b, &rank1(), ValueKind::Float, &DomainIndex::new(&d)).unwrap()
        );
        // Ordinals are the domain indices, ascending.
        let AxisBody::Label { positions } = &direct.axes[0].body else {
            panic!("not a label axis")
        };
        assert!(positions.windows(2).all(|w| w[0].ordinal < w[1].ordinal));
        for p in positions {
            assert_eq!(labels[p.ordinal as usize], p.label);
        }
    }
}
