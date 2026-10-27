//! End to end, the core's pure name checks and the database agree on name
//! identity (name_fold_identity_PLAN K9): random names drawn from the cased
//! code points, including the former drift cases (final sigma, `İ`, the
//! Unicode 16 case pairs), resolve in a commit exactly as `types::fold`
//! predicts. A name whose fold matches a stored name resolves to that row
//! (and warns `NameFolded`); any other name creates a new one.

use std::collections::HashMap;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{ingest_segments, Segment};
use nodal_db_core::fold_table::CANARY;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::text::nfc;
use nodal_db_core::types::{fold, WarningKind, WriteOptions};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn segment(names: &[String]) -> Segment {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; names.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    Segment {
        class: "node".into(),
        parameter: "p".into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: Vec::new(),
    }
}

/// Current entity name -> id.
fn entities(db: &std::path::Path) -> HashMap<String, i64> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare("SELECT name, id FROM entity WHERE valid_to_commit IS NULL")
        .unwrap();
    let out = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(stmt);
    conn.close().unwrap();
    out
}

#[test]
fn random_cased_names_resolve_as_the_fold_predicts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agree.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();

    // A small alphabet makes fold collisions frequent; the cased pool makes
    // every case pair reachable.
    let mut pool: Vec<char> = CANARY.iter().copied().step_by(7).collect();
    pool.extend([
        'Σ', 'σ', 'ς', '\u{130}', 'I', 'i', '\u{1c89}', '\u{1c8a}', 'A', 'a',
    ]);
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let name = |rng: &mut Rng| -> String {
        let len = 1 + rng.below(3);
        let s: String = (0..len).map(|_| pool[rng.below(pool.len())]).collect();
        // Names are stored in NFC; draw them in NFC so the stored spelling
        // is the written one.
        nfc(&format!("n{s}")).into_owned()
    };

    // First commit: names with pairwise distinct folds (the batch dedup
    // refuses two spellings of one name).
    let mut first: Vec<String> = Vec::new();
    let mut by_fold: HashMap<String, String> = HashMap::new();
    while first.len() < 300 {
        let n = name(&mut rng);
        if by_fold.contains_key(&fold(&n)) {
            continue;
        }
        by_fold.insert(fold(&n), n.clone());
        first.push(n);
    }
    ingest_segments(
        &db,
        vec![segment(&first)],
        "t",
        "first",
        WriteOptions::default(),
    )
    .expect("first write");
    let stored = entities(&db);
    assert_eq!(
        stored.len(),
        first.len(),
        "every distinct fold is its own entity"
    );

    // Second commit: fresh draws (distinct folds again), many of them
    // case variants of stored names.
    let mut second: Vec<String> = Vec::new();
    let mut seen: HashMap<String, ()> = HashMap::new();
    let mut variants = 0;
    while second.len() < 300 {
        let n = if rng.below(2) == 0 {
            // A case variant of a stored name: upper/lower each char at random.
            let base = &first[rng.below(first.len())];
            let v: String = base
                .chars()
                .map(|c| {
                    let lc = fold(&c.to_string());
                    if rng.below(2) == 0 {
                        lc
                    } else {
                        c.to_string()
                    }
                })
                .collect();
            nfc(&v).into_owned()
        } else {
            name(&mut rng)
        };
        if seen.insert(fold(&n), ()).is_some() {
            continue;
        }
        if by_fold.contains_key(&fold(&n)) && by_fold[&fold(&n)] != n {
            variants += 1;
        }
        second.push(n);
    }
    assert!(variants > 50, "only {variants} case variants drawn");
    let report = ingest_segments(
        &db,
        vec![segment(&second)],
        "t",
        "second",
        WriteOptions::default(),
    )
    .expect("second write");
    let after = entities(&db);

    for n in &second {
        let predicted = by_fold.get(&fold(n));
        match predicted {
            // The fold says "an existing name": no new row, same id.
            Some(old) => assert!(
                !after.contains_key(n) || n == old,
                "{n:?} folds to stored {old:?} but the database created a new entity"
            ),
            // The fold says "new": the database created it.
            None => assert!(
                after.contains_key(n) && !stored.contains_key(n),
                "{n:?} has a fold no stored name has, but the database did not create it"
            ),
        }
    }
    let expected_new = second
        .iter()
        .filter(|n| !by_fold.contains_key(&fold(n)))
        .count();
    assert_eq!(after.len(), stored.len() + expected_new);
    // Every resolved variant was reported.
    let folded: usize = report
        .warnings
        .iter()
        .filter_map(|w| match (&w.kind, w.object_type.as_str()) {
            (WarningKind::NameFolded { count, .. }, "entity") => Some(*count),
            _ => None,
        })
        .sum();
    assert_eq!(folded, variants);
}
