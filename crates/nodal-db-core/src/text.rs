//! Unicode text normalization: the one NFC definition of the core (plan T5,
//! `specs/unicode_text_rule_PLAN.md` §3).
//!
//! Every component of the core that normalizes text calls [`nfc`]; none
//! normalizes with its own code or tables. This mirrors
//! `encoding::canonical_value_f64` for floats: one definition shared by
//! storage and hash, so the two cannot drift (`CLAUDE.md` invariant 2).
//! The tables are those of the exactly pinned `unicode-normalization`
//! crate, Unicode [`UNICODE_VERSION`] (spec §3.4.1.1; bumping it is a
//! rehash-class change).
//!
//! **The text rule** (plan T1-T3, owner decision 2026-10-26): identifiers
//! are NFC, data text is bytes. Names (entity, class, parameter,
//! alternative, scenario, value list, unit, class set) and index labels
//! (map keys, label domains) are normalized to NFC at the writer boundary,
//! before any comparison, lookup or store; NFC-equal spellings are one
//! identifier and read back in NFC. String values, list members,
//! descriptions and commit messages are stored and compared exactly as
//! written. Normalizing is reported, not silent
//! ([`WarningKind::TextNormalized`]).
//!
//! The conformance oracle (`conformance/oracle/`) is the one sanctioned
//! exception: it is a second, independent implementation and must never
//! call this code (`tests/python_oracle.rs` guards that it does not import
//! the Python skin).

use std::borrow::Cow;
use std::collections::HashSet;

use unicode_normalization::{is_nfc_quick, IsNormalized, UnicodeNormalization};

use crate::types::{Warning, WarningKind};

/// The Unicode version of the normalization tables (spec §3.4.1.1 pins
/// 16.0.0; `tests/text_nfc.rs` fails when the crate pin moves it).
pub const UNICODE_VERSION: (u8, u8, u8) = unicode_normalization::UNICODE_VERSION;

/// `s` in Unicode Normalization Form C. Borrowed exactly when `s` is
/// already NFC, so the common case allocates nothing: ASCII returns at
/// once, other text after the NFC quick check, and only text the quick
/// check cannot pass is normalized in full.
pub fn nfc(s: &str) -> Cow<'_, str> {
    if s.is_ascii() {
        return Cow::Borrowed(s);
    }
    if is_nfc_quick(s.chars()) == IsNormalized::Yes {
        return Cow::Borrowed(s);
    }
    let n: String = s.nfc().collect();
    if n == s {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(n)
    }
}

/// Is `s` already in NFC? The quick check first, the full check only when
/// the quick check answers "maybe".
pub fn is_nfc(s: &str) -> bool {
    if s.is_ascii() {
        return true;
    }
    match is_nfc_quick(s.chars()) {
        IsNormalized::Yes => true,
        IsNormalized::No => false,
        IsNormalized::Maybe => unicode_normalization::is_nfc(s),
    }
}

/// `s` with each non-ASCII character followed by its code point, for
/// messages about strings that look identical on screen:
/// `"e\u{301}"` renders as `"é" (U+0065 U+0301)`.
pub fn describe(s: &str) -> String {
    let cps: Vec<String> = s.chars().map(|c| format!("U+{:04X}", c as u32)).collect();
    format!("{s:?} ({})", cps.join(" "))
}

/// The identifiers (names or labels) one write normalized, for one
/// [`WarningKind::TextNormalized`] warning: the number of distinct
/// spellings that changed and the first of them, as written and as
/// stored. Identifiers already in NFC (nearly all of them) cost one
/// [`nfc`] call and are not recorded.
#[derive(Debug, Default)]
pub(crate) struct Normalized {
    changed: HashSet<String>,
    first: Option<(String, String)>,
}

impl Normalized {
    /// `s` in NFC, recording it when that changed it.
    pub(crate) fn nfc<'a>(&mut self, s: &'a str) -> Cow<'a, str> {
        let n = nfc(s);
        if let Cow::Owned(stored) = &n {
            self.record(s, stored);
        }
        n
    }

    /// Normalize `s` in place, recording it when that changed it.
    pub(crate) fn nfc_in_place(&mut self, s: &mut String) {
        if let Cow::Owned(stored) = nfc(s) {
            self.record(s, &stored);
            *s = stored;
        }
    }

    /// Record a string the caller found not to be NFC.
    pub(crate) fn note(&mut self, s: &str) {
        if !is_nfc(s) {
            let stored = nfc(s).into_owned();
            self.record(s, &stored);
        }
    }

    fn record(&mut self, written: &str, stored: &str) {
        if self.changed.insert(written.to_string()) && self.first.is_none() {
            self.first = Some((written.to_string(), stored.to_string()));
        }
    }

    /// The warning, if anything was normalized. `what` is `"label"` or
    /// `"name"`; `object_type` and `name` say where (see
    /// [`WarningKind::TextNormalized`]).
    pub(crate) fn warning(&self, what: &str, object_type: &str, name: &str) -> Option<Warning> {
        let (written, stored) = self.first.clone()?;
        Some(Warning {
            kind: WarningKind::TextNormalized {
                what: what.to_string(),
                count: self.changed.len(),
                written,
                stored,
            },
            object_type: object_type.to_string(),
            name: name.to_string(),
            id: 0,
        })
    }
}
