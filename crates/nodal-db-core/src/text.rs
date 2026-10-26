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
//! The conformance oracle (`conformance/oracle/`) is the one sanctioned
//! exception: it is a second, independent implementation and must never
//! call this code (`tests/python_oracle.rs` guards that it does not import
//! the Python skin).

use std::borrow::Cow;

use unicode_normalization::{is_nfc_quick, IsNormalized, UnicodeNormalization};

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
