//! The core's NFC function (`text::nfc`, plan T5): its fast paths return
//! exactly what the full normalization returns, it borrows exactly when the
//! input is already NFC, and it is idempotent. The reference is the pinned
//! crate's own `nfc()` iterator, which the encoder used before the fast
//! paths existed, so encoding bytes are unchanged.

use std::borrow::Cow;

use nodal_db_core::text::{is_nfc, nfc};
use unicode_normalization::UnicodeNormalization;

/// Code points chosen to exercise normalization: combining marks (and
/// their reordering), precomposed Latin, Hangul syllables and conjoining
/// jamo, composition exclusions and singletons, the Unicode 16.0 additions
/// of plan §1.4, and CJK.
const POOL: &[u32] = &[
    0x61, 0x65, 0x41, 0x6F, 0x20, 0x5F, // ASCII
    0xE9, 0xC5, 0xF6, 0x1EA1, 0x1E0B, // precomposed
    0x300, 0x301, 0x302, 0x307, 0x308, 0x323, 0x328, 0x31B, 0x345, // combining
    0x1100, 0x1112, 0x1161, 0x1175, 0x11A8, 0x11C2, 0xAC00, 0xAC01, 0xD7A3, // Hangul
    0x958, 0x93C, 0x915, // a composition exclusion and its parts
    0x212B, 0x2126, 0x212A, 0x340, 0x341, 0x344, // singletons
    0x11382, 0x113C9, 0x105D2, 0x1ACF, 0x897, // the plan §1.4 Unicode 16/17 points
    0x4E2D, 0x6587, 0x1F600, // CJK, emoji
];

/// A small deterministic generator (xorshift64*), so the corpus is the
/// same on every run without a dependency.
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

    fn char(&mut self) -> char {
        if self.below(8) == 0 {
            // Any scalar value, now and then.
            loop {
                if let Some(c) = char::from_u32((self.next() % 0x11_0000) as u32) {
                    return c;
                }
            }
        }
        char::from_u32(POOL[self.below(POOL.len())]).unwrap()
    }
}

fn reference(s: &str) -> String {
    s.nfc().collect()
}

fn check(s: &str) {
    let want = reference(s);
    let got = nfc(s);
    assert_eq!(got, want, "nfc({s:?})");
    assert_eq!(
        matches!(got, Cow::Borrowed(_)),
        want == s,
        "nfc({s:?}) borrows exactly when the input is NFC"
    );
    assert_eq!(is_nfc(s), want == s, "is_nfc({s:?})");
    assert!(
        matches!(nfc(&want), Cow::Borrowed(_)),
        "nfc is idempotent on {want:?}"
    );
    assert!(is_nfc(&want));
}

#[test]
fn fast_paths_match_full_normalization_on_a_random_corpus() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut changed = 0;
    for _ in 0..50_000 {
        let len = 1 + rng.below(8);
        let s: String = (0..len).map(|_| rng.char()).collect();
        if reference(&s) != s {
            changed += 1;
        }
        check(&s);
    }
    // The corpus must exercise both outcomes.
    assert!(
        changed > 5_000,
        "only {changed} non-NFC strings in the corpus"
    );
}

#[test]
fn edge_cases() {
    for s in [
        "",
        "node_000123",
        "säätö",
        "sa\u{308}a\u{308}to\u{308}",
        "e\u{301}",
        "\u{e9}",
        "a\u{323}\u{302}",
        "a\u{302}\u{323}",
        "\u{1100}\u{1161}\u{11a8}",
        "\u{ac00}\u{11a8}",
        "\u{212b}",
        "\u{958}",
        "\u{11382}\u{113c9}",
        "\u{105d2}\u{307}",
        "a\u{1acf}\u{323}",
    ] {
        check(s);
    }
}
