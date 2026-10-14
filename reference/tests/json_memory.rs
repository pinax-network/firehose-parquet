//! `Json::parse`'s memory bound, measured: for each dense shape of answer,
//! the largest one the bound admits allocates no more than the bound while
//! it is parsed (its tree included), and the next larger one is refused
//! before its tree is built. A counting allocator sees every allocation of
//! this test binary, which has this one test, and counts a reallocation as
//! a new block beside the old one.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use fireparq_reference::json::{Json, ParseError};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call is `System`'s, with the same arguments; the counters
// only observe the sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The result of parsing `body` within `bound`, and the most it held at
/// once beyond what was held before.
fn measured(body: &str, bound: usize) -> (Result<Json, ParseError>, usize) {
    let before = CURRENT.load(Ordering::SeqCst);
    PEAK.store(before, Ordering::SeqCst);
    let parsed = Json::parse_within(body.as_bytes(), bound);
    (parsed, PEAK.load(Ordering::SeqCst) - before)
}

/// The largest `n` whose `body(n)` parses within `bound`: doubling, then
/// bisection.
fn largest_admitted(body: &dyn Fn(usize) -> String, bound: usize) -> usize {
    let admitted = |n: usize| Json::parse_within(body(n).as_bytes(), bound).is_ok();
    let mut high = 1;
    while admitted(high) {
        high *= 2;
    }
    let mut low = high / 2;
    while high - low > 1 {
        let middle = (low + high) / 2;
        if admitted(middle) {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

/// A document of `n` items of one shape.
type Body = Box<dyn Fn(usize) -> String>;

/// An array of `n` copies of `item`.
fn array_of(item: &str) -> Body {
    let item = item.to_owned();
    Box::new(move |n| format!("[{}]", vec![item.as_str(); n].join(",")))
}

#[test]
fn the_bound_covers_what_parsing_allocates() {
    const BOUND: usize = 1 << 20;
    let twelve_keys = (0..12)
        .map(|key| format!(r#""k{key}":{key}"#))
        .collect::<Vec<_>>()
        .join(",");
    let twelve_keys = format!("{{{twelve_keys}}}");
    let mut shapes: Vec<(String, Body)> = [
        "0",
        "-1.5e-7",
        r#""""#,
        r#""a\"b\té""#,
        "true",
        "[]",
        "[0]",
        "{}",
        r#"{"a":0}"#,
        r#"{"a":{"b":[0]}}"#,
        twelve_keys.as_str(),
    ]
    .into_iter()
    .map(|item| (format!("an array of {item}"), array_of(item)))
    .collect();
    shapes.push((
        "one object".into(),
        Box::new(|n| {
            let entries: Vec<String> = (0..n).map(|key| format!(r#""k{key:07}":{key}"#)).collect();
            format!("{{{}}}", entries.join(","))
        }),
    ));
    for (shape, body) in &shapes {
        let n = largest_admitted(body.as_ref(), BOUND);
        assert!(n > 100, "{shape}: only {n} admitted");
        let admitted = body(n);
        let (parsed, peak) = measured(&admitted, BOUND);
        assert!(parsed.is_ok(), "{shape}");
        assert!(peak <= BOUND, "{shape}: {n} took {peak} bytes");
        let refused = body(n + 1);
        let (parsed, peak) = measured(&refused, BOUND);
        assert_eq!(parsed, Err(ParseError::TooLarge), "{shape}");
        assert!(peak < 1024, "{shape}: refused after {peak} bytes");
    }
}
