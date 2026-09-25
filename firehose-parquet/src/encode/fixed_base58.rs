//! Fixed-width Base58 encoding using base 58^5 long division.
//!
//! This deliberately uses only safe, portable integer operations. The public
//! encoder keeps `bs58` for arbitrary lengths and all decoding.
#![forbid(unsafe_code)]

const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const RADIX: u64 = 58u64.pow(5);

pub(super) fn push_32(bytes: &[u8; 32], out: &mut Vec<u8>) {
    push::<8>(bytes, out);
}

pub(super) fn push_64(bytes: &[u8; 64], out: &mut Vec<u8>) {
    push::<16>(bytes, out);
}

#[inline]
fn push<const LIMBS: usize>(bytes: &[u8], out: &mut Vec<u8>) {
    // Only the two fixed-size wrappers above instantiate this function.
    assert_eq!(bytes.len(), LIMBS * 4);
    let mut limbs = [0u32; LIMBS];
    for (limb, chunk) in limbs.iter_mut().zip(bytes.chunks_exact(4)) {
        *limb = u32::from_be_bytes(chunk.try_into().expect("four-byte chunk"));
    }

    // 64 bytes need at most 88 Base58 digits, rounded to 18 five-digit chunks.
    let mut text = [0u8; 90];
    let mut pos = text.len();
    let mut first = 0;
    while first < LIMBS {
        if limbs[first] == 0 {
            first += 1;
            continue;
        }
        let mut remainder = 0u64;
        for limb in &mut limbs[first..] {
            // remainder < 58^5 < 2^30, so wide < 2^62. The quotient fits
            // u32 because wide < RADIX * 2^32. No overflowing arithmetic.
            let wide = (remainder << 32) | u64::from(*limb);
            *limb = (wide / RADIX) as u32;
            remainder = wide % RADIX;
        }
        // One division emits five digits. Constant divisors let the compiler
        // use reciprocal multiplication instead of per-byte long division.
        let mut chunk = remainder as u32;
        for _ in 0..5 {
            pos -= 1;
            text[pos] = ALPHABET[(chunk % 58) as usize];
            chunk /= 58;
        }
    }

    // Strip numerical padding, then preserve exactly one '1' per leading
    // input zero byte. This also handles an entirely zero input.
    while pos < text.len() && text[pos] == b'1' {
        pos += 1;
    }
    let zeros = bytes.iter().take_while(|&&byte| byte == 0).count();
    out.resize(out.len() + zeros, b'1');
    out.extend_from_slice(&text[pos..]);
}
