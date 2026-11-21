//! Bijective, byte-order-preserving 64-bit key packing (`Pack64`) for ASCII IDs up to 9 bytes,
//! plus a thread-safe `OverflowInterner` fallback for longer or non-ASCII/NUL byte strings.

use std::cmp::Ordering;
use std::sync::RwLock;

use ahash::RandomState;
use hashbrown::HashTable;

pub const OVERFLOW_BIT: u64 = 1u64 << 63;
pub const EMPTY_KEY: u64 = 0u64;

/// Fast bijective splitmix64 finalizer for hashing 64-bit packed keys and IDs.
#[inline(always)]
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Encode up to 9 non-NUL 7-bit ASCII bytes into a 63-bit integer (bit 63 is 0)
/// whose unsigned `u64` ordering is identical to byte-wise `&[u8]` ordering.
#[inline(always)]
pub fn pack64_inline(s: &[u8]) -> Option<u64> {
    let len = s.len();
    if len > 9 {
        return None;
    }
    if len >= 8 {
        let p = s.as_ptr();
        let v = u64::from_le_bytes(unsafe { *(p as *const [u8; 8]) });
        if ((v | (v.wrapping_sub(0x0101_0101_0101_0101) & !v)) & 0x8080_8080_8080_8080) != 0 {
            return None;
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        let mut acc = unsafe { std::arch::x86_64::_pext_u64(v.to_be(), 0x7F7F_7F7F_7F7F_7F7F) } << 7;
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        let mut acc = unsafe {
            ((*p as u64) << 56)
                | ((*p.add(1) as u64) << 49)
                | ((*p.add(2) as u64) << 42)
                | ((*p.add(3) as u64) << 35)
                | ((*p.add(4) as u64) << 28)
                | ((*p.add(5) as u64) << 21)
                | ((*p.add(6) as u64) << 14)
                | ((*p.add(7) as u64) << 7)
        };
        if len == 9 {
            let b8 = unsafe { *p.add(8) };
            if b8 == 0 || b8 >= 128 {
                return None;
            }
            acc |= b8 as u64;
        }
        return Some(acc);
    }
    let mut acc = 0u64;
    let mut i = 0usize;
    while i < len {
        let b = unsafe { *s.get_unchecked(i) };
        if b == 0 || b >= 128 {
            return None;
        }
        acc |= (b as u64) << (56 - 7 * i);
        i += 1;
    }
    Some(acc)
}

/// Unpack an inline `Pack64` key into `out`, returning the byte length (`0..=9`).
#[inline(always)]
pub fn unpack64_inline(v: u64, out: &mut [u8; 9]) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        let hi8 =
            unsafe { std::arch::x86_64::_pdep_u64(v >> 7, 0x7F7F_7F7F_7F7F_7F7F) }.to_be_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(hi8.as_ptr(), out.as_mut_ptr(), 8);
        }
        let b8 = (v & 0x7F) as u8;
        out[8] = b8;
        if b8 != 0 {
            return 9;
        }
        let hi_le = u64::from_le_bytes(hi8);
        let zero_mask = hi_le.wrapping_sub(0x0101_0101_0101_0101) & !hi_le & 0x8080_8080_8080_8080;
        if zero_mask == 0 {
            8
        } else {
            (zero_mask.trailing_zeros() >> 3) as usize
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut len = 0usize;
        while len < 9 {
            let b = ((v >> (56 - 7 * len)) & 0x7F) as u8;
            if b == 0 {
                break;
            }
            out[len] = b;
            len += 1;
        }
        len
    }
}

struct OverflowState {
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
}

/// Fallback interner for IDs > 9 bytes or containing NUL / non-ASCII bytes.
/// Inline IDs never touch the lock.
pub struct OverflowInterner {
    state: RwLock<OverflowState>,
}

impl OverflowInterner {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(OverflowState {
                hasher: RandomState::with_seeds(0x1357, 0x2468, 0x9abc, 0xdef0),
                table: HashTable::new(),
                arena: Vec::new(),
                spans: Vec::new(),
            }),
        }
    }

    #[inline(always)]
    pub fn encode(&self, s: &[u8]) -> u64 {
        if let Some(k) = pack64_inline(s) {
            return k;
        }
        self.encode_slow(s)
    }

    #[cold]
    fn encode_slow(&self, s: &[u8]) -> u64 {
        {
            let st = self.state.read().unwrap();
            let h = st.hasher.hash_one(s);
            if let Some(&id) = st.table.find(h, |&idx| {
                let (o, l) = st.spans[idx as usize];
                &st.arena[o as usize..(o + l) as usize] == s
            }) {
                return OVERFLOW_BIT | (id as u64);
            }
        }
        let mut st = self.state.write().unwrap();
        let h = st.hasher.hash_one(s);
        if let Some(&id) = st.table.find(h, |&idx| {
            let (o, l) = st.spans[idx as usize];
            &st.arena[o as usize..(o + l) as usize] == s
        }) {
            return OVERFLOW_BIT | (id as u64);
        }
        let id = st.spans.len() as u32;
        let off = st.arena.len() as u32;
        st.arena.extend_from_slice(s);
        st.spans.push((off, s.len() as u32));
        let OverflowState {
            hasher,
            table,
            arena,
            spans,
        } = &mut *st;
        table.insert_unique(h, id, |&idx| {
            let (o, l) = spans[idx as usize];
            hasher.hash_one(&arena[o as usize..(o + l) as usize])
        });
        OVERFLOW_BIT | (id as u64)
    }

    /// Lookup already-encoded overflow key if present without mutating state.
    #[inline(always)]
    pub fn decode_with<'a>(
        &self,
        k: u64,
        buf: &'a mut [u8; 9],
        out_vec: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        if (k & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(k, buf);
            &buf[..len]
        } else {
            self.decode_slow(k, out_vec)
        }
    }

    #[cold]
    fn decode_slow<'a>(&self, k: u64, out_vec: &'a mut Vec<u8>) -> &'a [u8] {
        let id = (k & 0xFFFF_FFFF) as usize;
        let st = self.state.read().unwrap();
        let (o, l) = st.spans[id];
        out_vec.clear();
        out_vec.extend_from_slice(&st.arena[o as usize..(o + l) as usize]);
        out_vec.as_slice()
    }

    #[inline(always)]
    pub fn cmp_key(&self, a: u64, b: u64) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        if ((a | b) & OVERFLOW_BIT) == 0 {
            return a.cmp(&b);
        }
        self.cmp_key_slow(a, b)
    }

    #[cold]
    fn cmp_key_slow(&self, a: u64, b: u64) -> Ordering {
        let st = self.state.read().unwrap();
        let mut ba = [0u8; 9];
        let mut bb = [0u8; 9];
        let sa: &[u8] = if (a & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(a, &mut ba);
            &ba[..len]
        } else {
            let (o, l) = st.spans[(a & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        let sb: &[u8] = if (b & OVERFLOW_BIT) == 0 {
            let len = unpack64_inline(b, &mut bb);
            &bb[..len]
        } else {
            let (o, l) = st.spans[(b & 0xFFFF_FFFF) as usize];
            &st.arena[o as usize..(o + l) as usize]
        };
        sa.cmp(sb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack64_roundtrip_and_byte_order() {
        for len in 1..=9 {
            for bad_pos in 0..len {
                for bad_byte in [0u8, 0x80u8, 0xFFu8] {
                    let mut s = vec![b'a'; len];
                    s[bad_pos] = bad_byte;
                    assert_eq!(pack64_inline(&s), None, "len={len} pos={bad_pos} b={bad_byte}");
                }
            }
        }
        let mut samples: Vec<&[u8]> = vec![
            b"",
            b" ",
            b" U1",
            b"@",
            b"a@",
            b"U1",
            b"u",
            b"u1",
            b"u1 ",
            b"u10",
            b"u2",
            b"1234567@",
            b"12345678@",
            b"u9zzzzzzz",
            b"k\r",
            b"\x7f",
            b"~~~~~~~~~",
        ];
        let mut encoded: Vec<(u64, &[u8])> = samples
            .iter()
            .map(|&s| (pack64_inline(s).expect("fits inline"), s))
            .collect();
        for &(k, orig) in &encoded {
            assert_eq!(k & OVERFLOW_BIT, 0);
            if !orig.is_empty() {
                assert_ne!(k, EMPTY_KEY);
            }
            let mut buf = [0u8; 9];
            let len = unpack64_inline(k, &mut buf);
            assert_eq!(&buf[..len], orig);
        }
        samples.sort();
        encoded.sort_by_key(|x| x.0);
        let decoded: Vec<&[u8]> = encoded.iter().map(|x| x.1).collect();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn overflow_interner_mixed_ordering_and_identity() {
        let interner = OverflowInterner::new();
        let strings: Vec<&[u8]> = vec![
            b"u1",
            b"u123456789",
            b"u1234567890",
            b"U1234567890",
            "usuário_ç_1".as_bytes(),
            b"very_long_identifier_beyond_nine_bytes",
            b"a",
            b"a_long_overflow_id_2",
        ];
        let mut keys: Vec<(u64, &[u8])> =
            strings.iter().map(|&s| (interner.encode(s), s)).collect();
        for &(k, s) in &keys {
            assert_ne!(k, EMPTY_KEY);
            assert_eq!(interner.encode(s), k);
            let mut b9 = [0u8; 9];
            let mut v = Vec::new();
            assert_eq!(interner.decode_with(k, &mut b9, &mut v), s);
        }
        let mut expected = strings.clone();
        expected.sort();
        keys.sort_by(|a, b| interner.cmp_key(a.0, b.0));
        let got: Vec<&[u8]> = keys.into_iter().map(|x| x.1).collect();
        assert_eq!(got, expected);
    }
}
