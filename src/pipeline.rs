use std::cmp::Ordering;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as MemOrdering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use ahash::RandomState;
use hashbrown::{HashMap, HashTable};
use memmap2::{Mmap, MmapOptions};

use crate::csv::{
    for_each_data_line, parse_csv_line, parse_ingest_fast_sec, parse_ingest_with_hint,
    parse_price_cents, parse_ts, parse_ts_rel_ms, parse_ts_rel_ms_with_hint, scan_csv_mmap_chunk,
    scan_csv_mmap_chunk_sink, CsvRowSink, FastDateCache, IngestHint, CLICK_REACH_MS, DAY_MS,
    LATE_LIMIT_MS, SESSION_GAP_MS,
};
use crate::huge::HugeSlab;
use crate::links::{build_link_redirect, LinkRedirect};
use crate::pack::{mix64, OverflowInterner, EMPTY_KEY, OVERFLOW_BIT};
use crate::parquet_writer::{
    write_campaign_daily_parquet, write_empty_user_daily, UserDailyParquetWriter,
};

const PERSON_BUCKETS: usize = 1024;
const PERSON_MASK: usize = PERSON_BUCKETS - 1;

const VIEW_U32: u32 = u32::from_ne_bytes(*b"view");
const CART_U32: u32 = u32::from_ne_bytes(*b"cart");
const PURCH_U64: u64 = u64::from_ne_bytes(*b"purchase");

#[inline(always)]
fn prefetch_ptr<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(
            ptr as *const i8,
            std::arch::x86_64::_MM_HINT_T0,
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

/// Break demand-zero page faults upfront on `len == 0` spare vector capacity.
#[inline]
unsafe fn touch_uninit_cap<T>(v: &mut Vec<T>) {
    debug_assert_eq!(v.len(), 0);
    let bytes = v.capacity() * std::mem::size_of::<T>();
    if bytes == 0 {
        return;
    }
    let p = v.as_mut_ptr() as *mut u8;
    let mut off = 0usize;
    while off < bytes {
        std::ptr::write_volatile(p.add(off), 0);
        off += 4096;
    }
    std::ptr::write_volatile(p.add(bytes - 1), 0);
}

/// Break COW zero-page faults upfront on initialized slices via read-modify-write.
#[inline]
unsafe fn touch_init_cow<T>(s: &mut [T]) {
    let bytes = std::mem::size_of_val(s);
    if bytes == 0 {
        return;
    }
    let p = s.as_mut_ptr() as *mut u8;
    let mut off = 0usize;
    while off < bytes {
        let b = std::ptr::read_volatile(p.add(off));
        std::ptr::write_volatile(p.add(off), b);
        off += 4096;
    }
    let last = bytes - 1;
    let b = std::ptr::read_volatile(p.add(last));
    std::ptr::write_volatile(p.add(last), b);
}

// =========================================================================
// Fast Direct Lookup Indexes with General Hash Fallbacks
// =========================================================================

#[inline(always)]
fn parse_p_num(b: &[u8]) -> Option<usize> {
    if b.len() < 2 || b.len() > 8 || b[0] != b'p' || b[1] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
    }
    if val > 2_000_000 {
        None
    } else {
        Some(val)
    }
}

#[inline(always)]
fn parse_c_num(b: &[u8]) -> Option<usize> {
    if b.len() < 3 || b.len() > 8 || b[0] != b'c' || b[1] != b'_' || b[2] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[2..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
    }
    if val > 100_000 {
        None
    } else {
        Some(val)
    }
}

struct ProductIndex {
    direct: Vec<u16>,
    has_fallback: bool,
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    prod_cat: Vec<u32>,
    pub categories: Vec<String>,
    pub unknown_cat: u32,
}

impl ProductIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read products.csv");
        let mut raw_prods: Vec<(&[u8], &[u8])> = Vec::with_capacity(65_536);
        let mut cats_set: Vec<&[u8]> = vec![b"unknown".as_slice()];
        let mut max_p = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                if !pid.is_empty() && !cat.is_empty() {
                    if let Some(n) = parse_p_num(pid) {
                        if n > max_p {
                            max_p = n;
                        }
                    }
                    raw_prods.push((pid, cat));
                    if !cats_set.iter().any(|&c| c == cat) {
                        cats_set.push(cat);
                    }
                }
            }
        });
        cats_set.sort_unstable();

        let unknown_cat = cats_set
            .binary_search(&b"unknown".as_slice())
            .expect("unknown present") as u32;
        let categories: Vec<String> = cats_set
            .iter()
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect();

        let use_direct = categories.len() <= u16::MAX as usize && max_p > 0;
        let mut direct = if use_direct {
            vec![unknown_cat as u16; max_p + 1]
        } else {
            Vec::new()
        };

        let hasher = RandomState::with_seeds(11, 22, 33, 44);
        let mut table = HashTable::new();
        let mut arena = Vec::new();
        let mut spans: Vec<(u32, u32)> = Vec::new();
        let mut prod_cat: Vec<u32> = Vec::new();

        for (pid, cat) in raw_prods {
            let cid = if cats_set.len() <= 64 {
                cats_set.iter().position(|&c| c == cat).unwrap() as u32
            } else {
                cats_set.binary_search(&cat).unwrap() as u32
            };
            if use_direct {
                if let Some(n) = parse_p_num(pid) {
                    if n < direct.len() {
                        direct[n] = cid as u16;
                        continue;
                    }
                }
            }
            let idx = spans.len() as u32;
            let off = arena.len() as u32;
            arena.extend_from_slice(pid);
            spans.push((off, pid.len() as u32));
            prod_cat.push(cid);
            let h = hasher.hash_one(pid);
            table.insert_unique(h, idx, |&id| {
                let (o, l) = spans[id as usize];
                hasher.hash_one(&arena[o as usize..(o + l) as usize])
            });
        }

        let has_fallback = !spans.is_empty();
        Self {
            direct,
            has_fallback,
            hasher,
            table,
            arena,
            spans,
            prod_cat,
            categories,
            unknown_cat,
        }
    }

    #[inline(always)]
    fn lookup(&self, product_id: &[u8]) -> u32 {
        if let Some(n) = parse_p_num(product_id) {
            if n < self.direct.len() {
                return unsafe { *self.direct.get_unchecked(n) } as u32;
            }
        }
        if !self.has_fallback {
            return self.unknown_cat;
        }
        self.lookup_slow(product_id)
    }

    #[cold]
    fn lookup_slow(&self, product_id: &[u8]) -> u32 {
        let h = self.hasher.hash_one(product_id);
        self.table
            .find(h, |&id| {
                let (o, l) = self.spans[id as usize];
                &self.arena[o as usize..(o + l) as usize] == product_id
            })
            .map_or(self.unknown_cat, |&id| self.prod_cat[id as usize])
    }
}

struct CampaignIndex {
    direct: Vec<u32>,
    has_fallback: bool,
    hasher: RandomState,
    table: HashTable<u32>,
    pub ids: Vec<String>,
    pub channels: Vec<String>,
    pub organic_id: u32,
}

impl CampaignIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read campaigns.csv");
        let mut raw: Vec<(&[u8], &[u8])> = Vec::with_capacity(2048);
        let mut max_c = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([cid, ch]) = parse_csv_line::<2>(line) {
                if !cid.is_empty() {
                    if let Some(n) = parse_c_num(cid) {
                        if n > max_c {
                            max_c = n;
                        }
                    }
                    raw.push((cid, ch));
                }
            }
        });

        let mut direct = if max_c > 0 {
            vec![u32::MAX; max_c + 1]
        } else {
            Vec::new()
        };

        let hasher = RandomState::with_seeds(55, 66, 77, 88);
        let mut table = HashTable::new();
        let mut ids = Vec::with_capacity(raw.len() + 1);
        let mut channels = Vec::with_capacity(raw.len() + 1);
        let mut has_fallback = false;

        for (i, (cid, ch)) in raw.into_iter().enumerate() {
            let camp_idx = i as u32;
            ids.push(String::from_utf8_lossy(cid).into_owned());
            channels.push(String::from_utf8_lossy(ch).into_owned());
            if let Some(n) = parse_c_num(cid) {
                if n < direct.len() {
                    direct[n] = camp_idx;
                    continue;
                }
            }
            has_fallback = true;
            let h = hasher.hash_one(cid);
            table.insert_unique(h, camp_idx, |&id| {
                hasher.hash_one(ids[id as usize].as_bytes())
            });
        }

        let organic_id = ids.len() as u32;
        ids.push("organic".to_string());
        channels.push("organic".to_string());

        Self {
            direct,
            has_fallback,
            hasher,
            table,
            ids,
            channels,
            organic_id,
        }
    }

    #[inline(always)]
    fn lookup(&self, camp_id: &[u8]) -> Option<u32> {
        if let Some(n) = parse_c_num(camp_id) {
            if n < self.direct.len() {
                let v = unsafe { *self.direct.get_unchecked(n) };
                return if v == u32::MAX { None } else { Some(v) };
            }
        }
        if !self.has_fallback {
            return None;
        }
        self.lookup_slow(camp_id)
    }

    #[cold]
    fn lookup_slow(&self, camp_id: &[u8]) -> Option<u32> {
        let h = self.hasher.hash_one(camp_id);
        self.table
            .find(h, |&id| self.ids[id as usize].as_bytes() == camp_id)
            .copied()
    }
}

fn parse_manifest(path: &Path) -> (i64, i64) {
    let text = fs::read_to_string(path).expect("read manifest.json");
    let extract = |key: &str| -> i64 {
        let pat = format!("\"{key}\"");
        let kpos = text.find(&pat).expect("manifest key");
        let after_colon = text[kpos + pat.len()..].find(':').expect("colon") + kpos + pat.len() + 1;
        let q1 = text[after_colon..].find('"').expect("q1") + after_colon + 1;
        let q2 = text[q1..].find('"').expect("q2") + q1;
        parse_ts(text[q1..q2].as_bytes()).expect("valid manifest ts")
    };
    (extract("window_start"), extract("window_end"))
}

fn list_csvs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("csv"))
        .collect();
    paths.sort();
    paths
}

// =========================================================================
// Zero-Realloc Contiguous Slab Buckets for Low-DTLB Routing
// =========================================================================

struct SlabBucketsOut<T> {
    slabs: Vec<HugeSlab<T>>,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

impl<T> SlabBucketsOut<T> {
    #[inline(always)]
    unsafe fn get_slice<'a>(
        all_slabs: &'a [Vec<HugeSlab<T>>],
        tid: u8,
        sidx: u16,
        start: u32,
        len: u16,
    ) -> &'a [T] {
        let slab = all_slabs
            .get_unchecked(tid as usize)
            .get_unchecked(sidx as usize);
        let raw = slab.get_unchecked(start as usize, len as usize);
        std::slice::from_raw_parts(raw.as_ptr() as *const T, len as usize)
    }
}

struct DirectSlabBuckets<T: Copy, const BLOCK: usize> {
    num_buckets: usize,
    extra_slab_elems: usize,
    cur_ptr: Vec<*mut T>,
    rem: Vec<u16>,
    active_meta: Vec<(u16, u32)>,
    slabs: Vec<HugeSlab<T>>,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

unsafe impl<T: Copy + Send, const BLOCK: usize> Send for DirectSlabBuckets<T, BLOCK> {}

impl<T: Copy, const BLOCK: usize> DirectSlabBuckets<T, BLOCK> {
    const _CHECK: () = assert!(BLOCK >= 16 && BLOCK <= u16::MAX as usize);

    fn new(num_buckets: usize, initial_slab_elems: usize, extra_slab_elems: usize) -> Self {
        let _ = Self::_CHECK;
        let init_cap = ((initial_slab_elems.max(BLOCK) + BLOCK - 1) / BLOCK) * BLOCK;
        let extra_cap = ((extra_slab_elems.max(BLOCK) + BLOCK - 1) / BLOCK) * BLOCK;
        let mut slabs = Vec::with_capacity(4);
        slabs.push(HugeSlab::with_capacity(init_cap));
        Self {
            num_buckets,
            extra_slab_elems: extra_cap,
            cur_ptr: vec![std::ptr::NonNull::<T>::dangling().as_ptr(); num_buckets],
            rem: vec![0u16; num_buckets],
            active_meta: vec![(u16::MAX, 0); num_buckets],
            slabs,
            bucket_runs: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[cold]
    fn refill_block(&mut self, b: usize) {
        let (prev_sidx, prev_start) = unsafe { *self.active_meta.get_unchecked(b) };
        if prev_sidx != u16::MAX {
            unsafe { self.bucket_runs.get_unchecked_mut(b) }.push((
                prev_sidx,
                prev_start,
                BLOCK as u16,
            ));
        }
        let last_ok = !self.slabs.is_empty() && {
            let last = &self.slabs[self.slabs.len() - 1];
            last.len() + BLOCK <= last.capacity()
        };
        if !last_ok {
            self.slabs.push(HugeSlab::with_capacity(self.extra_slab_elems));
        }
        let sidx = self.slabs.len() - 1;
        let slab = &mut self.slabs[sidx];
        let start = slab.len();
        unsafe {
            slab.set_len(start + BLOCK);
            let ptr = (slab.as_mut_ptr() as *mut T).add(start);
            *self.cur_ptr.get_unchecked_mut(b) = ptr;
            *self.active_meta.get_unchecked_mut(b) = (sidx as u16, start as u32);
            *self.rem.get_unchecked_mut(b) = BLOCK as u16;
        }
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let mut r = unsafe { *self.rem.get_unchecked(b) };
        if r == 0 {
            self.refill_block(b);
            r = BLOCK as u16;
        }
        unsafe {
            let p = *self.cur_ptr.get_unchecked(b);
            p.write(item);
            *self.cur_ptr.get_unchecked_mut(b) = p.add(1);
            *self.rem.get_unchecked_mut(b) = r - 1;
        }
    }

    fn finish(mut self) -> SlabBucketsOut<T> {
        for b in 0..self.num_buckets {
            let (sidx, start) = self.active_meta[b];
            if sidx != u16::MAX {
                let filled = (BLOCK as u16) - self.rem[b];
                if filled > 0 {
                    self.bucket_runs[b].push((sidx, start, filled));
                }
            }
        }
        SlabBucketsOut {
            slabs: self.slabs,
            bucket_runs: self.bucket_runs,
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct StageBuf<T: Copy, const K: usize>([T; K]);

/// L1-staged, 64-byte-aligned non-temporal (`_mm256_stream_si256`) slab bucket router
/// that eliminates RFO cache-line ownership traffic and DRAM thrashing across 128+ buckets.
struct NtSlabBuckets<T: Copy + Default, const K: usize, const BLOCK: usize> {
    num_buckets: usize,
    extra_slab_elems: usize,
    stage: Vec<StageBuf<T, K>>,
    stage_len: Vec<u8>,
    cursor: Vec<u32>,
    active_meta: Vec<(u16, u32)>,
    slabs: Vec<HugeSlab<T>>,
    base_ptr: *mut T,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

unsafe impl<T: Copy + Default + Send, const K: usize, const BLOCK: usize> Send
    for NtSlabBuckets<T, K, BLOCK>
{
}

impl<T: Copy + Default, const K: usize, const BLOCK: usize> NtSlabBuckets<T, K, BLOCK> {
    const _CHECK: () = {
        assert!(BLOCK >= K && BLOCK <= u16::MAX as usize);
        assert!(BLOCK.is_power_of_two());
        assert!(BLOCK % K == 0);
        assert!(K <= u8::MAX as usize);
        assert!((K * std::mem::size_of::<T>()) % 64 == 0);
        assert!((BLOCK * std::mem::size_of::<T>()) % 64 == 0);
    };

    fn new(num_buckets: usize, initial_slab_elems: usize, extra_slab_elems: usize) -> Self {
        let _ = Self::_CHECK;
        let init_cap = (((initial_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK)
            .max(BLOCK * 2);
        let extra_cap =
            (((extra_slab_elems.max(BLOCK * 2) + BLOCK - 1) / BLOCK) * BLOCK).max(BLOCK * 2);
        let mut first_slab = HugeSlab::with_capacity(init_cap);
        unsafe { first_slab.set_len(BLOCK) };
        let base_ptr = first_slab.as_mut_ptr() as *mut T;
        Self {
            num_buckets,
            extra_slab_elems: extra_cap,
            stage: vec![StageBuf([T::default(); K]); num_buckets],
            stage_len: vec![0u8; num_buckets],
            cursor: vec![0u32; num_buckets],
            active_meta: vec![(u16::MAX, 0); num_buckets],
            slabs: vec![first_slab],
            base_ptr,
            bucket_runs: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[cold]
    fn refill_block(&mut self, b: usize) -> u32 {
        let (prev_sidx, prev_start) = unsafe { *self.active_meta.get_unchecked(b) };
        if prev_sidx != u16::MAX {
            unsafe { self.bucket_runs.get_unchecked_mut(b) }.push((
                prev_sidx,
                prev_start,
                BLOCK as u16,
            ));
            unsafe { *self.active_meta.get_unchecked_mut(b) = (u16::MAX, 0) };
        }
        let cur_sidx = self.slabs.len() - 1;
        if self.slabs[cur_sidx].len() + BLOCK > self.slabs[cur_sidx].capacity() {
            for i in 0..self.num_buckets {
                let (asidx, astart) = self.active_meta[i];
                if asidx != u16::MAX {
                    let filled = (self.cursor[i] - astart) as u16;
                    if filled > 0 {
                        self.bucket_runs[i].push((asidx, astart, filled));
                    }
                    self.active_meta[i] = (u16::MAX, 0);
                }
                self.cursor[i] = 0;
            }
            let mut next_slab = HugeSlab::with_capacity(self.extra_slab_elems);
            unsafe { next_slab.set_len(BLOCK) };
            self.base_ptr = next_slab.as_mut_ptr() as *mut T;
            self.slabs.push(next_slab);
        }
        let sidx = self.slabs.len() - 1;
        let slab = &mut self.slabs[sidx];
        let start = slab.len();
        unsafe { slab.set_len(start + BLOCK) };
        self.active_meta[b] = (sidx as u16, start as u32);
        start as u32
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let l = unsafe { *self.stage_len.get_unchecked(b) } as usize;
        unsafe {
            *self
                .stage
                .get_unchecked_mut(b)
                .0
                .get_unchecked_mut(l) = item;
        }
        let nl = l + 1;
        if nl < K {
            unsafe { *self.stage_len.get_unchecked_mut(b) = nl as u8 };
            return;
        }
        self.flush_k(b);
    }

    #[inline(never)]
    fn flush_k(&mut self, b: usize) {
        let mut idx = unsafe { *self.cursor.get_unchecked(b) };
        if (idx & ((BLOCK as u32) - 1)) == 0 {
            idx = self.refill_block(b);
        }
        unsafe {
            let src = self.stage.get_unchecked(b).0.as_ptr() as *const u8;
            let dst = self.base_ptr.add(idx as usize) as *mut u8;
            let bytes = K * std::mem::size_of::<T>();
            #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
            {
                use std::arch::x86_64::*;
                let mut off = 0usize;
                while off < bytes {
                    let v = _mm256_load_si256(src.add(off) as *const __m256i);
                    _mm256_stream_si256(dst.add(off) as *mut __m256i, v);
                    off += 32;
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
            {
                std::ptr::copy_nonoverlapping(src, dst, bytes);
            }
            *self.cursor.get_unchecked_mut(b) = idx + K as u32;
            *self.stage_len.get_unchecked_mut(b) = 0;
        }
    }

    fn finish(mut self) -> SlabBucketsOut<T> {
        for b in 0..self.num_buckets {
            let rem = self.stage_len[b] as usize;
            if rem > 0 {
                let mut idx = self.cursor[b];
                if (idx & ((BLOCK as u32) - 1)) == 0 {
                    idx = self.refill_block(b);
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.stage[b].0.as_ptr(),
                        self.base_ptr.add(idx as usize),
                        rem,
                    );
                }
                self.cursor[b] = idx + rem as u32;
                self.stage_len[b] = 0;
            }
        }
        for b in 0..self.num_buckets {
            let (sidx, start) = self.active_meta[b];
            if sidx != u16::MAX {
                let filled = (self.cursor[b] - start) as u16;
                if filled > 0 {
                    self.bucket_runs[b].push((sidx, start, filled));
                }
            }
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        SlabBucketsOut {
            slabs: self.slabs,
            bucket_runs: self.bucket_runs,
        }
    }
}

// =========================================================================
// Compact Event & Click Representations + Inline Ingest Packing
// =========================================================================

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawEvent {
    eid: u64,
    uid: u64,
    packed: u64,
}

#[derive(Clone, Copy)]
struct ExtraEvent {
    ts: i64,
    ingest_ts: i64,
    price_cents: u32,
    cat_id: u32,
}

#[inline(always)]
fn inline_ingest_sec(ingest_ts: i64, win_start: i64) -> Option<i32> {
    let diff = ingest_ts.checked_sub(win_start)?;
    if diff % 1000 != 0 {
        return None;
    }
    let sec = diff / 1000;
    if sec > (i32::MIN as i64) && sec <= (i32::MAX as i64) {
        Some(sec as i32)
    } else {
        None
    }
}

#[inline(always)]
fn cmp_ingest(
    a: &RawEvent,
    b: &RawEvent,
    extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
) -> Ordering {
    let la = (a.packed as u32) as i32;
    let lb = (b.packed as u32) as i32;
    if la != i32::MIN && lb != i32::MIN {
        return la.cmp(&lb);
    }
    let get_exact = |r: &RawEvent, l: i32| -> i64 {
        if l != i32::MIN {
            win_start + (l as i64) * 1000
        } else {
            let eref = (r.packed >> 32) & ((1u64 << 30) - 1);
            let tid = (eref >> idx_bits) as usize;
            let eidx = (eref & idx_mask) as usize;
            extras[tid][eidx].ingest_ts
        }
    };
    get_exact(a, la).cmp(&get_exact(b, lb))
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SurvEvent {
    person: u64,
    ts_rel: u32,
    price_cat: u32, // 0 for view/cart; (cat_id << 24) | price_cents for purchase
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SurvClick {
    person: u64,
    cid: u64,
    ts_rel: u32, // ms relative to click_win_start (= ts_rel_ev + CLICK_REACH_MS)
    camp_idx: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CompactClick {
    cid: u64,
    ts_rel: u32,
    camp_idx: u32,
}

#[derive(Clone, Copy, Default)]
struct SurvEventWide {
    person: u64,
    ts: i64,
    price_cents: u32,
    cat_id: u32,
}

#[derive(Clone, Copy, Default)]
struct SurvClickWide {
    person: u64,
    cid: u64,
    ts: i64,
    camp_idx: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CampCore {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
    assisted_sessions: i32,
}

enum CampStore {
    Flat {
        core: Vec<CampCore>,
        assisted: Vec<i32>,
        min_day: i32,
        day_span: usize,
    },
    Map(HashMap<(u32, i32), CampAgg, RandomState>),
}

impl CampStore {
    fn prefault(&mut self) {
        if let CampStore::Flat { core, assisted, .. } = self {
            unsafe {
                touch_init_cow(core.as_mut_slice());
                touch_init_cow(assisted.as_mut_slice());
            }
        }
    }

    fn new(use_flat: bool, num_camps: usize, min_day: i32, day_span: usize) -> Self {
        if use_flat {
            CampStore::Flat {
                core: vec![CampCore::default(); num_camps * day_span],
                assisted: vec![0i32; num_camps * day_span],
                min_day,
                day_span,
            }
        } else {
            CampStore::Map(HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9)))
        }
    }

    #[inline(always)]
    fn add_credited(&mut self, camp_idx: u32, day: i32, sess_rev: i64) {
        match self {
            CampStore::Flat {
                core,
                min_day,
                day_span,
                ..
            } => {
                let d = (day - *min_day) as usize;
                let slot = (camp_idx as usize) * (*day_span) + d;
                let agg = unsafe { core.get_unchecked_mut(slot) };
                agg.sessions += 1;
                if sess_rev > 0 {
                    agg.purchasing_sessions += 1;
                    agg.revenue_cents += sess_rev;
                }
            }
            CampStore::Map(m) => {
                let agg = m.entry((camp_idx, day)).or_default();
                agg.sessions += 1;
                if sess_rev > 0 {
                    agg.purchasing_sessions += 1;
                    agg.revenue_cents += sess_rev;
                }
            }
        }
    }

    #[inline(always)]
    fn add_assisted(&mut self, camp_idx: u32, day: i32, delta: i32) {
        match self {
            CampStore::Flat {
                assisted,
                min_day,
                day_span,
                ..
            } => {
                let d = (day - *min_day) as usize;
                let slot = (camp_idx as usize) * (*day_span) + d;
                unsafe {
                    *assisted.get_unchecked_mut(slot) += delta;
                }
            }
            CampStore::Map(m) => {
                m.entry((camp_idx, day)).or_default().assisted_sessions += delta;
            }
        }
    }
}

// =========================================================================
// Stage 4a: Ultra-Fast Compact Path (Hash-Grouping + 8B Events + 8-Bit LSD Radix + Zero-Copy Parquet)
// =========================================================================

/// 4-pass 8-bit LSD radix sort on high 32 bits (`ts_rel`) of `grouped_evs` (`u64`),
/// using a 4 KB L1d-hot histogram table and skipping uniform passes.
#[inline]
fn radix_sort_events_u64(arr: &mut [u64], tmp: &mut Vec<u64>) {
    let n = arr.len();
    if n <= 1 {
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp_slice = &mut tmp[..n];

    let mut counts = [[0u32; 256]; 4];
    for &v in arr.iter() {
        let k = (v >> 32) as u32;
        counts[0][(k & 0xFF) as usize] += 1;
        counts[1][((k >> 8) & 0xFF) as usize] += 1;
        counts[2][((k >> 16) & 0xFF) as usize] += 1;
        counts[3][((k >> 24) & 0xFF) as usize] += 1;
    }

    let n_u32 = n as u32;
    let first_k = (arr[0] >> 32) as u32;
    let mut in_tmp = false;

    for pass in 0..4usize {
        let shift = 32 + pass * 8;
        let b0 = ((first_k >> (pass * 8)) & 0xFF) as usize;
        if counts[pass][b0] == n_u32 {
            continue;
        }
        let mut off = [0u32; 256];
        let mut sum = 0u32;
        for b in 0..256 {
            off[b] = sum;
            sum += counts[pass][b];
        }
        if !in_tmp {
            for &v in arr.iter() {
                let b = ((v >> shift) & 0xFF) as usize;
                let dst = off[b] as usize;
                off[b] += 1;
                unsafe {
                    *tmp_slice.get_unchecked_mut(dst) = v;
                }
            }
        } else {
            for &v in tmp_slice.iter() {
                let b = ((v >> shift) & 0xFF) as usize;
                let dst = off[b] as usize;
                off[b] += 1;
                unsafe {
                    *arr.get_unchecked_mut(dst) = v;
                }
            }
        }
        in_tmp = !in_tmp;
    }

    if in_tmp {
        arr.copy_from_slice(tmp_slice);
    }
}

#[inline(always)]
fn cmp_compact_click(a: &CompactClick, b: &CompactClick, overflow: &OverflowInterner) -> Ordering {
    a.ts_rel.cmp(&b.ts_rel).then_with(|| {
        if a.cid == b.cid {
            Ordering::Equal
        } else if ((a.cid | b.cid) & OVERFLOW_BIT) == 0 {
            b.cid.cmp(&a.cid)
        } else {
            overflow.cmp_key(b.cid, a.cid)
        }
    })
}

fn process_person_buckets_compact(
    num_threads: usize,
    all_ev_slabs: &[Vec<HugeSlab<SurvEvent>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<HugeSlab<SurvClick>>],
    ck_runs: &[Vec<(u8, u16, u32, u16)>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    win_start: i64,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
    drop_gate: &(Mutex<bool>, Condvar),
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;
    let start_in_day = win_start.rem_euclid(DAY_MS) as u64;

    // Sort buckets largest-first by event+click count to eliminate whale tail latency
    let mut total_surv_ev = 0usize;
    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
            if ev_n == 0 {
                None
            } else {
                total_surv_ev += ev_n;
                let ck_n: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum();
                Some((ev_n + ck_n, pb))
            }
        })
        .collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work_order: Vec<usize> = order.into_iter().map(|x| x.1).collect();

    if work_order.is_empty() {
        *drop_gate.0.lock().unwrap() = true;
        drop_gate.1.notify_all();
        return (Vec::new(), false);
    }

    let est_rows_per_thread = ((total_surv_ev / 8 / nt).max(4_096)).min(393_216);
    let next_idx = AtomicUsize::new(0);
    let ready_cnt = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;
        let ready_cnt_ref = &ready_cnt;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store = CampStore::new(use_flat, num_camps, min_day, day_span);
                camp_store.prefault();
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer =
                    UserDailyParquetWriter::new(part_path, &products.categories, est_rows_per_thread);
                pq_writer.prefault();

                // Thread-local reusable buffers (2048 * 8B = 16KB fits L1d)
                let mut ht_keys: Vec<u64> = vec![EMPTY_KEY; 2048];
                let mut ht_pids: Vec<u32> = vec![0u32; 2048];

                let mut person_keys: Vec<u64> = Vec::with_capacity(4096);
                let mut ev_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_pids: Vec<u32> = Vec::with_capacity(65_536);
                let mut bucket_evs: Vec<u64> = Vec::with_capacity(65_536);
                let mut grouped_evs: Vec<u64> = Vec::with_capacity(65_536);
                let mut radix_tmp: Vec<u64> = Vec::with_capacity(8192);

                let mut ck_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut ck_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_ck_off: Vec<u32> = Vec::with_capacity(4096);
                let mut matched_cks: Vec<(u32, CompactClick)> = Vec::with_capacity(16_384);
                let mut grouped_cks: Vec<CompactClick> = Vec::with_capacity(16_384);

                let mut camp_in_win: Vec<u32> = vec![0u32; num_camps];
                let mut camp_active_since: Vec<u32> = vec![u32::MAX; num_camps];
                let mut in_active: Vec<u8> = vec![0u8; num_camps];
                let mut active_camps: Vec<u32> = Vec::with_capacity(64);
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut cat_dirty: Vec<usize> = Vec::with_capacity(16);

                let mut buf9 = [0u8; 9];
                let mut tmp_vec: Vec<u8> = Vec::with_capacity(64);
                let mut uid_pref_vec: Vec<u8> = Vec::with_capacity(64);

                unsafe {
                    touch_uninit_cap(&mut person_keys);
                    touch_uninit_cap(&mut ev_cnt);
                    touch_uninit_cap(&mut ev_off);
                    touch_uninit_cap(&mut cur_ev_off);
                    touch_uninit_cap(&mut ev_pids);
                    touch_uninit_cap(&mut bucket_evs);
                    touch_uninit_cap(&mut grouped_evs);
                    touch_uninit_cap(&mut radix_tmp);
                    touch_uninit_cap(&mut ck_cnt);
                    touch_uninit_cap(&mut ck_off);
                    touch_uninit_cap(&mut cur_ck_off);
                    touch_uninit_cap(&mut matched_cks);
                    touch_uninit_cap(&mut grouped_cks);
                    touch_init_cow(&mut ht_keys);
                    touch_init_cow(&mut ht_pids);
                    touch_init_cow(&mut camp_in_win);
                    touch_init_cow(&mut camp_active_since);
                    touch_init_cow(&mut in_active);
                    touch_init_cow(&mut cat_rev);
                }
                if ready_cnt_ref.fetch_add(1, MemOrdering::AcqRel) + 1 == nt {
                    *drop_gate.0.lock().unwrap() = true;
                    drop_gate.1.notify_all();
                }

                loop {
                    let wi = next_idx_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&pb) = work_order_ref.get(wi) else {
                        break;
                    };
                    let pb_ev_runs = &ev_runs[pb];
                    let total_ev: usize = pb_ev_runs.iter().map(|r| r.3 as usize).sum();
                    if total_ev == 0 {
                        continue;
                    }

                    // Pass 1: Hash-group surviving events by person & copy compact 8B payload once
                    let mut cur_ht_cap = 2048usize;
                    ht_keys[..cur_ht_cap].fill(EMPTY_KEY);
                    let mut ht_mask = cur_ht_cap - 1;
                    person_keys.clear();
                    ev_cnt.clear();
                    ev_pids.clear();
                    ev_pids.reserve(total_ev);
                    bucket_evs.clear();
                    bucket_evs.reserve(total_ev);

                    let pids_ptr = ev_pids.as_mut_ptr();
                    let bevs_ptr = bucket_evs.as_mut_ptr();
                    let mut b_idx = 0usize;

                    for &(rtid, sidx, start, len) in pb_ev_runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_ev_slabs, rtid, sidx, start, len)
                        };
                        for &ev in chunk {
                            let pk = ev.person;
                            let mut pos = ((mix64(pk) >> 16) as usize) & ht_mask;
                            let pid = loop {
                                let k = unsafe { *ht_keys.get_unchecked(pos) };
                                if k == pk {
                                    break unsafe { *ht_pids.get_unchecked(pos) };
                                }
                                if k == EMPTY_KEY {
                                    let npid = person_keys.len() as u32;
                                    person_keys.push(pk);
                                    ev_cnt.push(0);
                                    unsafe {
                                        *ht_keys.get_unchecked_mut(pos) = pk;
                                        *ht_pids.get_unchecked_mut(pos) = npid;
                                    }
                                    if person_keys.len() * 2 >= cur_ht_cap {
                                        let new_cap = cur_ht_cap * 2;
                                        let new_mask = new_cap - 1;
                                        if new_cap > ht_keys.len() {
                                            ht_keys.resize(new_cap, EMPTY_KEY);
                                            ht_pids.resize(new_cap, 0);
                                        }
                                        ht_keys[..new_cap].fill(EMPTY_KEY);
                                        for (i, &opk) in person_keys.iter().enumerate() {
                                            let mut npos = ((mix64(opk) >> 16) as usize) & new_mask;
                                            while unsafe { *ht_keys.get_unchecked(npos) } != EMPTY_KEY {
                                                npos = (npos + 1) & new_mask;
                                            }
                                            unsafe {
                                                *ht_keys.get_unchecked_mut(npos) = opk;
                                                *ht_pids.get_unchecked_mut(npos) = i as u32;
                                            }
                                        }
                                        cur_ht_cap = new_cap;
                                        ht_mask = new_mask;
                                    }
                                    break npid;
                                }
                                pos = (pos + 1) & ht_mask;
                            };
                            unsafe {
                                *ev_cnt.get_unchecked_mut(pid as usize) += 1;
                                *pids_ptr.add(b_idx) = pid;
                                *bevs_ptr.add(b_idx) =
                                    ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
                            }
                            b_idx += 1;
                        }
                    }
                    unsafe {
                        ev_pids.set_len(b_idx);
                        bucket_evs.set_len(b_idx);
                    }

                    let num_people = person_keys.len();
                    ev_off.clear();
                    cur_ev_off.clear();
                    let mut run = 0u32;
                    for &c in &ev_cnt {
                        ev_off.push(run);
                        cur_ev_off.push(run);
                        run += c;
                    }
                    ev_off.push(run);

                    // Pass 2: Scatter compact 8-byte events from L2-hot local buffers (never re-reads slabs)
                    if grouped_evs.len() < total_ev {
                        grouped_evs.resize(total_ev, 0);
                    }
                    for k in 0..total_ev {
                        unsafe {
                            let pid = *ev_pids.get_unchecked(k) as usize;
                            let dst = *cur_ev_off.get_unchecked(pid) as usize;
                            *cur_ev_off.get_unchecked_mut(pid) = (dst + 1) as u32;
                            *grouped_evs.get_unchecked_mut(dst) = *bucket_evs.get_unchecked(k);
                        }
                    }
                    // Pass 3: Match clicks against active persons in this bucket and scatter per person
                    ck_cnt.clear();
                    ck_cnt.resize(num_people, 0);
                    matched_cks.clear();
                    for &(rtid, sidx, start, len) in &ck_runs[pb] {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_ck_slabs, rtid, sidx, start, len)
                        };
                        for &ck in chunk {
                            let pk = ck.person;
                            let mut pos = ((mix64(pk) >> 16) as usize) & ht_mask;
                            loop {
                                let sk = unsafe { *ht_keys.get_unchecked(pos) };
                                if sk == pk {
                                    let pid = unsafe { *ht_pids.get_unchecked(pos) };
                                    unsafe {
                                        *ck_cnt.get_unchecked_mut(pid as usize) += 1;
                                    }
                                    matched_cks.push((
                                        pid,
                                        CompactClick {
                                            cid: ck.cid,
                                            ts_rel: ck.ts_rel,
                                            camp_idx: ck.camp_idx,
                                        },
                                    ));
                                    break;
                                }
                                if sk == EMPTY_KEY {
                                    break;
                                }
                                pos = (pos + 1) & ht_mask;
                            }
                        }
                    }

                    let total_matched_cks = matched_cks.len();
                    if total_matched_cks > 0 {
                        ck_off.clear();
                        cur_ck_off.clear();
                        let mut crun = 0u32;
                        for &c in &ck_cnt {
                            ck_off.push(crun);
                            cur_ck_off.push(crun);
                            crun += c;
                        }
                        ck_off.push(crun);
                        if grouped_cks.len() < total_matched_cks {
                            grouped_cks.resize(total_matched_cks, CompactClick::default());
                        }
                        for &(pid, ck) in &matched_cks {
                            let dst = unsafe { *cur_ck_off.get_unchecked(pid as usize) } as usize;
                            unsafe {
                                *cur_ck_off.get_unchecked_mut(pid as usize) = (dst + 1) as u32;
                                *grouped_cks.get_unchecked_mut(dst) = ck;
                            }
                        }
                    }
                    // Pass 4: Sort & sessionize each person cleanly inside L1/L2 cache
                    for pid in 0..num_people {
                        let es = unsafe { *ev_off.get_unchecked(pid) } as usize;
                        let ee = unsafe { *ev_off.get_unchecked(pid + 1) } as usize;
                        let p_evs = &mut grouped_evs[es..ee];
                        if p_evs.len() > 256 {
                            radix_sort_events_u64(p_evs, &mut radix_tmp);
                        } else if p_evs.len() > 1 {
                            p_evs.sort_unstable();
                        }

                        let p_cks: &[CompactClick] = if total_matched_cks > 0 {
                            let cs = unsafe { *ck_off.get_unchecked(pid) } as usize;
                            let ce = unsafe { *ck_off.get_unchecked(pid + 1) } as usize;
                            let slice = &mut grouped_cks[cs..ce];
                            if slice.len() > 1 {
                                slice.sort_unstable_by(|a, b| cmp_compact_click(a, b, overflow));
                            }
                            slice
                        } else {
                            &[]
                        };

                        let p_key = person_keys[pid];
                        let pname_raw = overflow.decode_with(p_key, &mut buf9, &mut tmp_vec);
                        let pname_cow;
                        let pname_bytes: &[u8] = if (p_key & OVERFLOW_BIT) == 0 {
                            pname_raw
                        } else {
                            pname_cow = String::from_utf8_lossy(pname_raw);
                            pname_cow.as_bytes()
                        };
                        let mut uid_pref_stack = [0u8; 16];
                        let uid_short_len = if pname_bytes.len() <= 12 {
                            uid_pref_stack[..4]
                                .copy_from_slice(&(pname_bytes.len() as u32).to_le_bytes());
                            uid_pref_stack[4..4 + pname_bytes.len()].copy_from_slice(pname_bytes);
                            4 + pname_bytes.len()
                        } else {
                            uid_pref_vec.clear();
                            uid_pref_vec
                                .extend_from_slice(&(pname_bytes.len() as u32).to_le_bytes());
                            uid_pref_vec.extend_from_slice(pname_bytes);
                            0
                        };

                        let mut ck_left = 0usize;
                        let mut ck_right = 0usize;
                        let mut cur_day = i32::MIN;
                        let mut cur_day_end = 0u64;
                        let mut day_sess_seq = 0u32;
                        let mut d_sessions = 0i32;
                        let mut d_events = 0i32;
                        let mut d_rev = 0i64;
                        cat_dirty.clear();

                        let mut i = 0usize;
                        while i < p_evs.len() {
                            let ev0 = unsafe { *p_evs.get_unchecked(i) };
                            let sess_start_rel = ev0 >> 32;
                            if sess_start_rel >= cur_day_end {
                                if cur_day != i32::MIN {
                                    let mut widx = 0usize;
                                    for ridx in 0..active_camps.len() {
                                        let c = active_camps[ridx] as usize;
                                        if camp_in_win[c] > 0 {
                                            let delta = day_sess_seq - camp_active_since[c];
                                            if delta > 0 {
                                                camp_store.add_assisted(c as u32, cur_day, delta as i32);
                                            }
                                            camp_active_since[c] = 0;
                                            active_camps[widx] = c as u32;
                                            widx += 1;
                                        } else {
                                            in_active[c] = 0;
                                        }
                                    }
                                    active_camps.truncate(widx);
                                    day_sess_seq = 0;

                                    let top_cat = if cat_dirty.is_empty() {
                                        None
                                    } else {
                                        let mut best_cid = usize::MAX;
                                        let mut best_rev = -1i64;
                                        for &cid in &cat_dirty {
                                            let r = cat_rev[cid];
                                            cat_rev[cid] = 0;
                                            if r > best_rev || (r == best_rev && cid < best_cid) {
                                                best_rev = r;
                                                best_cid = cid;
                                            }
                                        }
                                        cat_dirty.clear();
                                        Some(best_cid)
                                    };
                                    if uid_short_len > 0 {
                                        pq_writer.push_prefixed_uid16(
                                            &uid_pref_stack,
                                            uid_short_len,
                                            cur_day,
                                            d_sessions,
                                            d_events,
                                            d_rev,
                                            top_cat,
                                        );
                                    } else {
                                        pq_writer.push_prefixed_uid(
                                            &uid_pref_vec,
                                            cur_day,
                                            d_sessions,
                                            d_events,
                                            d_rev,
                                            top_cat,
                                        );
                                    }
                                }
                                let day_idx = (sess_start_rel + start_in_day) / (DAY_MS as u64);
                                cur_day = min_day + (day_idx as i32);
                                cur_day_end = (day_idx + 1) * (DAY_MS as u64) - start_in_day;
                                d_sessions = 0;
                                d_events = 0;
                                d_rev = 0;
                            }
                            let sess_day = cur_day;

                            let mut sess_events = 1i32;
                            let mut sess_rev = 0i64;
                            let pc0 = ev0 as u32;
                            if pc0 != 0 {
                                let p0 = (pc0 & 0x00FF_FFFF) as i64;
                                let c0 = (pc0 >> 24) as usize;
                                sess_rev += p0;
                                if cat_rev[c0] == 0 {
                                    cat_dirty.push(c0);
                                }
                                cat_rev[c0] += p0;
                            }

                            let mut prev_ts_rel = sess_start_rel;
                            let mut j = i + 1;
                            while j < p_evs.len() {
                                let evj = unsafe { *p_evs.get_unchecked(j) };
                                let next_ts_rel = evj >> 32;
                                if next_ts_rel - prev_ts_rel > (SESSION_GAP_MS as u64) {
                                    break;
                                }
                                prev_ts_rel = next_ts_rel;
                                sess_events += 1;
                                let pcj = evj as u32;
                                if pcj != 0 {
                                    let pj = (pcj & 0x00FF_FFFF) as i64;
                                    let cj = (pcj >> 24) as usize;
                                    sess_rev += pj;
                                    if cat_rev[cj] == 0 {
                                        cat_dirty.push(cj);
                                    }
                                    cat_rev[cj] += pj;
                                }
                                j += 1;
                            }
                            i = j;

                            d_sessions += 1;
                            d_events += sess_events;
                            d_rev += sess_rev;

                            if !p_cks.is_empty() {
                                let max_ck_rel = sess_start_rel + (CLICK_REACH_MS as u64);
                                while ck_right < p_cks.len()
                                    && (unsafe { p_cks.get_unchecked(ck_right) }.ts_rel as u64)
                                        <= max_ck_rel
                                {
                                    let c =
                                        unsafe { p_cks.get_unchecked(ck_right) }.camp_idx as usize;
                                    if camp_in_win[c] == 0 {
                                        camp_active_since[c] = day_sess_seq;
                                        if in_active[c] == 0 {
                                            in_active[c] = 1;
                                            active_camps.push(c as u32);
                                        }
                                    }
                                    camp_in_win[c] += 1;
                                    ck_right += 1;
                                }
                                while ck_left < ck_right
                                    && (unsafe { p_cks.get_unchecked(ck_left) }.ts_rel as u64)
                                        < sess_start_rel
                                {
                                    let c =
                                        unsafe { p_cks.get_unchecked(ck_left) }.camp_idx as usize;
                                    camp_in_win[c] -= 1;
                                    if camp_in_win[c] == 0 {
                                        let delta = day_sess_seq - camp_active_since[c];
                                        if delta > 0 {
                                            camp_store.add_assisted(c as u32, sess_day, delta as i32);
                                        }
                                        camp_active_since[c] = u32::MAX;
                                    }
                                    ck_left += 1;
                                }
                            }

                            if ck_left == ck_right {
                                camp_store.add_credited(campaigns.organic_id, sess_day, sess_rev);
                            } else {
                                let win_camp =
                                    unsafe { p_cks.get_unchecked(ck_right - 1) }.camp_idx;
                                camp_store.add_credited(win_camp, sess_day, sess_rev);
                            }
                            day_sess_seq += 1;
                        }

                        if cur_day != i32::MIN {
                            for &c_u32 in &active_camps {
                                let c = c_u32 as usize;
                                if camp_in_win[c] > 0 {
                                    let delta = day_sess_seq - camp_active_since[c];
                                    if delta > 0 {
                                        camp_store.add_assisted(c_u32, cur_day, delta as i32);
                                    }
                                    camp_in_win[c] = 0;
                                    camp_active_since[c] = u32::MAX;
                                }
                                in_active[c] = 0;
                            }
                            active_camps.clear();

                            let top_cat = if cat_dirty.is_empty() {
                                None
                            } else {
                                let mut best_cid = usize::MAX;
                                let mut best_rev = -1i64;
                                for &cid in &cat_dirty {
                                    let r = cat_rev[cid];
                                    cat_rev[cid] = 0;
                                    if r > best_rev || (r == best_rev && cid < best_cid) {
                                        best_rev = r;
                                        best_cid = cid;
                                    }
                                }
                                cat_dirty.clear();
                                Some(best_cid)
                            };
                            if uid_short_len > 0 {
                                pq_writer.push_prefixed_uid16(
                                    &uid_pref_stack,
                                    uid_short_len,
                                    cur_day,
                                    d_sessions,
                                    d_events,
                                    d_rev,
                                    top_cat,
                                );
                            } else {
                                pq_writer.push_prefixed_uid(
                                    &uid_pref_vec,
                                    cur_day,
                                    d_sessions,
                                    d_events,
                                    d_rev,
                                    top_cat,
                                );
                            }
                        } else {
                            for &c in &active_camps {
                                camp_in_win[c as usize] = 0;
                                camp_active_since[c as usize] = u32::MAX;
                                in_active[c as usize] = 0;
                            }
                            active_camps.clear();
                        }
                    }
                }

                let wrote = pq_writer.close();
                (camp_store, wrote)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut wrote_any = false;
    let mut stores = Vec::with_capacity(results.len());
    for (st, w) in results {
        wrote_any |= w;
        stores.push(st);
    }
    (stores, wrote_any)
}

// =========================================================================
// Stage 4b: General Wide Path (> 25d window or > 256 categories)
// =========================================================================

fn process_person_buckets_wide(
    num_threads: usize,
    all_ev_slabs: &[Vec<HugeSlab<SurvEventWide>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<HugeSlab<SurvClickWide>>],
    ck_runs: &[Vec<(u8, u16, u32, u16)>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;

    let mut total_surv_ev = 0usize;
    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
            if ev_n == 0 {
                None
            } else {
                total_surv_ev += ev_n;
                let ck_n: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum();
                Some((ev_n + ck_n, pb))
            }
        })
        .collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let work_order: Vec<usize> = order.into_iter().map(|x| x.1).collect();

    if work_order.is_empty() {
        return (Vec::new(), false);
    }

    let est_rows_per_thread = ((total_surv_ev / 8 / nt).max(4_096)).min(393_216);
    let next_idx = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store = CampStore::new(use_flat, num_camps, min_day, day_span);
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer =
                    UserDailyParquetWriter::new(part_path, &products.categories, est_rows_per_thread);

                let mut evs: Vec<SurvEventWide> = Vec::new();
                let mut cks: Vec<SurvClickWide> = Vec::new();
                let mut camp_in_win: Vec<u32> = vec![0u32; num_camps];
                let mut active_camps: Vec<u32> = Vec::with_capacity(32);
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut cat_dirty: Vec<usize> = Vec::with_capacity(16);

                let mut buf9 = [0u8; 9];
                let mut tmp_vec: Vec<u8> = Vec::with_capacity(64);

                loop {
                    let wi = next_idx_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&pb) = work_order_ref.get(wi) else {
                        break;
                    };
                    let total_ev: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
                    if total_ev == 0 {
                        continue;
                    }
                    evs.clear();
                    evs.reserve(total_ev);
                    for &(rtid, sidx, start, len) in &ev_runs[pb] {
                        let c = unsafe {
                            SlabBucketsOut::get_slice(all_ev_slabs, rtid, sidx, start, len)
                        };
                        evs.extend_from_slice(c);
                    }
                    let total_ck: usize = ck_runs[pb].iter().map(|r| r.3 as usize).sum();
                    cks.clear();
                    if total_ck > 0 {
                        cks.reserve(total_ck);
                        for &(rtid, sidx, start, len) in &ck_runs[pb] {
                            let c = unsafe {
                                SlabBucketsOut::get_slice(all_ck_slabs, rtid, sidx, start, len)
                            };
                            cks.extend_from_slice(c);
                        }
                    }

                    evs.sort_unstable_by(|a, b| a.person.cmp(&b.person).then_with(|| a.ts.cmp(&b.ts)));
                    if cks.len() > 1 {
                        cks.sort_unstable_by(|a, b| {
                            a.person
                                .cmp(&b.person)
                                .then_with(|| a.ts.cmp(&b.ts))
                                .then_with(|| overflow.cmp_key(b.cid, a.cid))
                        });
                    }

                    let mut ei = 0usize;
                    let mut ci = 0usize;
                    while ei < evs.len() {
                        let p_key = evs[ei].person;
                        let ev_s = ei;
                        ei += 1;
                        while ei < evs.len() && evs[ei].person == p_key {
                            ei += 1;
                        }
                        let p_evs = &evs[ev_s..ei];

                        while ci < cks.len() && cks[ci].person < p_key {
                            ci += 1;
                        }
                        let ck_s = ci;
                        while ci < cks.len() && cks[ci].person == p_key {
                            ci += 1;
                        }
                        let p_cks = &cks[ck_s..ci];

                        let pname_raw = overflow.decode_with(p_key, &mut buf9, &mut tmp_vec);
                        let pname_cow;
                        let pname_bytes: &[u8] = if (p_key & OVERFLOW_BIT) == 0 {
                            pname_raw
                        } else {
                            pname_cow = String::from_utf8_lossy(pname_raw);
                            pname_cow.as_bytes()
                        };

                        let mut ck_left = 0usize;
                        let mut ck_right = 0usize;
                        let mut cur_day = i32::MIN;
                        let mut d_sessions = 0i32;
                        let mut d_events = 0i32;
                        let mut d_rev = 0i64;
                        cat_dirty.clear();

                        let mut i = 0usize;
                        while i < p_evs.len() {
                            let sess_start = p_evs[i].ts;
                            let sess_day = sess_start.div_euclid(DAY_MS) as i32;

                            if sess_day != cur_day {
                                if cur_day != i32::MIN {
                                    let top_cat = if cat_dirty.is_empty() {
                                        None
                                    } else {
                                        let mut best_cid = usize::MAX;
                                        let mut best_rev = -1i64;
                                        for &cid in &cat_dirty {
                                            let r = cat_rev[cid];
                                            cat_rev[cid] = 0;
                                            if r > best_rev || (r == best_rev && cid < best_cid) {
                                                best_rev = r;
                                                best_cid = cid;
                                            }
                                        }
                                        cat_dirty.clear();
                                        Some(best_cid)
                                    };
                                    pq_writer.push(
                                        pname_bytes,
                                        cur_day,
                                        d_sessions,
                                        d_events,
                                        d_rev,
                                        top_cat,
                                    );
                                }
                                cur_day = sess_day;
                                d_sessions = 0;
                                d_events = 0;
                                d_rev = 0;
                            }

                            let mut sess_events = 1i32;
                            let mut sess_rev = 0i64;
                            let p0 = p_evs[i].price_cents as i64;
                            let c0 = p_evs[i].cat_id as usize;
                            if p0 > 0 {
                                sess_rev += p0;
                                if cat_rev[c0] == 0 {
                                    cat_dirty.push(c0);
                                }
                                cat_rev[c0] += p0;
                            }

                            let mut prev_ts = sess_start;
                            let mut j = i + 1;
                            while j < p_evs.len() {
                                let next_ts = p_evs[j].ts;
                                if next_ts - prev_ts > SESSION_GAP_MS {
                                    break;
                                }
                                prev_ts = next_ts;
                                sess_events += 1;
                                let pj = p_evs[j].price_cents as i64;
                                let cj = p_evs[j].cat_id as usize;
                                if pj > 0 {
                                    sess_rev += pj;
                                    if cat_rev[cj] == 0 {
                                        cat_dirty.push(cj);
                                    }
                                    cat_rev[cj] += pj;
                                }
                                j += 1;
                            }
                            i = j;

                            d_sessions += 1;
                            d_events += sess_events;
                            d_rev += sess_rev;

                            if !p_cks.is_empty() {
                                while ck_right < p_cks.len() && p_cks[ck_right].ts <= sess_start {
                                    let c = p_cks[ck_right].camp_idx;
                                    if camp_in_win[c as usize] == 0 {
                                        active_camps.push(c);
                                    }
                                    camp_in_win[c as usize] += 1;
                                    ck_right += 1;
                                }
                                let min_ck_ts = sess_start - CLICK_REACH_MS;
                                while ck_left < ck_right && p_cks[ck_left].ts < min_ck_ts {
                                    let c = p_cks[ck_left].camp_idx;
                                    camp_in_win[c as usize] -= 1;
                                    ck_left += 1;
                                }
                            }

                            if ck_left == ck_right {
                                active_camps.clear();
                                camp_store.add_credited(campaigns.organic_id, sess_day, sess_rev);
                            } else {
                                let win_camp = p_cks[ck_right - 1].camp_idx;
                                camp_store.add_credited(win_camp, sess_day, sess_rev);

                                let mut widx = 0usize;
                                for ridx in 0..active_camps.len() {
                                    let c = active_camps[ridx];
                                    if camp_in_win[c as usize] > 0 {
                                        active_camps[widx] = c;
                                        widx += 1;
                                        camp_store.add_assisted(c, sess_day, 1);
                                    }
                                }
                                active_camps.truncate(widx);
                            }
                        }

                        if cur_day != i32::MIN {
                            let top_cat = if cat_dirty.is_empty() {
                                None
                            } else {
                                let mut best_cid = usize::MAX;
                                let mut best_rev = -1i64;
                                for &cid in &cat_dirty {
                                    let r = cat_rev[cid];
                                    cat_rev[cid] = 0;
                                    if r > best_rev || (r == best_rev && cid < best_cid) {
                                        best_rev = r;
                                        best_cid = cid;
                                    }
                                }
                                cat_dirty.clear();
                                Some(best_cid)
                            };
                            pq_writer.push(pname_bytes, cur_day, d_sessions, d_events, d_rev, top_cat);
                        }

                        for &c in &active_camps {
                            camp_in_win[c as usize] = 0;
                        }
                        active_camps.clear();
                    }
                }

                let wrote = pq_writer.close();
                (camp_store, wrote)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut wrote_any = false;
    let mut stores = Vec::with_capacity(results.len());
    for (st, w) in results {
        wrote_any |= w;
        stores.push(st);
    }
    (stores, wrote_any)
}

// =========================================================================
// Main Pipeline Orchestration
// =========================================================================

struct ChunkJob {
    fidx: usize,
    start: usize,
    end: usize,
}

#[derive(Clone, Copy, Default)]
struct PendingClick {
    uid_k: u64,
    cid_k: u64,
    ts_rel: i64,
    camp_idx: u32,
}

struct ReaderThreadOut {
    eid_out: SlabBucketsOut<RawEvent>,
    extras: Vec<ExtraEvent>,
    ck_compact_out: Option<SlabBucketsOut<SurvClick>>,
    ck_wide_out: Option<SlabBucketsOut<SurvClickWide>>,
}

#[cold]
#[inline(never)]
fn push_extra_event(
    extras: &mut Vec<ExtraEvent>,
    tid: usize,
    idx_bits: u32,
    ts: i64,
    ingest_ts: i64,
    price_cents: u32,
    cat_id: u32,
    lo: u64,
    top_tag: u64,
) -> u64 {
    let eidx = extras.len();
    extras.push(ExtraEvent {
        ts,
        ingest_ts,
        price_cents,
        cat_id,
    });
    let eref = ((tid as u64) << idx_bits) | (eidx as u64);
    top_tag | (eref << 32) | lo
}

const NT_BLOCK: usize = 512;
const WIDE_BLOCK: usize = 256;

#[inline(always)]
fn flush_click_batch(
    batch: &[PendingClick],
    link_redirect: &LinkRedirect,
    use_compact_ts: bool,
    ck_compact_staged: &mut Option<NtSlabBuckets<SurvClick, 8, NT_BLOCK>>,
    ck_wide_staged: &mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    win_start: i64,
) {
    let mut slot_idxs = [usize::MAX; 32];
    let mut hashes = [0u64; 32];
    for (j, pk) in batch.iter().enumerate() {
        let (h, opt_slot) = link_redirect.bloom_hit_slot_hash(pk.uid_k);
        hashes[j] = h;
        if let Some(sidx) = opt_slot {
            link_redirect.prefetch_slot(sidx);
            slot_idxs[j] = sidx;
        }
    }
    if use_compact_ts {
        let out = ck_compact_staged.get_or_insert_with(|| {
            NtSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
        });
        for (j, pk) in batch.iter().enumerate() {
            let sidx = slot_idxs[j];
            let (person, pb) = if sidx == usize::MAX {
                (pk.uid_k, (hashes[j] as usize) & PERSON_MASK)
            } else {
                let p = link_redirect.resolve_from_slot(pk.uid_k, sidx);
                let hh = if p == pk.uid_k { hashes[j] } else { mix64(p) };
                (p, (hh as usize) & PERSON_MASK)
            };
            out.push(
                pb,
                SurvClick {
                    person,
                    cid: pk.cid_k,
                    ts_rel: (pk.ts_rel + CLICK_REACH_MS) as u32,
                    camp_idx: pk.camp_idx,
                },
            );
        }
    } else {
        let out = ck_wide_staged.get_or_insert_with(|| {
            DirectSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
        });
        for (j, pk) in batch.iter().enumerate() {
            let sidx = slot_idxs[j];
            let (person, pb) = if sidx == usize::MAX {
                (pk.uid_k, (hashes[j] as usize) & PERSON_MASK)
            } else {
                let p = link_redirect.resolve_from_slot(pk.uid_k, sidx);
                let hh = if p == pk.uid_k { hashes[j] } else { mix64(p) };
                (p, (hh as usize) & PERSON_MASK)
            };
            out.push(
                pb,
                SurvClickWide {
                    person,
                    cid: pk.cid_k,
                    ts: win_start + pk.ts_rel,
                    camp_idx: pk.camp_idx,
                },
            );
        }
    }
}

#[inline(always)]
fn push_surv_event(
    rec: RawEvent,
    person: u64,
    pb: usize,
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    ev_compact: &mut Option<NtSlabBuckets<SurvEvent, 4, NT_BLOCK>>,
    ev_wide: &mut Option<DirectSlabBuckets<SurvEventWide, WIDE_BLOCK>>,
) {
    let kind = rec.packed >> 62;
    if let Some(ref mut out) = ev_compact {
        let (ts_rel, price_cat) = if kind < 2 {
            (((rec.packed >> 32) & 0x7FFF_FFFF) as u32, 0u32)
        } else {
            let eref = (rec.packed >> 32) & ((1u64 << 30) - 1);
            let tid = (eref >> idx_bits) as usize;
            let eidx = (eref & idx_mask) as usize;
            let ex = unsafe { *all_extras.get_unchecked(tid).get_unchecked(eidx) };
            let pc = if ex.price_cents == 0 {
                0
            } else {
                (ex.cat_id << 24) | ex.price_cents
            };
            ((ex.ts - win_start) as u32, pc)
        };
        out.push(
            pb,
            SurvEvent {
                person,
                ts_rel,
                price_cat,
            },
        );
    } else if let Some(ref mut out) = ev_wide {
        let (ts, price_cents, cat_id) = if kind < 2 {
            let rel = ((rec.packed >> 32) & 0x7FFF_FFFF) as i64;
            (win_start + rel, 0u32, 0u32)
        } else {
            let eref = (rec.packed >> 32) & ((1u64 << 30) - 1);
            let tid = (eref >> idx_bits) as usize;
            let eidx = (eref & idx_mask) as usize;
            let ex = unsafe { *all_extras.get_unchecked(tid).get_unchecked(eidx) };
            (ex.ts, ex.price_cents, ex.cat_id)
        };
        out.push(
            pb,
            SurvEventWide {
                person,
                ts,
                price_cents,
                cat_id,
            },
        );
    }
}

#[inline(never)]
fn handle_event_row_rest<const COMPACT: bool>(
    eid_k: u64,
    uid: &[u8],
    ts_b: &[u8],
    ing_val: i64,
    ing_is_rel_sec: bool,
    hint: &IngestHint,
    etype: &[u8],
    pid: &[u8],
    price_b: &[u8],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    eid_mask: usize,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent, 8, NT_BLOCK>,
) {
    let ingest_rel_ms = if ing_is_rel_sec {
        ing_val * 1_000
    } else {
        ing_val - win_start
    };

    let mut valid = false;
    let mut valid_ts_rel = 0i64;
    let mut price_cents = 0u32;
    let mut cat_id = 0u32;

    if !uid.is_empty() && !pid.is_empty() {
        if etype.len() == 4 {
            let tag = unsafe { (etype.as_ptr() as *const u32).read_unaligned() };
            if (tag == VIEW_U32) | (tag == CART_U32) {
                if let Some(ts_rel) = parse_ts_rel_ms_with_hint(ts_b, date_cache, hint) {
                    if (ts_rel as u64) < win_len_u64 && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS {
                        valid = true;
                        valid_ts_rel = ts_rel;
                    }
                }
            }
        } else if etype.len() == 8
            && unsafe { (etype.as_ptr() as *const u64).read_unaligned() } == PURCH_U64
        {
            if let Some(p) = parse_price_cents(price_b) {
                if let Some(ts_rel) = parse_ts_rel_ms_with_hint(ts_b, date_cache, hint) {
                    if (ts_rel as u64) < win_len_u64 && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS {
                        valid = true;
                        valid_ts_rel = ts_rel;
                        price_cents = p;
                        cat_id = products.lookup(pid);
                    }
                }
            }
        }
    }

    let uid_k = if valid { overflow.encode(uid) } else { 0 };
    let in_sec = if ing_is_rel_sec {
        if ing_val > (i32::MIN as i64) && ing_val <= (i32::MAX as i64) {
            Some(ing_val as i32)
        } else {
            None
        }
    } else {
        inline_ingest_sec(ing_val, win_start)
    };

    let packed = if !valid {
        if let Some(sec) = in_sec {
            (2u64 << 62) | ((sec as u32) as u64)
        } else {
            let ingest_abs = if ing_is_rel_sec {
                win_start + ing_val * 1_000
            } else {
                ing_val
            };
            push_extra_event(
                extras,
                tid,
                idx_bits,
                0,
                ingest_abs,
                0,
                0,
                (i32::MIN as u32) as u64,
                2u64 << 62,
            )
        }
    } else if COMPACT && price_cents == 0 && in_sec.is_some() {
        let ts_rel = valid_ts_rel as u64;
        let sec = in_sec.unwrap();
        (ts_rel << 32) | ((sec as u32) as u64)
    } else {
        let ingest_abs = if ing_is_rel_sec {
            win_start + ing_val * 1_000
        } else {
            ing_val
        };
        let lo = match in_sec {
            Some(sec) => (sec as u32) as u64,
            None => (i32::MIN as u32) as u64,
        };
        push_extra_event(
            extras,
            tid,
            idx_bits,
            win_start + valid_ts_rel,
            ingest_abs,
            price_cents,
            cat_id,
            lo,
            3u64 << 62,
        )
    };

    let eb = (mix64(eid_k) as usize) & eid_mask;
    eid_staged.push(
        eb,
        RawEvent {
            eid: eid_k,
            uid: uid_k,
            packed,
        },
    );
}

#[cold]
#[inline(never)]
fn handle_event_row_cold<const COMPACT: bool>(
    eid: &[u8],
    uid: &[u8],
    ts_b: &[u8],
    ingest_b: &[u8],
    etype: &[u8],
    pid: &[u8],
    price_b: &[u8],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    eid_mask: usize,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent, 8, NT_BLOCK>,
) {
    let Some((ing_val, ing_is_rel_sec, hint)) = parse_ingest_with_hint(ingest_b, date_cache) else {
        return;
    };
    let eid_k = overflow.encode(eid);
    handle_event_row_rest::<COMPACT>(
        eid_k,
        uid,
        ts_b,
        ing_val,
        ing_is_rel_sec,
        &hint,
        etype,
        pid,
        price_b,
        date_cache,
        win_start,
        win_len_u64,
        products,
        overflow,
        tid,
        idx_bits,
        eid_mask,
        extras,
        eid_staged,
    );
}

#[inline(always)]
fn handle_event_row<const COMPACT: bool>(
    &[eid, uid, ts_b, ingest_b, etype, pid, price_b]: &[&[u8]; 7],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    eid_mask: usize,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent, 8, NT_BLOCK>,
) {
    if eid.is_empty() {
        return;
    }
    if let Some((ing_sec, hint)) = parse_ingest_fast_sec(ingest_b, date_cache) {
        let eid_k = overflow.encode(eid);
        if COMPACT {
            if !uid.is_empty() && !pid.is_empty() {
                if etype.len() == 4 {
                    let tag = unsafe { (etype.as_ptr() as *const u32).read_unaligned() };
                    if (tag == VIEW_U32) | (tag == CART_U32) {
                        if let Some(ts_rel) = parse_ts_rel_ms_with_hint(ts_b, date_cache, &hint) {
                            let ingest_rel_ms = (ing_sec as i64) * 1_000;
                            if (ts_rel as u64) < win_len_u64
                                && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                            {
                                let uid_k = overflow.encode(uid);
                                let packed =
                                    ((ts_rel as u64) << 32) | ((ing_sec as u32) as u64);
                                let eb = (mix64(eid_k) as usize) & eid_mask;
                                eid_staged.push(
                                    eb,
                                    RawEvent {
                                        eid: eid_k,
                                        uid: uid_k,
                                        packed,
                                    },
                                );
                                return;
                            }
                        }
                    }
                } else if etype.len() == 8
                    && unsafe { (etype.as_ptr() as *const u64).read_unaligned() } == PURCH_U64
                {
                    handle_event_row_rest::<true>(
                        eid_k, uid, ts_b, ing_sec as i64, true, &hint, etype, pid, price_b,
                        date_cache, win_start, win_len_u64, products, overflow, tid, idx_bits,
                        eid_mask, extras, eid_staged,
                    );
                    return;
                }
            }
            let packed = (2u64 << 62) | ((ing_sec as u32) as u64);
            let eb = (mix64(eid_k) as usize) & eid_mask;
            eid_staged.push(
                eb,
                RawEvent {
                    eid: eid_k,
                    uid: 0,
                    packed,
                },
            );
            return;
        }
        handle_event_row_rest::<false>(
            eid_k, uid, ts_b, ing_sec as i64, true, &hint, etype, pid, price_b, date_cache,
            win_start, win_len_u64, products, overflow, tid, idx_bits, eid_mask, extras,
            eid_staged,
        );
        return;
    }
    handle_event_row_cold::<COMPACT>(
        eid, uid, ts_b, ingest_b, etype, pid, price_b, date_cache, win_start, win_len_u64,
        products, overflow, tid, idx_bits, eid_mask, extras, eid_staged,
    );
}

#[inline(always)]
fn handle_click_row(
    &[cid, uid, camp_b, ts_b]: &[&[u8]; 4],
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    win_start: i64,
    overflow: &OverflowInterner,
    links_empty: bool,
    use_compact_ts: bool,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    link_redirect: &LinkRedirect,
    ck_buf: &mut [PendingClick; 32],
    ck_len: &mut usize,
    ck_compact_staged: &mut Option<NtSlabBuckets<SurvClick, 8, NT_BLOCK>>,
    ck_wide_staged: &mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
) {
    if cid.is_empty() || uid.is_empty() {
        return;
    }
    let Some(camp_idx) = campaigns.lookup(camp_b) else {
        return;
    };
    let Some(ts_rel) = parse_ts_rel_ms(ts_b, date_cache) else {
        return;
    };
    if ts_rel < -CLICK_REACH_MS || ts_rel >= win_len_i64 {
        return;
    }
    let uid_k = overflow.encode(uid);
    let cid_k = overflow.encode(cid);
    if links_empty {
        let pb = (mix64(uid_k) as usize) & PERSON_MASK;
        if use_compact_ts {
            let out = ck_compact_staged.get_or_insert_with(|| {
                NtSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
            });
            out.push(
                pb,
                SurvClick {
                    person: uid_k,
                    cid: cid_k,
                    ts_rel: (ts_rel + CLICK_REACH_MS) as u32,
                    camp_idx,
                },
            );
        } else {
            let out = ck_wide_staged.get_or_insert_with(|| {
                DirectSlabBuckets::new(PERSON_BUCKETS, ck_init_elems, ck_extra_elems)
            });
            out.push(
                pb,
                SurvClickWide {
                    person: uid_k,
                    cid: cid_k,
                    ts: win_start + ts_rel,
                    camp_idx,
                },
            );
        }
    } else {
        ck_buf[*ck_len] = PendingClick {
            uid_k,
            cid_k,
            ts_rel,
            camp_idx,
        };
        *ck_len += 1;
        if *ck_len == 32 {
            flush_click_batch(
                ck_buf,
                link_redirect,
                use_compact_ts,
                ck_compact_staged,
                ck_wide_staged,
                ck_init_elems,
                ck_extra_elems,
                win_start,
            );
            *ck_len = 0;
        }
    }
}

struct OpenedInputs {
    event_mmaps: Vec<Mmap>,
    click_mmaps: Vec<Mmap>,
    event_jobs: Vec<ChunkJob>,
    click_jobs: Vec<ChunkJob>,
    total_event_bytes: u64,
}

fn build_interleaved_mmap_jobs(paths: &[PathBuf]) -> (Vec<Mmap>, Vec<ChunkJob>, u64) {
    let mut pairs: Vec<(usize, Mmap)> = Vec::with_capacity(paths.len());
    let mut total_bytes = 0u64;
    for p in paths {
        let Ok(f) = File::open(p) else {
            continue;
        };
        let Ok(meta) = f.metadata() else {
            continue;
        };
        let len = meta.len() as usize;
        if len == 0 {
            continue;
        }
        let Ok(m) = (unsafe { MmapOptions::new().map(&f) }) else {
            continue;
        };
        total_bytes += len as u64;
        pairs.push((len, m));
    }
    pairs.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let mmaps: Vec<Mmap> = pairs.into_iter().map(|x| x.1).collect();

    const CHUNK_BYTES: usize = 2 * 1024 * 1024;
    let mut jobs: Vec<ChunkJob> = Vec::new();
    let mut offs = vec![0usize; mmaps.len()];
    loop {
        let mut any = false;
        for (fidx, m) in mmaps.iter().enumerate() {
            if offs[fidx] < m.len() {
                let start = offs[fidx];
                let end = (start + CHUNK_BYTES).min(m.len());
                jobs.push(ChunkJob { fidx, start, end });
                offs[fidx] = end;
                any = true;
            }
        }
        if !any {
            break;
        }
    }
    (mmaps, jobs, total_bytes)
}

fn open_inputs_and_jobs(event_files: &[PathBuf], click_files: &[PathBuf]) -> OpenedInputs {
    let (event_mmaps, event_jobs, total_event_bytes) = build_interleaved_mmap_jobs(event_files);
    let (click_mmaps, click_jobs, _) = build_interleaved_mmap_jobs(click_files);
    OpenedInputs {
        event_mmaps,
        click_mmaps,
        event_jobs,
        click_jobs,
        total_event_bytes,
    }
}

#[inline(always)]
fn drain_event_jobs(
    next_ev_job: &AtomicUsize,
    event_jobs: &[ChunkJob],
    event_mmaps: &[Mmap],
    date_cache: &FastDateCache,
    win_start: i64,
    win_len_u64: u64,
    use_compact_ts: bool,
    products: &ProductIndex,
    overflow: &OverflowInterner,
    tid: usize,
    idx_bits: u32,
    eid_mask: usize,
    extras: &mut Vec<ExtraEvent>,
    eid_staged: &mut NtSlabBuckets<RawEvent, 8, NT_BLOCK>,
) {
    loop {
        let jid = next_ev_job.fetch_add(1, MemOrdering::Relaxed);
        let Some(job) = event_jobs.get(jid) else {
            break;
        };
        let mmap = &event_mmaps[job.fidx];
        if use_compact_ts {
            struct EvSink<'a> {
                date_cache: &'a FastDateCache,
                win_start: i64,
                win_len_u64: u64,
                products: &'a ProductIndex,
                overflow: &'a OverflowInterner,
                tid: usize,
                idx_bits: u32,
                eid_mask: usize,
                extras: &'a mut Vec<ExtraEvent>,
                eid_staged: &'a mut NtSlabBuckets<RawEvent, 8, NT_BLOCK>,
            }
            impl CsvRowSink<7> for EvSink<'_> {
                #[inline(always)]
                unsafe fn on_pos(&mut self, ptr: *const u8, s0: usize, rpos: *const u16) {
                    let e0 = *rpos as usize;
                    let q0 = usize::from(*ptr.add(s0) == b'"');
                    let eid_len = (e0 - s0) - (q0 << 1);
                    if eid_len == 0 {
                        return;
                    }
                    let eid = std::slice::from_raw_parts(ptr.add(s0 + q0), eid_len);

                    let e1 = *rpos.add(1) as usize;
                    let e2 = *rpos.add(2) as usize;
                    let e3 = *rpos.add(3) as usize;
                    let s3 = e2 + 1;
                    let q3 = usize::from(*ptr.add(s3) == b'"');
                    let ingest_b =
                        std::slice::from_raw_parts(ptr.add(s3 + q3), (e3 - s3) - (q3 << 1));

                    if let Some((ing_sec, hint)) = parse_ingest_fast_sec(ingest_b, self.date_cache)
                    {
                        let eid_k = self.overflow.encode(eid);
                        let e4 = *rpos.add(4) as usize;
                        let s4 = e3 + 1;
                        let q4 = usize::from(*ptr.add(s4) == b'"');
                        let et_len = (e4 - s4) - (q4 << 1);
                        let et_ptr = ptr.add(s4 + q4);

                        let s1 = e0 + 1;
                        let q1 = usize::from(*ptr.add(s1) == b'"');
                        let uid_len = (e1 - s1) - (q1 << 1);

                        let e5 = *rpos.add(5) as usize;
                        let s5 = e4 + 1;
                        let pid_raw = e5 - s5;
                        let pid_ok = pid_raw >= 3
                            || (pid_raw > 0 && !(pid_raw == 2 && *ptr.add(s5) == b'"'));

                        if uid_len != 0 && pid_ok {
                            if et_len == 4 {
                                let tag = (et_ptr as *const u32).read_unaligned();
                                if (tag == VIEW_U32) | (tag == CART_U32) {
                                    let s2 = e1 + 1;
                                    let q2 = usize::from(*ptr.add(s2) == b'"');
                                    let ts_b = std::slice::from_raw_parts(
                                        ptr.add(s2 + q2),
                                        (e2 - s2) - (q2 << 1),
                                    );
                                    if let Some(ts_rel) =
                                        parse_ts_rel_ms_with_hint(ts_b, self.date_cache, &hint)
                                    {
                                        let ingest_rel_ms = (ing_sec as i64) * 1_000;
                                        if (ts_rel as u64) < self.win_len_u64
                                            && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                                        {
                                            let uid = std::slice::from_raw_parts(
                                                ptr.add(s1 + q1),
                                                uid_len,
                                            );
                                            let uid_k = self.overflow.encode(uid);
                                            let packed = ((ts_rel as u64) << 32)
                                                | ((ing_sec as u32) as u64);
                                            let eb = (mix64(eid_k) as usize) & self.eid_mask;
                                            self.eid_staged.push(
                                                eb,
                                                RawEvent {
                                                    eid: eid_k,
                                                    uid: uid_k,
                                                    packed,
                                                },
                                            );
                                            return;
                                        }
                                    }
                                }
                            } else if et_len == 8
                                && (et_ptr as *const u64).read_unaligned() == PURCH_U64
                            {
                                let uid = std::slice::from_raw_parts(ptr.add(s1 + q1), uid_len);
                                let s2 = e1 + 1;
                                let q2 = usize::from(*ptr.add(s2) == b'"');
                                let ts_b = std::slice::from_raw_parts(
                                    ptr.add(s2 + q2),
                                    (e2 - s2) - (q2 << 1),
                                );
                                let q5 = usize::from(*ptr.add(s5) == b'"');
                                let pid = std::slice::from_raw_parts(
                                    ptr.add(s5 + q5),
                                    pid_raw - (q5 << 1),
                                );
                                let e6 = *rpos.add(6) as usize;
                                let s6 = e5 + 1;
                                let q6 = usize::from(*ptr.add(s6) == b'"');
                                let price_b = std::slice::from_raw_parts(
                                    ptr.add(s6 + q6),
                                    (e6 - s6) - (q6 << 1),
                                );
                                handle_event_row_rest::<true>(
                                    eid_k,
                                    uid,
                                    ts_b,
                                    ing_sec as i64,
                                    true,
                                    &hint,
                                    std::slice::from_raw_parts(et_ptr, 8),
                                    pid,
                                    price_b,
                                    self.date_cache,
                                    self.win_start,
                                    self.win_len_u64,
                                    self.products,
                                    self.overflow,
                                    self.tid,
                                    self.idx_bits,
                                    self.eid_mask,
                                    self.extras,
                                    self.eid_staged,
                                );
                                return;
                            }
                        }
                        let packed = (2u64 << 62) | ((ing_sec as u32) as u64);
                        let eb = (mix64(eid_k) as usize) & self.eid_mask;
                        self.eid_staged.push(
                            eb,
                            RawEvent {
                                eid: eid_k,
                                uid: 0,
                                packed,
                            },
                        );
                        return;
                    }

                    let s1 = e0 + 1;
                    let q1 = usize::from(*ptr.add(s1) == b'"');
                    let uid = std::slice::from_raw_parts(ptr.add(s1 + q1), (e1 - s1) - (q1 << 1));
                    let s2 = e1 + 1;
                    let q2 = usize::from(*ptr.add(s2) == b'"');
                    let ts_b = std::slice::from_raw_parts(ptr.add(s2 + q2), (e2 - s2) - (q2 << 1));
                    let e4 = *rpos.add(4) as usize;
                    let s4 = e3 + 1;
                    let q4 = usize::from(*ptr.add(s4) == b'"');
                    let etype =
                        std::slice::from_raw_parts(ptr.add(s4 + q4), (e4 - s4) - (q4 << 1));
                    let e5 = *rpos.add(5) as usize;
                    let s5 = e4 + 1;
                    let q5 = usize::from(*ptr.add(s5) == b'"');
                    let pid = std::slice::from_raw_parts(ptr.add(s5 + q5), (e5 - s5) - (q5 << 1));
                    let e6 = *rpos.add(6) as usize;
                    let s6 = e5 + 1;
                    let q6 = usize::from(*ptr.add(s6) == b'"');
                    let price_b =
                        std::slice::from_raw_parts(ptr.add(s6 + q6), (e6 - s6) - (q6 << 1));
                    handle_event_row_cold::<true>(
                        eid,
                        uid,
                        ts_b,
                        ingest_b,
                        etype,
                        pid,
                        price_b,
                        self.date_cache,
                        self.win_start,
                        self.win_len_u64,
                        self.products,
                        self.overflow,
                        self.tid,
                        self.idx_bits,
                        self.eid_mask,
                        self.extras,
                        self.eid_staged,
                    );
                }

                #[inline(always)]
                fn on_row(&mut self, row: &[&[u8]; 7]) {
                    handle_event_row::<true>(
                        row,
                        self.date_cache,
                        self.win_start,
                        self.win_len_u64,
                        self.products,
                        self.overflow,
                        self.tid,
                        self.idx_bits,
                        self.eid_mask,
                        self.extras,
                        self.eid_staged,
                    );
                }
            }
            let mut sink = EvSink {
                date_cache,
                win_start,
                win_len_u64,
                products,
                overflow,
                tid,
                idx_bits,
                eid_mask,
                extras,
                eid_staged,
            };
            scan_csv_mmap_chunk_sink::<7, _>(mmap, job.start, job.end, &mut sink);
        } else {
            scan_csv_mmap_chunk::<7, _>(mmap, job.start, job.end, |row| {
                handle_event_row::<false>(
                    row, date_cache, win_start, win_len_u64, products, overflow, tid, idx_bits,
                    eid_mask, extras, eid_staged,
                );
            });
        }
    }
}

#[inline(always)]
fn drain_click_jobs(
    next_ck_job: &AtomicUsize,
    click_jobs: &[ChunkJob],
    click_mmaps: &[Mmap],
    campaigns: &CampaignIndex,
    date_cache: &FastDateCache,
    win_len_i64: i64,
    win_start: i64,
    overflow: &OverflowInterner,
    use_compact_ts: bool,
    ck_init_elems: usize,
    ck_extra_elems: usize,
    link_redirect: &LinkRedirect,
    ck_compact_staged: &mut Option<NtSlabBuckets<SurvClick, 8, NT_BLOCK>>,
    ck_wide_staged: &mut Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>>,
) {
    let links_empty = link_redirect.is_empty();
    let mut ck_buf = [PendingClick::default(); 32];
    loop {
        let jid = next_ck_job.fetch_add(1, MemOrdering::Relaxed);
        let Some(job) = click_jobs.get(jid) else {
            break;
        };
        let mmap = &click_mmaps[job.fidx];
        let mut ck_len = 0usize;
        scan_csv_mmap_chunk::<4, _>(mmap, job.start, job.end, |row| {
            handle_click_row(
                row,
                campaigns,
                date_cache,
                win_len_i64,
                win_start,
                overflow,
                links_empty,
                use_compact_ts,
                ck_init_elems,
                ck_extra_elems,
                link_redirect,
                &mut ck_buf,
                &mut ck_len,
                ck_compact_staged,
                ck_wide_staged,
            );
        });
        if ck_len > 0 {
            flush_click_batch(
                &ck_buf[..ck_len],
                link_redirect,
                use_compact_ts,
                ck_compact_staged,
                ck_wide_staged,
                ck_init_elems,
                ck_extra_elems,
                win_start,
            );
        }
    }
}

#[inline(always)]
fn dedup_open_addr<const IDX_BITS: u32>(
    slices: &[&[RawEvent]],
    total_recs: usize,
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    slots: &mut Vec<u32>,
    kept: &mut Vec<RawEvent>,
) {
    let cap = ((total_recs * 3) / 2)
        .max(total_recs + 16)
        .next_power_of_two()
        .max(64);
    if slots.len() < cap {
        slots.resize(cap, 0);
    }
    slots[..cap].fill(0);
    let mask = cap - 1;
    let kept_ptr = kept.as_mut_ptr();
    let mut klen = 0usize;
    let tag_shift = 64 - (32 - IDX_BITS);
    let kidx_mask = (1u32 << IDX_BITS) - 1;

    for &chunk in slices {
        let clen = chunk.len();
        if clen == 0 {
            continue;
        }
        let mut phashes = [0u64; 4];
        let pre = clen.min(4);
        for j in 0..pre {
            let h = mix64(unsafe { chunk.get_unchecked(j) }.eid);
            phashes[j] = h;
            prefetch_ptr(unsafe { slots.as_ptr().add(((h >> 16) as usize) & mask) });
        }
        for ci in 0..clen {
            let h = phashes[ci & 3];
            if ci + 4 < clen {
                let nh = mix64(unsafe { chunk.get_unchecked(ci + 4) }.eid);
                phashes[(ci + 4) & 3] = nh;
                prefetch_ptr(unsafe { slots.as_ptr().add(((nh >> 16) as usize) & mask) });
            }
            let rec = unsafe { *chunk.get_unchecked(ci) };
            let tag = (h >> tag_shift) as u32;
            let mut pos = ((h >> 16) as usize) & mask;
            loop {
                let slot = unsafe { *slots.get_unchecked(pos) };
                if slot == 0 {
                    unsafe {
                        *kept_ptr.add(klen) = rec;
                        *slots.get_unchecked_mut(pos) =
                            (tag << IDX_BITS) | ((klen as u32) + 1);
                    }
                    klen += 1;
                    break;
                }
                if (slot >> IDX_BITS) == tag {
                    let kidx = ((slot & kidx_mask) - 1) as usize;
                    let cur = unsafe { &mut *kept_ptr.add(kidx) };
                    if cur.eid == rec.eid {
                        if cmp_ingest(&rec, cur, all_extras, win_start, idx_bits, idx_mask)
                            == Ordering::Less
                        {
                            *cur = rec;
                        }
                        break;
                    }
                }
                pos = (pos + 1) & mask;
            }
        }
    }
    unsafe {
        kept.set_len(klen);
    }
}

#[inline(always)]
fn dedup_and_emit(
    slices: &[&[RawEvent]],
    total_recs: usize,
    all_extras: &[Vec<ExtraEvent>],
    link_redirect: &LinkRedirect,
    links_empty: bool,
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    slots: &mut Vec<u32>,
    kept: &mut Vec<RawEvent>,
    ev_compact: &mut Option<NtSlabBuckets<SurvEvent, 4, NT_BLOCK>>,
    ev_wide: &mut Option<DirectSlabBuckets<SurvEventWide, WIDE_BLOCK>>,
) {
    kept.clear();
    kept.reserve(total_recs);

    if total_recs < 65_535 {
        dedup_open_addr::<16>(
            slices,
            total_recs,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            slots,
            kept,
        );
    } else if total_recs < 1_000_000 {
        dedup_open_addr::<20>(
            slices,
            total_recs,
            all_extras,
            win_start,
            idx_bits,
            idx_mask,
            slots,
            kept,
        );
    } else {
        let mut ht: HashTable<usize> = HashTable::with_capacity(total_recs);
        for &chunk in slices {
            for &rec in chunk {
                let h = mix64(rec.eid);
                if let Some(&kidx) = ht.find(h, |&idx| kept[idx].eid == rec.eid) {
                    if cmp_ingest(&rec, &kept[kidx], all_extras, win_start, idx_bits, idx_mask)
                        == Ordering::Less
                    {
                        kept[kidx] = rec;
                    }
                } else {
                    let idx = kept.len();
                    kept.push(rec);
                    ht.insert_unique(h, idx, |&i| mix64(kept[i].eid));
                }
            }
        }
    }

    if links_empty {
        for &rec in kept.iter() {
            if (rec.packed >> 62) == 2 {
                continue;
            }
            let pb = (mix64(rec.uid) as usize) & PERSON_MASK;
            push_surv_event(
                rec, rec.uid, pb, all_extras, win_start, idx_bits, idx_mask, ev_compact, ev_wide,
            );
        }
    } else {
        let kslice = kept.as_slice();
        let klen = kslice.len();
        if klen > 0 {
            let mut hbufs = [[0u64; 32]; 2];
            let mut cur_buf = 0usize;
            let first_end = klen.min(32);
            for j in 0..first_end {
                let rec = unsafe { *kslice.get_unchecked(j) };
                if (rec.packed >> 62) != 2 {
                    hbufs[cur_buf][j] = link_redirect.prefetch_bloom_for(rec.uid);
                }
            }

            let mut bstart = 0usize;
            while bstart < klen {
                let bend = (bstart + 32).min(klen);
                let blen = bend - bstart;
                let nxt_buf = cur_buf ^ 1;
                let mut slot_idxs = [usize::MAX; 32];

                for j in 0..blen {
                    let rec = unsafe { *kslice.get_unchecked(bstart + j) };
                    let kind = rec.packed >> 62;
                    if kind != 2 {
                        if let Some(sidx) =
                            link_redirect.bloom_hit_slot_with_hash(hbufs[cur_buf][j])
                        {
                            link_redirect.prefetch_slot(sidx);
                            slot_idxs[j] = sidx;
                        }
                        if kind == 3 {
                            let eref = (rec.packed >> 32) & ((1u64 << 30) - 1);
                            let tid = (eref >> idx_bits) as usize;
                            let eidx = (eref & idx_mask) as usize;
                            prefetch_ptr(unsafe {
                                all_extras.get_unchecked(tid).as_ptr().add(eidx)
                            });
                        }
                    }
                }

                if bend < klen {
                    let nend = (bend + 32).min(klen);
                    for j in 0..(nend - bend) {
                        let rec = unsafe { *kslice.get_unchecked(bend + j) };
                        if (rec.packed >> 62) != 2 {
                            hbufs[nxt_buf][j] = link_redirect.prefetch_bloom_for(rec.uid);
                        }
                    }
                }

                for j in 0..blen {
                    let rec = unsafe { *kslice.get_unchecked(bstart + j) };
                    if (rec.packed >> 62) == 2 {
                        continue;
                    }
                    let sidx = slot_idxs[j];
                    let ch = hbufs[cur_buf][j];
                    let (person, pb) = if sidx == usize::MAX {
                        (rec.uid, (ch as usize) & PERSON_MASK)
                    } else {
                        let p = link_redirect.resolve_from_slot(rec.uid, sidx);
                        let hh = if p == rec.uid { ch } else { mix64(p) };
                        (p, (hh as usize) & PERSON_MASK)
                    };
                    push_surv_event(
                        rec, person, pb, all_extras, win_start, idx_bits, idx_mask, ev_compact,
                        ev_wide,
                    );
                }
                cur_buf = nxt_buf;
                bstart = bend;
            }
        }
    }
}

pub fn run_pipeline(data_dir: &Path, out_dir: &Path, num_threads: usize) {
    let t_total = Instant::now();
    let (win_start, win_end) = parse_manifest(&data_dir.join("manifest.json"));
    let click_win_start = win_start - CLICK_REACH_MS;
    let win_len_i64 = win_end - win_start;
    let win_len_u64 = win_len_i64.max(0) as u64;
    let date_cache = FastDateCache::new(win_start, win_end);

    let min_day = win_start.div_euclid(DAY_MS) as i32;
    let max_day = (win_end - 1).max(win_start).div_euclid(DAY_MS) as i32;
    let day_span = (max_day - min_day + 1).max(1) as usize;

    let link_files = list_csvs(&data_dir.join("links"));
    let event_files = list_csvs(&data_dir.join("events"));
    let click_files = list_csvs(&data_dir.join("clicks"));

    let overflow = OverflowInterner::new();

    // Stage 1: Load small metadata, mmap/interleave event+click files, and build LinkRedirect in parallel
    let t0 = Instant::now();
    let (products, campaigns, link_redirect, inputs): (
        ProductIndex,
        CampaignIndex,
        LinkRedirect,
        OpenedInputs,
    ) = std::thread::scope(|s| {
        let hp = s.spawn(|| ProductIndex::load(&data_dir.join("products.csv")));
        let hc = s.spawn(|| CampaignIndex::load(&data_dir.join("campaigns.csv")));
        let hf = s.spawn(|| open_inputs_and_jobs(&event_files, &click_files));
        let hl = s.spawn(|| build_link_redirect(&link_files, num_threads, &overflow));
        (
            hp.join().unwrap(),
            hc.join().unwrap(),
            hl.join().unwrap(),
            hf.join().unwrap(),
        )
    });
    let dt_links = t0.elapsed();

    let OpenedInputs {
        event_mmaps,
        click_mmaps,
        event_jobs,
        click_jobs,
        total_event_bytes,
    } = inputs;

    let use_compact_ts = win_end >= win_start
        && win_len_i64 <= 2_000_000_000
        && (win_end - click_win_start) <= 4_000_000_000
        && products.categories.len() <= 256;

    let nt_read = num_threads.max(1);
    let num_eid_buckets: usize = if total_event_bytes >= 50_000_000 {
        2048
    } else {
        256
    };
    let eid_mask = num_eid_buckets - 1;

    let (ev_init_elems, ev_extra_elems) = if total_event_bytes >= 10_000_000 {
        let init = (((total_event_bytes as usize / 52 / nt_read).max(524_288) + 511) & !511)
            .max(1_048_576);
        (init, 524_288usize)
    } else {
        (4096usize, 4096usize)
    };
    let (ck_init_elems, ck_extra_elems) = if total_event_bytes >= 10_000_000 {
        (1_048_576usize, 524_288usize)
    } else {
        (4096usize, 4096usize)
    };

    let tid_bits = if nt_read <= 1 {
        0
    } else {
        (nt_read - 1).ilog2() + 1
    };
    let idx_bits = 30 - tid_bits;
    let idx_mask = (1u64 << idx_bits) - 1;

    // Pre-create Stage 2 NtSlabBuckets before entering CSV scan
    let eid_staged_pool: Vec<(usize, NtSlabBuckets<RawEvent, 8, NT_BLOCK>)> = (0..nt_read)
        .map(|tid| {
            (
                tid,
                NtSlabBuckets::new(num_eid_buckets, ev_init_elems, ev_extra_elems),
            )
        })
        .collect();

    // Stage 2: Parallel mmap CSV scan + inline ingest/parse + NtSlabBuckets routing
    let t_read = Instant::now();
    let next_ev_job = AtomicUsize::new(0);
    let next_ck_job = AtomicUsize::new(0);
    let thread_outs: Vec<ReaderThreadOut> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let next_ev_ref = &next_ev_job;
        let next_ck_ref = &next_ck_job;
        let ev_jobs_ref = &event_jobs;
        let ck_jobs_ref = &click_jobs;
        let ev_mmaps_ref = &event_mmaps;
        let ck_mmaps_ref = &click_mmaps;
        let date_cache_ref = &date_cache;
        let products_ref = &products;
        let campaigns_ref = &campaigns;
        let overflow_ref = &overflow;
        let lr_ref = &link_redirect;

        for (tid, mut eid_staged) in eid_staged_pool {
            handles.push(s.spawn(move || {
                let mut extras: Vec<ExtraEvent> = Vec::new();
                let mut ck_compact_staged: Option<NtSlabBuckets<SurvClick, 8, NT_BLOCK>> = None;
                let mut ck_wide_staged: Option<DirectSlabBuckets<SurvClickWide, WIDE_BLOCK>> = None;

                drain_event_jobs(
                    next_ev_ref,
                    ev_jobs_ref,
                    ev_mmaps_ref,
                    date_cache_ref,
                    win_start,
                    win_len_u64,
                    use_compact_ts,
                    products_ref,
                    overflow_ref,
                    tid,
                    idx_bits,
                    eid_mask,
                    &mut extras,
                    &mut eid_staged,
                );

                drain_click_jobs(
                    next_ck_ref,
                    ck_jobs_ref,
                    ck_mmaps_ref,
                    campaigns_ref,
                    date_cache_ref,
                    win_len_i64,
                    win_start,
                    overflow_ref,
                    use_compact_ts,
                    ck_init_elems,
                    ck_extra_elems,
                    lr_ref,
                    &mut ck_compact_staged,
                    &mut ck_wide_staged,
                );

                ReaderThreadOut {
                    eid_out: eid_staged.finish(),
                    extras,
                    ck_compact_out: ck_compact_staged.map(|s| s.finish()),
                    ck_wide_out: ck_wide_staged.map(|s| s.finish()),
                }
            }));
        }

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let dt_read = t_read.elapsed();

    let mut all_raw_slabs: Vec<Vec<HugeSlab<RawEvent>>> = Vec::with_capacity(nt_read);
    let mut eid_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..num_eid_buckets)
        .map(|_| Vec::with_capacity(nt_read))
        .collect();
    let mut all_ck_compact_slabs: Vec<Vec<HugeSlab<SurvClick>>> = if use_compact_ts {
        Vec::with_capacity(nt_read)
    } else {
        Vec::new()
    };
    let mut ck_compact_runs: Vec<Vec<(u8, u16, u32, u16)>> = if use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut all_ck_wide_slabs: Vec<Vec<HugeSlab<SurvClickWide>>> = if !use_compact_ts {
        Vec::with_capacity(nt_read)
    } else {
        Vec::new()
    };
    let mut ck_wide_runs: Vec<Vec<(u8, u16, u32, u16)>> = if !use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut all_extras: Vec<Vec<ExtraEvent>> = Vec::with_capacity(nt_read);

    for (tid_idx, tout) in thread_outs.into_iter().enumerate() {
        let rtid = tid_idx as u8;
        all_extras.push(tout.extras);
        all_raw_slabs.push(tout.eid_out.slabs);
        for (b, runs) in tout.eid_out.bucket_runs.into_iter().enumerate() {
            for (sidx, start, len) in runs {
                eid_runs[b].push((rtid, sidx, start, len));
            }
        }
        if use_compact_ts {
            if let Some(ck_out) = tout.ck_compact_out {
                let ctid = all_ck_compact_slabs.len() as u8;
                all_ck_compact_slabs.push(ck_out.slabs);
                for (pb, runs) in ck_out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ck_compact_runs[pb].push((ctid, sidx, start, len));
                    }
                }
            }
        } else if let Some(ck_out) = tout.ck_wide_out {
            let ctid = all_ck_wide_slabs.len() as u8;
            all_ck_wide_slabs.push(ck_out.slabs);
            for (pb, runs) in ck_out.bucket_runs.into_iter().enumerate() {
                for (sidx, start, len) in runs {
                    ck_wide_runs[pb].push((ctid, sidx, start, len));
                }
            }
        }
    }

    // Stage 3: Pre-alloc output NtSlabBuckets, then run L2-hot direct dedup with zero munmap contention
    let t_dedup = Instant::now();
    let next_eb = AtomicUsize::new(0);
    let links_empty = link_redirect.is_empty();
    let (surv_init_elems, surv_extra_elems) = if total_event_bytes >= 10_000_000 {
        let init = ((total_event_bytes as usize / 52 / nt_read).max(524_288) + 511) & !511;
        (init, 524_288usize)
    } else {
        (4096usize, 4096usize)
    };

    let stage3_pools: Vec<(
        Option<NtSlabBuckets<SurvEvent, 4, NT_BLOCK>>,
        Option<DirectSlabBuckets<SurvEventWide, WIDE_BLOCK>>,
    )> = (0..nt_read)
        .map(|_| {
            if use_compact_ts {
                (
                    Some(NtSlabBuckets::new(
                        PERSON_BUCKETS,
                        surv_init_elems,
                        surv_extra_elems,
                    )),
                    None,
                )
            } else {
                (
                    None,
                    Some(DirectSlabBuckets::new(
                        PERSON_BUCKETS,
                        surv_init_elems,
                        surv_extra_elems,
                    )),
                )
            }
        })
        .collect();

    let (dedup_compact_outs, dedup_wide_outs): (
        Vec<SlabBucketsOut<SurvEvent>>,
        Vec<SlabBucketsOut<SurvEventWide>>,
    ) = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let all_raw_slabs_ref = &all_raw_slabs;
        let eid_runs_ref = &eid_runs;
        let all_extras_ref = &all_extras;
        let link_redirect_ref = &link_redirect;
        let next_eb_ref = &next_eb;

        for (mut ev_compact, mut ev_wide) in stage3_pools {
            handles.push(s.spawn(move || {
                let mut slots: Vec<u32> = Vec::new();
                let mut kept: Vec<RawEvent> = Vec::new();
                let mut chunk_refs: Vec<&[RawEvent]> = Vec::new();

                loop {
                    let b = next_eb_ref.fetch_add(1, MemOrdering::Relaxed);
                    if b >= num_eid_buckets {
                        break;
                    }
                    let runs = &eid_runs_ref[b];
                    if runs.is_empty() {
                        continue;
                    }
                    let total_recs: usize = runs.iter().map(|r| r.3 as usize).sum();
                    if total_recs == 0 {
                        continue;
                    }

                    chunk_refs.clear();
                    for &(rtid, sidx, start, len) in runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_raw_slabs_ref, rtid, sidx, start, len)
                        };
                        if !chunk.is_empty() {
                            chunk_refs.push(chunk);
                        }
                    }
                    dedup_and_emit(
                        &chunk_refs,
                        total_recs,
                        all_extras_ref,
                        link_redirect_ref,
                        links_empty,
                        win_start,
                        idx_bits,
                        idx_mask,
                        &mut slots,
                        &mut kept,
                        &mut ev_compact,
                        &mut ev_wide,
                    );
                }

                (
                    ev_compact.map(|s| s.finish()),
                    ev_wide.map(|s| s.finish()),
                )
            }));
        }

        let mut comp = Vec::new();
        let mut wide = Vec::new();
        for h in handles {
            let (c, w) = h.join().unwrap();
            if let Some(x) = c {
                comp.push(x);
            }
            if let Some(x) = w {
                wide.push(x);
            }
        }
        (comp, wide)
    });
    let dt_dedup = t_dedup.elapsed();

    // Stage 4: Sessionize, attribute, write parquet & overlap ALL prior munmap/teardown on 1 fault-free thread
    let t_sess = Instant::now();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_daily_dir).expect("mkdir campaign_daily");

    let drop_gate = (Mutex::new(false), Condvar::new());
    let drop_gate_ref = &drop_gate;
    let (camp_stores, wrote_any_user) = std::thread::scope(|s| {
        let drop_handle = s.spawn(move || {
            let mut g = drop_gate_ref.0.lock().unwrap();
            while !*g {
                g = drop_gate_ref.1.wait(g).unwrap();
            }
            drop(g);
            drop(event_mmaps);
            drop(click_mmaps);
            drop(event_jobs);
            drop(click_jobs);
            drop(eid_runs);
            drop(all_raw_slabs);
            drop(all_extras);
            drop(link_redirect);
        });

        let res = if use_compact_ts {
            let mut all_ev_slabs: Vec<Vec<HugeSlab<SurvEvent>>> =
                Vec::with_capacity(dedup_compact_outs.len());
            let mut ev_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..PERSON_BUCKETS)
                .map(|_| Vec::with_capacity(dedup_compact_outs.len()))
                .collect();
            for (tid_idx, out) in dedup_compact_outs.into_iter().enumerate() {
                let rtid = tid_idx as u8;
                all_ev_slabs.push(out.slabs);
                for (pb, runs) in out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ev_runs[pb].push((rtid, sidx, start, len));
                    }
                }
            }
            process_person_buckets_compact(
                nt_read,
                &all_ev_slabs,
                &ev_runs,
                &all_ck_compact_slabs,
                &ck_compact_runs,
                &overflow,
                &products,
                &campaigns,
                win_start,
                min_day,
                day_span,
                &user_daily_dir,
                drop_gate_ref,
            )
        } else {
            *drop_gate_ref.0.lock().unwrap() = true;
            drop_gate_ref.1.notify_all();
            let mut all_ev_slabs: Vec<Vec<HugeSlab<SurvEventWide>>> =
                Vec::with_capacity(dedup_wide_outs.len());
            let mut ev_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..PERSON_BUCKETS)
                .map(|_| Vec::with_capacity(dedup_wide_outs.len()))
                .collect();
            for (tid_idx, out) in dedup_wide_outs.into_iter().enumerate() {
                let rtid = tid_idx as u8;
                all_ev_slabs.push(out.slabs);
                for (pb, runs) in out.bucket_runs.into_iter().enumerate() {
                    for (sidx, start, len) in runs {
                        ev_runs[pb].push((rtid, sidx, start, len));
                    }
                }
            }
            process_person_buckets_wide(
                nt_read,
                &all_ev_slabs,
                &ev_runs,
                &all_ck_wide_slabs,
                &ck_wide_runs,
                &overflow,
                &products,
                &campaigns,
                min_day,
                day_span,
                &user_daily_dir,
            )
        };
        drop_handle.join().unwrap();
        res
    });

    if !wrote_any_user {
        write_empty_user_daily(&user_daily_dir.join("part-00000.parquet"));
    }

    // Merge thread-local CampStores and write campaign_daily/part-00000.parquet
    let num_camps = campaigns.ids.len();
    let mut cd_ids: Vec<&str> = Vec::new();
    let mut cd_channels: Vec<&str> = Vec::new();
    let mut cd_days: Vec<i32> = Vec::new();
    let mut cd_sessions: Vec<i32> = Vec::new();
    let mut cd_purchasing: Vec<i32> = Vec::new();
    let mut cd_revenues: Vec<i64> = Vec::new();
    let mut cd_assisted: Vec<i32> = Vec::new();

    if day_span <= 128 && num_camps <= 20_000 {
        let total_slots = num_camps * day_span;
        let mut merged_core = vec![CampCore::default(); total_slots];
        let mut merged_assisted = vec![0i32; total_slots];
        for st in &camp_stores {
            if let CampStore::Flat { core, assisted, .. } = st {
                for (i, agg) in core.iter().enumerate() {
                    if agg.sessions != 0 {
                        merged_core[i].sessions += agg.sessions;
                        merged_core[i].purchasing_sessions += agg.purchasing_sessions;
                        merged_core[i].revenue_cents += agg.revenue_cents;
                    }
                }
                for (i, &a) in assisted.iter().enumerate() {
                    if a != 0 {
                        merged_assisted[i] += a;
                    }
                }
            }
        }
        for c in 0..num_camps {
            for d in 0..day_span {
                let slot = c * day_span + d;
                let agg = merged_core[slot];
                let ass = merged_assisted[slot];
                if agg.sessions > 0 || ass > 0 {
                    cd_ids.push(&campaigns.ids[c]);
                    cd_channels.push(&campaigns.channels[c]);
                    cd_days.push(min_day + d as i32);
                    cd_sessions.push(agg.sessions);
                    cd_purchasing.push(agg.purchasing_sessions);
                    cd_revenues.push(agg.revenue_cents);
                    cd_assisted.push(ass);
                }
            }
        }
    } else {
        let mut merged: HashMap<(u32, i32), CampAgg, RandomState> =
            HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9));
        for st in camp_stores {
            if let CampStore::Map(m) = st {
                for (k, agg) in m {
                    let e = merged.entry(k).or_default();
                    e.sessions += agg.sessions;
                    e.purchasing_sessions += agg.purchasing_sessions;
                    e.revenue_cents += agg.revenue_cents;
                    e.assisted_sessions += agg.assisted_sessions;
                }
            }
        }
        for ((c, day), agg) in merged {
            if agg.sessions > 0 || agg.assisted_sessions > 0 {
                cd_ids.push(&campaigns.ids[c as usize]);
                cd_channels.push(&campaigns.channels[c as usize]);
                cd_days.push(day);
                cd_sessions.push(agg.sessions);
                cd_purchasing.push(agg.purchasing_sessions);
                cd_revenues.push(agg.revenue_cents);
                cd_assisted.push(agg.assisted_sessions);
            }
        }
    }

    write_campaign_daily_parquet(
        &camp_daily_dir.join("part-00000.parquet"),
        &cd_ids,
        &cd_channels,
        &cd_days,
        &cd_sessions,
        &cd_purchasing,
        &cd_revenues,
        &cd_assisted,
    );

    let dt_sess = t_sess.elapsed();
    eprintln!(
        "PROFILE links={:.3}s read={:.3}s dedup={:.3}s sess_write={:.3}s total={:.3}s",
        dt_links.as_secs_f64(),
        dt_read.as_secs_f64(),
        dt_dedup.as_secs_f64(),
        dt_sess.as_secs_f64(),
        t_total.elapsed().as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nt_slab_buckets_multi_slab_and_partial_flushes() {
        // Test RawEvent (K=8, BLOCK=512 -> NT_BLOCK=512) with forced multi-slab rollovers & all tail lens 0..8
        let mut nt_raw: NtSlabBuckets<RawEvent, 8, 512> = NtSlabBuckets::new(16, 1024, 1024);
        let mut expected_raw: Vec<(u64, u64, u64)> = Vec::new();
        for b in 0..16usize {
            let n = 505 + b; // tests partial flushes 1..7, exact multiple 512, and >512 block refills across slabs
            for i in 0..n {
                let item = RawEvent {
                    eid: ((b as u64) << 32) | (i as u64),
                    uid: (i as u64) * 3 + 1,
                    packed: ((b as u64) << 16) ^ (i as u64),
                };
                expected_raw.push((item.eid, item.uid, item.packed));
                nt_raw.push(b, item);
            }
        }
        let out_raw = nt_raw.finish();
        assert!(out_raw.slabs.len() > 2, "expected multiple slab rollovers");
        let mut got_raw: Vec<(u64, u64, u64)> = Vec::new();
        for b in 0..16usize {
            for &(sidx, start, len) in &out_raw.bucket_runs[b] {
                let slice = unsafe {
                    SlabBucketsOut::get_slice(
                        std::slice::from_ref(&out_raw.slabs),
                        0,
                        sidx,
                        start,
                        len,
                    )
                };
                for r in slice {
                    got_raw.push((r.eid, r.uid, r.packed));
                }
            }
        }
        expected_raw.sort_unstable();
        got_raw.sort_unstable();
        assert_eq!(got_raw, expected_raw, "NtSlabBuckets RawEvent mismatch");

        // Test SurvEvent (K=4, BLOCK=512) with partial tail lengths 0..4 and slab rollover
        let mut nt_ev: NtSlabBuckets<SurvEvent, 4, 512> = NtSlabBuckets::new(12, 1024, 1024);
        let mut expected_ev: Vec<(u64, u32, u32)> = Vec::new();
        for b in 0..12usize {
            let n = 510 + b;
            for i in 0..n {
                let item = SurvEvent {
                    person: ((b as u64) << 32) | (i as u64),
                    ts_rel: (i * 10) as u32,
                    price_cat: (b as u32) ^ (i as u32),
                };
                expected_ev.push((item.person, item.ts_rel, item.price_cat));
                nt_ev.push(b, item);
            }
        }
        let out_ev = nt_ev.finish();
        assert!(out_ev.slabs.len() > 2, "expected multiple SurvEvent slabs");
        let mut got_ev: Vec<(u64, u32, u32)> = Vec::new();
        for b in 0..12usize {
            for &(sidx, start, len) in &out_ev.bucket_runs[b] {
                let slice = unsafe {
                    SlabBucketsOut::get_slice(
                        std::slice::from_ref(&out_ev.slabs),
                        0,
                        sidx,
                        start,
                        len,
                    )
                };
                for r in slice {
                    got_ev.push((r.person, r.ts_rel, r.price_cat));
                }
            }
        }
        expected_ev.sort_unstable();
        got_ev.sort_unstable();
        assert_eq!(got_ev, expected_ev, "NtSlabBuckets SurvEvent mismatch");
    }

    #[test]
    fn radix_sort_events_u64_matches_std() {
        let mut tmp = Vec::new();

        // Uniform high byte (triggers pass skipping), duplicates, max range
        let mut v_const: Vec<u64> = (0..1000u64).map(|i| (42u64 << 32) | (i & 0xFFFF)).collect();
        let mut exp_const = v_const.clone();
        radix_sort_events_u64(&mut v_const, &mut tmp);
        exp_const.sort_unstable_by_key(|x| x >> 32);
        assert_eq!(v_const.iter().map(|x| x >> 32).collect::<Vec<_>>(), exp_const.iter().map(|x| x >> 32).collect::<Vec<_>>());

        // Pseudo-random across full 32-bit ts_rel range + varied price_cat
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for n in [1usize, 2, 17, 256, 257, 513, 4099] {
            let mut arr = Vec::with_capacity(n);
            for _ in 0..n {
                x = mix64(x);
                arr.push(x);
            }
            let mut expected = arr.clone();
            expected.sort_unstable_by_key(|v| v >> 32);
            radix_sort_events_u64(&mut arr, &mut tmp);
            assert_eq!(arr, expected, "radix sort n={n}");
        }

        // Empty slice
        let mut empty: Vec<u64> = Vec::new();
        radix_sort_events_u64(&mut empty, &mut tmp);
        assert!(empty.is_empty());
    }
}
