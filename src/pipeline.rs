use std::cmp::Ordering;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as MemOrdering};
use std::time::Instant;

use ahash::RandomState;
use hashbrown::{HashMap, HashTable};
use memmap2::{Mmap, MmapOptions};

use crate::csv::{
    for_each_data_line, parse_csv_line, parse_ingest_rel_sec, parse_price_cents, parse_ts,
    parse_ts_rel_ms, scan_csv_mmap_chunk, FastDateCache, CLICK_REACH_MS, DAY_MS, LATE_LIMIT_MS,
    SESSION_GAP_MS,
};
use crate::links::{build_link_redirect, LinkRedirect};
use crate::pack::{mix64, OverflowInterner, EMPTY_KEY, OVERFLOW_BIT};
use crate::parquet_writer::{
    write_campaign_daily_parquet, write_empty_user_daily, UserDailyParquetWriter,
};

const PERSON_BUCKETS: usize = 1024;
const PERSON_MASK: usize = PERSON_BUCKETS - 1;

#[inline(always)]
fn discard_mem<T>(v: T, forget: bool) {
    if forget {
        std::mem::forget(v);
    } else {
        drop(v);
    }
}

// =========================================================================
// Fast Direct Lookup Indexes with General Hash Fallbacks
// =========================================================================

#[inline(always)]
fn parse_p_num(b: &[u8]) -> Option<usize> {
    if b.len() < 2 || b[0] != b'p' || b[1] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
        if val > 2_000_000 {
            return None;
        }
    }
    Some(val)
}

#[inline(always)]
fn parse_c_num(b: &[u8]) -> Option<usize> {
    if b.len() < 3 || b[0] != b'c' || b[1] != b'_' || b[2] == b'0' {
        return None;
    }
    let mut val = 0usize;
    for &c in &b[2..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as usize;
        if val > 100_000 {
            return None;
        }
    }
    Some(val)
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
    slabs: Vec<Vec<std::mem::MaybeUninit<T>>>,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

impl<T> SlabBucketsOut<T> {
    #[inline(always)]
    unsafe fn get_slice<'a>(
        all_slabs: &'a [Vec<Vec<std::mem::MaybeUninit<T>>>],
        tid: u8,
        sidx: u16,
        start: u32,
        len: u16,
    ) -> &'a [T] {
        let slab = all_slabs
            .get_unchecked(tid as usize)
            .get_unchecked(sidx as usize);
        let raw = slab.get_unchecked(start as usize..start as usize + len as usize);
        std::slice::from_raw_parts(raw.as_ptr() as *const T, len as usize)
    }
}

struct SlabBuckets<T: Copy, const CAP: usize, const BLOCK: usize> {
    num_buckets: usize,
    slab_elems: usize,
    staging: Vec<T>,
    counts: Vec<u16>,
    active_block: Vec<(u16, u32, u16)>, // (slab_idx, start_off, filled_in_block)
    slabs: Vec<Vec<std::mem::MaybeUninit<T>>>,
    bucket_runs: Vec<Vec<(u16, u32, u16)>>,
}

impl<T: Copy, const CAP: usize, const BLOCK: usize> SlabBuckets<T, CAP, BLOCK> {
    const _CHECK: () = assert!(BLOCK % CAP == 0);

    fn new(num_buckets: usize, slab_elems: usize, zero: T) -> Self {
        let _ = Self::_CHECK;
        let slab_elems = ((slab_elems.max(BLOCK) + BLOCK - 1) / BLOCK) * BLOCK;
        Self {
            num_buckets,
            slab_elems,
            staging: vec![zero; num_buckets * CAP],
            counts: vec![0u16; num_buckets],
            active_block: vec![(0, 0, BLOCK as u16); num_buckets],
            slabs: Vec::new(),
            bucket_runs: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[cold]
    fn alloc_block(&mut self, len: usize) -> (u16, u32) {
        let last_ok = !self.slabs.is_empty() && {
            let last = &self.slabs[self.slabs.len() - 1];
            last.len() + len <= last.capacity()
        };
        if !last_ok {
            let cap = self.slab_elems.max(len);
            self.slabs.push(Vec::with_capacity(cap));
        }
        let sidx = self.slabs.len() - 1;
        let slab = &mut self.slabs[sidx];
        let start = slab.len();
        unsafe {
            slab.set_len(start + len);
        }
        (sidx as u16, start as u32)
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let c = unsafe { *self.counts.get_unchecked(b) } as usize;
        let base = b * CAP;
        unsafe {
            *self.staging.get_unchecked_mut(base + c) = item;
        }
        let next = c + 1;
        if next == CAP {
            self.flush_bucket_stage(b, base);
        } else {
            unsafe {
                *self.counts.get_unchecked_mut(b) = next as u16;
            }
        }
    }

    #[inline]
    fn flush_bucket_stage(&mut self, b: usize, base: usize) {
        let (mut sidx, mut bstart, mut bfill) = unsafe { *self.active_block.get_unchecked(b) };
        if bfill == BLOCK as u16 {
            let (ns, nst) = self.alloc_block(BLOCK);
            sidx = ns;
            bstart = nst;
            bfill = 0;
        }
        let dst_off = bstart as usize + bfill as usize;
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.staging.as_ptr().add(base) as *const std::mem::MaybeUninit<T>,
                self.slabs
                    .get_unchecked_mut(sidx as usize)
                    .as_mut_ptr()
                    .add(dst_off),
                CAP,
            );
        }
        bfill += CAP as u16;
        if bfill == BLOCK as u16 {
            unsafe { self.bucket_runs.get_unchecked_mut(b) }.push((sidx, bstart, BLOCK as u16));
        }
        unsafe {
            *self.active_block.get_unchecked_mut(b) = (sidx, bstart, bfill);
            *self.counts.get_unchecked_mut(b) = 0;
        }
    }

    fn finish(mut self) -> SlabBucketsOut<T> {
        for b in 0..self.num_buckets {
            let c = self.counts[b] as usize;
            let (sidx, bstart, bfill) = self.active_block[b];
            if c > 0 {
                let base = b * CAP;
                if bfill == BLOCK as u16 {
                    let (ns, nst) = self.alloc_block(c);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            self.staging.as_ptr().add(base) as *const std::mem::MaybeUninit<T>,
                            self.slabs
                                .get_unchecked_mut(ns as usize)
                                .as_mut_ptr()
                                .add(nst as usize),
                            c,
                        );
                    }
                    self.bucket_runs[b].push((ns, nst, c as u16));
                } else {
                    let dst_off = bstart as usize + bfill as usize;
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            self.staging.as_ptr().add(base) as *const std::mem::MaybeUninit<T>,
                            self.slabs
                                .get_unchecked_mut(sidx as usize)
                                .as_mut_ptr()
                                .add(dst_off),
                            c,
                        );
                    }
                    self.bucket_runs[b].push((sidx, bstart, bfill + c as u16));
                }
            } else if bfill > 0 && bfill < BLOCK as u16 {
                self.bucket_runs[b].push((sidx, bstart, bfill));
            }
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

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
    assisted_sessions: i32,
}

enum CampStore {
    Flat {
        data: Vec<CampAgg>,
        min_day: i32,
        day_span: usize,
    },
    Map(HashMap<(u32, i32), CampAgg, RandomState>),
}

impl CampStore {
    fn new(use_flat: bool, num_camps: usize, min_day: i32, day_span: usize) -> Self {
        if use_flat {
            CampStore::Flat {
                data: vec![CampAgg::default(); num_camps * day_span],
                min_day,
                day_span,
            }
        } else {
            CampStore::Map(HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9)))
        }
    }

    #[inline(always)]
    fn entry_mut(&mut self, camp_idx: u32, day: i32) -> &mut CampAgg {
        match self {
            CampStore::Flat {
                data,
                min_day,
                day_span,
            } => {
                let d = (day - *min_day) as usize;
                let slot = (camp_idx as usize) * (*day_span) + d;
                unsafe { data.get_unchecked_mut(slot) }
            }
            CampStore::Map(m) => m.entry((camp_idx, day)).or_default(),
        }
    }
}

// =========================================================================
// Stage 4a: Ultra-Fast Compact Path (Hash-Grouping + 8B Events + Radix Sort + Zero-Copy Parquet)
// =========================================================================

/// 3-pass 11-bit LSD radix sort on high 32 bits (`ts_rel`) of `grouped_evs` (`u64`).
#[inline]
fn radix_sort_events_u64(arr: &mut [u64], tmp: &mut Vec<u64>) {
    let n = arr.len();
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let tmp_slice = &mut tmp[..n];

    let mut c0 = [0usize; 2048];
    let mut c1 = [0usize; 2048];
    let mut c2 = [0usize; 1024];

    for &v in arr.iter() {
        let k = (v >> 32) as usize;
        c0[k & 0x7FF] += 1;
        c1[(k >> 11) & 0x7FF] += 1;
        c2[(k >> 22) & 0x3FF] += 1;
    }

    let mut s0 = 0usize;
    let mut s1 = 0usize;
    for i in 0..2048 {
        let v0 = c0[i];
        c0[i] = s0;
        s0 += v0;
        let v1 = c1[i];
        c1[i] = s1;
        s1 += v1;
    }
    let mut s2 = 0usize;
    for i in 0..1024 {
        let v2 = c2[i];
        c2[i] = s2;
        s2 += v2;
    }

    // Pass 0: arr -> tmp_slice
    for &v in arr.iter() {
        let b = ((v >> 32) as usize) & 0x7FF;
        let dst = c0[b];
        c0[b] = dst + 1;
        unsafe {
            *tmp_slice.get_unchecked_mut(dst) = v;
        }
    }
    // Pass 1: tmp_slice -> arr
    for &v in tmp_slice.iter() {
        let b = ((v >> 43) as usize) & 0x7FF;
        let dst = c1[b];
        c1[b] = dst + 1;
        unsafe {
            *arr.get_unchecked_mut(dst) = v;
        }
    }
    // Pass 2: arr -> tmp_slice
    for &v in arr.iter() {
        let b = ((v >> 54) as usize) & 0x3FF;
        let dst = c2[b];
        c2[b] = dst + 1;
        unsafe {
            *tmp_slice.get_unchecked_mut(dst) = v;
        }
    }
    arr.copy_from_slice(tmp_slice);
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
    all_ev_slabs: &[Vec<Vec<std::mem::MaybeUninit<SurvEvent>>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<Vec<std::mem::MaybeUninit<SurvClick>>>],
    ck_runs: &[Vec<(u8, u16, u32, u16)>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    win_start: i64,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;
    let start_in_day = win_start.rem_euclid(DAY_MS) as u64;

    // Sort buckets largest-first by event+click count to eliminate whale tail latency
    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
            if ev_n == 0 {
                None
            } else {
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

    let next_idx = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store = CampStore::new(use_flat, num_camps, min_day, day_span);
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer = UserDailyParquetWriter::new(part_path, &products.categories);

                // Thread-local reusable buffers (2048 * 8B = 16KB fits L1d)
                let mut ht_keys: Vec<u64> = vec![EMPTY_KEY; 2048];
                let mut ht_pids: Vec<u32> = vec![0u32; 2048];
                let mut ht_cap: usize = 2048;

                let mut person_keys: Vec<u64> = Vec::with_capacity(4096);
                let mut ev_cnt: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut cur_ev_off: Vec<u32> = Vec::with_capacity(4096);
                let mut ev_pids: Vec<u32> = Vec::with_capacity(65_536);
                let mut grouped_evs: Vec<u64> = Vec::with_capacity(65_536);
                let mut radix_tmp: Vec<u64> = Vec::new();

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

                    // Pass 1: Hash-group surviving events by person (shift >> 16 disjoint from PERSON_MASK)
                    ht_keys[..ht_cap].fill(EMPTY_KEY);
                    let mut ht_mask = ht_cap - 1;
                    person_keys.clear();
                    ev_cnt.clear();
                    ev_pids.clear();
                    ev_pids.reserve(total_ev);

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
                                    if person_keys.len() * 2 >= ht_cap {
                                        let new_cap = ht_cap * 2;
                                        let new_mask = new_cap - 1;
                                        ht_keys.resize(new_cap, EMPTY_KEY);
                                        ht_pids.resize(new_cap, 0);
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
                                        ht_cap = new_cap;
                                        ht_mask = new_mask;
                                    }
                                    break npid;
                                }
                                pos = (pos + 1) & ht_mask;
                            };
                            unsafe {
                                *ev_cnt.get_unchecked_mut(pid as usize) += 1;
                            }
                            ev_pids.push(pid);
                        }
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

                    // Pass 2: Scatter compact 8-byte events per person
                    if grouped_evs.len() < total_ev {
                        grouped_evs.resize(total_ev, 0);
                    }
                    let mut k = 0usize;
                    for &(rtid, sidx, start, len) in pb_ev_runs {
                        let chunk = unsafe {
                            SlabBucketsOut::get_slice(all_ev_slabs, rtid, sidx, start, len)
                        };
                        for ev in chunk {
                            let pid = unsafe { *ev_pids.get_unchecked(k) } as usize;
                            k += 1;
                            let dst = unsafe { *cur_ev_off.get_unchecked(pid) } as usize;
                            unsafe {
                                *cur_ev_off.get_unchecked_mut(pid) = (dst + 1) as u32;
                                *grouped_evs.get_unchecked_mut(dst) =
                                    ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
                            }
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
                        if p_evs.len() > 512 {
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
                        let uid_prefixed: &[u8] = if pname_bytes.len() <= 12 {
                            uid_pref_stack[..4]
                                .copy_from_slice(&(pname_bytes.len() as u32).to_le_bytes());
                            uid_pref_stack[4..4 + pname_bytes.len()].copy_from_slice(pname_bytes);
                            &uid_pref_stack[..4 + pname_bytes.len()]
                        } else {
                            uid_pref_vec.clear();
                            uid_pref_vec
                                .extend_from_slice(&(pname_bytes.len() as u32).to_le_bytes());
                            uid_pref_vec.extend_from_slice(pname_bytes);
                            &uid_pref_vec
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
                                                camp_store
                                                    .entry_mut(c as u32, cur_day)
                                                    .assisted_sessions += delta as i32;
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
                                    pq_writer.push_prefixed_uid(
                                        uid_prefixed,
                                        cur_day,
                                        d_sessions,
                                        d_events,
                                        d_rev,
                                        top_cat,
                                    );
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
                                            camp_store
                                                .entry_mut(c as u32, sess_day)
                                                .assisted_sessions += delta as i32;
                                        }
                                        camp_active_since[c] = u32::MAX;
                                    }
                                    ck_left += 1;
                                }
                            }

                            if ck_left == ck_right {
                                let agg = camp_store.entry_mut(campaigns.organic_id, sess_day);
                                agg.sessions += 1;
                                if sess_rev > 0 {
                                    agg.purchasing_sessions += 1;
                                    agg.revenue_cents += sess_rev;
                                }
                            } else {
                                let win_camp =
                                    unsafe { p_cks.get_unchecked(ck_right - 1) }.camp_idx;
                                let agg = camp_store.entry_mut(win_camp, sess_day);
                                agg.sessions += 1;
                                if sess_rev > 0 {
                                    agg.purchasing_sessions += 1;
                                    agg.revenue_cents += sess_rev;
                                }
                            }
                            day_sess_seq += 1;
                        }

                        if cur_day != i32::MIN {
                            for &c_u32 in &active_camps {
                                let c = c_u32 as usize;
                                if camp_in_win[c] > 0 {
                                    let delta = day_sess_seq - camp_active_since[c];
                                    if delta > 0 {
                                        camp_store
                                            .entry_mut(c_u32, cur_day)
                                            .assisted_sessions += delta as i32;
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
                            pq_writer.push_prefixed_uid(
                                uid_prefixed,
                                cur_day,
                                d_sessions,
                                d_events,
                                d_rev,
                                top_cat,
                            );
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
    all_ev_slabs: &[Vec<Vec<std::mem::MaybeUninit<SurvEventWide>>>],
    ev_runs: &[Vec<(u8, u16, u32, u16)>],
    all_ck_slabs: &[Vec<Vec<std::mem::MaybeUninit<SurvClickWide>>>],
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

    let mut order: Vec<(usize, usize)> = (0..PERSON_BUCKETS)
        .filter_map(|pb| {
            let ev_n: usize = ev_runs[pb].iter().map(|r| r.3 as usize).sum();
            if ev_n == 0 {
                None
            } else {
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

    let next_idx = AtomicUsize::new(0);

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let work_order_ref = &work_order;
        let next_idx_ref = &next_idx;

        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store = CampStore::new(use_flat, num_camps, min_day, day_span);
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut pq_writer = UserDailyParquetWriter::new(part_path, &products.categories);

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
                                let agg = camp_store.entry_mut(campaigns.organic_id, sess_day);
                                agg.sessions += 1;
                                if sess_rev > 0 {
                                    agg.purchasing_sessions += 1;
                                    agg.revenue_cents += sess_rev;
                                }
                            } else {
                                let win_camp = p_cks[ck_right - 1].camp_idx;
                                let agg = camp_store.entry_mut(win_camp, sess_day);
                                agg.sessions += 1;
                                if sess_rev > 0 {
                                    agg.purchasing_sessions += 1;
                                    agg.revenue_cents += sess_rev;
                                }

                                let mut widx = 0usize;
                                for ridx in 0..active_camps.len() {
                                    let c = active_camps[ridx];
                                    if camp_in_win[c as usize] > 0 {
                                        active_camps[widx] = c;
                                        widx += 1;
                                        camp_store.entry_mut(c, sess_day).assisted_sessions += 1;
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

enum ChunkKind {
    Event(usize),
    Click(usize),
}

struct ChunkJob {
    kind: ChunkKind,
    start: usize,
    end: usize,
    file_len: usize,
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

#[inline(always)]
fn flush_click_batch(
    batch: &[PendingClick],
    link_redirect: &LinkRedirect,
    use_compact_ts: bool,
    ck_compact_staged: &mut Option<SlabBuckets<SurvClick, 8, 256>>,
    ck_wide_staged: &mut Option<SlabBuckets<SurvClickWide, 8, 256>>,
    ck_slab_elems: usize,
    win_start: i64,
) {
    let mut slot_idxs = [usize::MAX; 32];
    for (j, pk) in batch.iter().enumerate() {
        if let Some(sidx) = link_redirect.bloom_hit_slot(pk.uid_k) {
            link_redirect.prefetch_slot(sidx);
            slot_idxs[j] = sidx;
        }
    }
    if use_compact_ts {
        let out = ck_compact_staged
            .get_or_insert_with(|| SlabBuckets::new(PERSON_BUCKETS, ck_slab_elems, SurvClick::default()));
        for (j, pk) in batch.iter().enumerate() {
            let sidx = slot_idxs[j];
            let person = if sidx == usize::MAX {
                pk.uid_k
            } else {
                link_redirect.resolve_from_slot(pk.uid_k, sidx)
            };
            let pb = (mix64(person) as usize) & PERSON_MASK;
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
        let out = ck_wide_staged
            .get_or_insert_with(|| SlabBuckets::new(PERSON_BUCKETS, ck_slab_elems, SurvClickWide::default()));
        for (j, pk) in batch.iter().enumerate() {
            let sidx = slot_idxs[j];
            let person = if sidx == usize::MAX {
                pk.uid_k
            } else {
                link_redirect.resolve_from_slot(pk.uid_k, sidx)
            };
            let pb = (mix64(person) as usize) & PERSON_MASK;
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
    all_extras: &[Vec<ExtraEvent>],
    win_start: i64,
    idx_bits: u32,
    idx_mask: u64,
    ev_compact: &mut Option<SlabBuckets<SurvEvent, 8, 256>>,
    ev_wide: &mut Option<SlabBuckets<SurvEventWide, 8, 256>>,
) {
    let kind = rec.packed >> 62;
    let pb = (mix64(person) as usize) & PERSON_MASK;
    if let Some(ref mut out) = ev_compact {
        let (ts_rel, price_cat) = if kind < 2 {
            (((rec.packed >> 32) & 0x7FFF_FFFF) as u32, 0u32)
        } else {
            let eref = (rec.packed >> 32) & ((1u64 << 30) - 1);
            let tid = (eref >> idx_bits) as usize;
            let eidx = (eref & idx_mask) as usize;
            let ex = all_extras[tid][eidx];
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
            let ex = all_extras[tid][eidx];
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

    // Stage 1: Load products.csv, campaigns.csv, and build LinkRedirect in parallel
    let t0 = Instant::now();
    let (products, campaigns, link_redirect): (ProductIndex, CampaignIndex, LinkRedirect) =
        std::thread::scope(|s| {
            let hp = s.spawn(|| ProductIndex::load(&data_dir.join("products.csv")));
            let hc = s.spawn(|| CampaignIndex::load(&data_dir.join("campaigns.csv")));
            let hl = s.spawn(|| build_link_redirect(&link_files, num_threads, &overflow));
            (hp.join().unwrap(), hc.join().unwrap(), hl.join().unwrap())
        });
    let dt_links = t0.elapsed();

    let use_compact_ts = win_end >= win_start
        && win_len_i64 <= 2_000_000_000
        && (win_end - click_win_start) <= 4_000_000_000
        && products.categories.len() <= 256;

    // Stage 2: Mmap non-empty event & click files, build largest-first unified chunk queue
    let t_read = Instant::now();
    let mut event_mmaps: Vec<Mmap> = Vec::with_capacity(event_files.len());
    let mut click_mmaps: Vec<Mmap> = Vec::with_capacity(click_files.len());
    let mut jobs: Vec<ChunkJob> = Vec::new();
    let mut total_event_bytes = 0u64;
    const CHUNK_BYTES: usize = 2 * 1024 * 1024;

    for p in &event_files {
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
        #[cfg(unix)]
        let _ = m.advise(memmap2::Advice::Sequential);
        total_event_bytes += len as u64;
        let idx = event_mmaps.len();
        event_mmaps.push(m);
        let mut pos = 0usize;
        while pos < len {
            let end = (pos + CHUNK_BYTES).min(len);
            jobs.push(ChunkJob {
                kind: ChunkKind::Event(idx),
                start: pos,
                end,
                file_len: len,
            });
            pos = end;
        }
    }

    for p in &click_files {
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
        #[cfg(unix)]
        let _ = m.advise(memmap2::Advice::Sequential);
        let idx = click_mmaps.len();
        click_mmaps.push(m);
        let mut pos = 0usize;
        while pos < len {
            let end = (pos + CHUNK_BYTES).min(len);
            jobs.push(ChunkJob {
                kind: ChunkKind::Click(idx),
                start: pos,
                end,
                file_len: len,
            });
            pos = end;
        }
    }

    let forget_drops = total_event_bytes < 2_500_000_000;

    jobs.sort_unstable_by(|a, b| {
        b.file_len
            .cmp(&a.file_len)
            .then_with(|| a.start.cmp(&b.start))
    });

    let num_eid_buckets: usize = if total_event_bytes >= 300_000_000 {
        2048
    } else {
        256
    };
    let eid_mask = num_eid_buckets - 1;
    let ev_slab_elems: usize = if total_event_bytes >= 10_000_000 {
        65_536
    } else {
        4096
    };
    let ck_slab_elems: usize = if total_event_bytes >= 10_000_000 {
        32_768
    } else {
        4096
    };

    let nt_read = num_threads.max(1);
    let tid_bits = if nt_read <= 1 {
        0
    } else {
        (nt_read - 1).ilog2() + 1
    };
    let idx_bits = 30 - tid_bits;
    let idx_mask = (1u64 << idx_bits) - 1;
    let links_empty = link_redirect.is_empty();

    let next_job = AtomicUsize::new(0);
    let thread_outs: Vec<ReaderThreadOut> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let next_job_ref = &next_job;
        let jobs_ref = &jobs;
        let event_mmaps_ref = &event_mmaps;
        let click_mmaps_ref = &click_mmaps;
        let date_cache_ref = &date_cache;
        let products_ref = &products;
        let campaigns_ref = &campaigns;
        let overflow_ref = &overflow;
        let link_redirect_ref = &link_redirect;
        for tid in 0..nt_read {
            handles.push(s.spawn(move || {
                let mut eid_staged =
                    SlabBuckets::<RawEvent, 8, 256>::new(num_eid_buckets, ev_slab_elems, RawEvent::default());
                let mut extras: Vec<ExtraEvent> = Vec::new();
                let mut ck_compact_staged: Option<SlabBuckets<SurvClick, 8, 256>> = None;
                let mut ck_wide_staged: Option<SlabBuckets<SurvClickWide, 8, 256>> = None;

                loop {
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(job) = jobs_ref.get(jid) else {
                        break;
                    };
                    match job.kind {
                        ChunkKind::Event(fidx) => {
                            scan_csv_mmap_chunk::<7, _>(
                                &event_mmaps_ref[fidx],
                                job.start,
                                job.end,
                                |&[eid, uid, ts_b, ingest_b, etype, pid, price_b]| {
                                    if eid.is_empty() {
                                        return;
                                    }
                                    let Some((ing_val, ing_is_rel_sec)) =
                                        parse_ingest_rel_sec(ingest_b, date_cache_ref)
                                    else {
                                        return;
                                    };
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
                                        if etype == b"view" || etype == b"cart" {
                                            if let Some(ts_rel) = parse_ts_rel_ms(ts_b, date_cache_ref)
                                            {
                                                if (ts_rel as u64) < win_len_u64
                                                    && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                                                {
                                                    valid = true;
                                                    valid_ts_rel = ts_rel;
                                                }
                                            }
                                        } else if etype == b"purchase" {
                                            if let Some(p) = parse_price_cents(price_b) {
                                                if let Some(ts_rel) =
                                                    parse_ts_rel_ms(ts_b, date_cache_ref)
                                                {
                                                    if (ts_rel as u64) < win_len_u64
                                                        && ingest_rel_ms - ts_rel <= LATE_LIMIT_MS
                                                    {
                                                        valid = true;
                                                        valid_ts_rel = ts_rel;
                                                        price_cents = p;
                                                        cat_id = products_ref.lookup(pid);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let eid_k = overflow_ref.encode(eid);
                                    let uid_k = if valid { overflow_ref.encode(uid) } else { 0 };
                                    let in_sec = if ing_is_rel_sec {
                                        if ing_val > (i32::MIN as i64)
                                            && ing_val <= (i32::MAX as i64)
                                        {
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
                                                &mut extras,
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
                                    } else if use_compact_ts
                                        && price_cents == 0
                                        && in_sec.is_some()
                                    {
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
                                            &mut extras,
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
                                },
                            );
                        }
                        ChunkKind::Click(fidx) => {
                            let mut ck_buf = [PendingClick::default(); 32];
                            let mut ck_len = 0usize;
                            scan_csv_mmap_chunk::<4, _>(
                                &click_mmaps_ref[fidx],
                                job.start,
                                job.end,
                                |&[cid, uid, camp_b, ts_b]| {
                                    if cid.is_empty() || uid.is_empty() {
                                        return;
                                    }
                                    let Some(camp_idx) = campaigns_ref.lookup(camp_b) else {
                                        return;
                                    };
                                    let Some(ts_rel) = parse_ts_rel_ms(ts_b, date_cache_ref)
                                    else {
                                        return;
                                    };
                                    if ts_rel < -CLICK_REACH_MS || ts_rel >= win_len_i64 {
                                        return;
                                    }
                                    let uid_k = overflow_ref.encode(uid);
                                    let cid_k = overflow_ref.encode(cid);
                                    if links_empty {
                                        let pb = (mix64(uid_k) as usize) & PERSON_MASK;
                                        if use_compact_ts {
                                            let out = ck_compact_staged.get_or_insert_with(|| {
                                                SlabBuckets::new(
                                                    PERSON_BUCKETS,
                                                    ck_slab_elems,
                                                    SurvClick::default(),
                                                )
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
                                                SlabBuckets::new(
                                                    PERSON_BUCKETS,
                                                    ck_slab_elems,
                                                    SurvClickWide::default(),
                                                )
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
                                        ck_buf[ck_len] = PendingClick {
                                            uid_k,
                                            cid_k,
                                            ts_rel,
                                            camp_idx,
                                        };
                                        ck_len += 1;
                                        if ck_len == 32 {
                                            flush_click_batch(
                                                &ck_buf,
                                                link_redirect_ref,
                                                use_compact_ts,
                                                &mut ck_compact_staged,
                                                &mut ck_wide_staged,
                                                ck_slab_elems,
                                                win_start,
                                            );
                                            ck_len = 0;
                                        }
                                    }
                                },
                            );
                            if ck_len > 0 {
                                flush_click_batch(
                                    &ck_buf[..ck_len],
                                    link_redirect_ref,
                                    use_compact_ts,
                                    &mut ck_compact_staged,
                                    &mut ck_wide_staged,
                                    ck_slab_elems,
                                    win_start,
                                );
                            }
                        }
                    }
                }

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
    discard_mem(event_mmaps, forget_drops);
    discard_mem(click_mmaps, forget_drops);
    discard_mem(jobs, forget_drops);
    let dt_read = t_read.elapsed();

    let mut all_raw_slabs: Vec<Vec<Vec<std::mem::MaybeUninit<RawEvent>>>> =
        Vec::with_capacity(nt_read);
    let mut eid_runs: Vec<Vec<(u8, u16, u32, u16)>> = (0..num_eid_buckets)
        .map(|_| Vec::with_capacity(nt_read))
        .collect();
    let mut all_ck_compact_slabs: Vec<Vec<Vec<std::mem::MaybeUninit<SurvClick>>>> =
        if use_compact_ts {
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
    let mut all_ck_wide_slabs: Vec<Vec<Vec<std::mem::MaybeUninit<SurvClickWide>>>> =
        if !use_compact_ts {
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

    // Stage 3: ev_dedup -> resolve LinkRedirect (batched MLP prefetch) -> route to person buckets
    let t_dedup = Instant::now();
    let next_eb = AtomicUsize::new(0);
    let surv_slab_elems: usize = if total_event_bytes >= 10_000_000 {
        65_536
    } else {
        4096
    };

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

        for _ in 0..nt_read {
            handles.push(s.spawn(move || {
                let mut slots: Vec<u32> = Vec::new();
                let mut kept: Vec<RawEvent> = Vec::new();
                let mut ev_compact = if use_compact_ts {
                    Some(SlabBuckets::<SurvEvent, 8, 256>::new(
                        PERSON_BUCKETS,
                        surv_slab_elems,
                        SurvEvent::default(),
                    ))
                } else {
                    None
                };
                let mut ev_wide = if !use_compact_ts {
                    Some(SlabBuckets::<SurvEventWide, 8, 256>::new(
                        PERSON_BUCKETS,
                        surv_slab_elems,
                        SurvEventWide::default(),
                    ))
                } else {
                    None
                };

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
                    kept.clear();

                    if total_recs < 65_535 {
                        // 16-bit tag (bits 16..32), 16-bit idx+1 (bits 0..16), pos uses bits >> 16 (disjoint from bucket bits)
                        let cap = ((total_recs * 3) / 2).max(total_recs + 16).next_power_of_two().max(64);
                        if slots.len() < cap {
                            slots.resize(cap, 0);
                        }
                        slots[..cap].fill(0);
                        let mask = cap - 1;

                        for &(rtid, sidx, start, len) in runs {
                            let chunk = unsafe {
                                SlabBucketsOut::get_slice(all_raw_slabs_ref, rtid, sidx, start, len)
                            };
                            for &rec in chunk {
                                let h = mix64(rec.eid);
                                let tag = (h >> 48) as u32;
                                let mut pos = ((h >> 16) as usize) & mask;
                                loop {
                                    let slot = unsafe { *slots.get_unchecked(pos) };
                                    if slot == 0 {
                                        let idx = kept.len();
                                        kept.push(rec);
                                        unsafe {
                                            *slots.get_unchecked_mut(pos) =
                                                (tag << 16) | ((idx as u32) + 1);
                                        }
                                        break;
                                    }
                                    if (slot >> 16) == tag {
                                        let kidx = ((slot & 0xFFFF) - 1) as usize;
                                        if unsafe { kept.get_unchecked(kidx) }.eid == rec.eid {
                                            if cmp_ingest(
                                                &rec,
                                                &kept[kidx],
                                                all_extras_ref,
                                                win_start,
                                                idx_bits,
                                                idx_mask,
                                            ) == Ordering::Less
                                            {
                                                kept[kidx] = rec;
                                            }
                                            break;
                                        }
                                    }
                                    pos = (pos + 1) & mask;
                                }
                            }
                        }
                    } else if total_recs < 1_000_000 {
                        let cap = ((total_recs * 3) / 2).max(total_recs + 16).next_power_of_two().max(64);
                        if slots.len() < cap {
                            slots.resize(cap, 0);
                        }
                        slots[..cap].fill(0);
                        let mask = cap - 1;

                        for &(rtid, sidx, start, len) in runs {
                            let chunk = unsafe {
                                SlabBucketsOut::get_slice(all_raw_slabs_ref, rtid, sidx, start, len)
                            };
                            for &rec in chunk {
                                let h = mix64(rec.eid);
                                let tag = (h >> 52) as u32;
                                let mut pos = ((h >> 16) as usize) & mask;
                                loop {
                                    let slot = unsafe { *slots.get_unchecked(pos) };
                                    if slot == 0 {
                                        let idx = kept.len();
                                        kept.push(rec);
                                        unsafe {
                                            *slots.get_unchecked_mut(pos) =
                                                (tag << 20) | ((idx as u32) + 1);
                                        }
                                        break;
                                    }
                                    if (slot >> 20) == tag {
                                        let kidx = ((slot & 0x000F_FFFF) - 1) as usize;
                                        if unsafe { kept.get_unchecked(kidx) }.eid == rec.eid {
                                            if cmp_ingest(
                                                &rec,
                                                &kept[kidx],
                                                all_extras_ref,
                                                win_start,
                                                idx_bits,
                                                idx_mask,
                                            ) == Ordering::Less
                                            {
                                                kept[kidx] = rec;
                                            }
                                            break;
                                        }
                                    }
                                    pos = (pos + 1) & mask;
                                }
                            }
                        }
                    } else {
                        let mut ht: HashTable<usize> = HashTable::with_capacity(total_recs);
                        for &(rtid, sidx, start, len) in runs {
                            let chunk = unsafe {
                                SlabBucketsOut::get_slice(all_raw_slabs_ref, rtid, sidx, start, len)
                            };
                            for &rec in chunk {
                                let h = mix64(rec.eid);
                                if let Some(&kidx) =
                                    ht.find(h, |&idx| kept[idx].eid == rec.eid)
                                {
                                    if cmp_ingest(
                                        &rec,
                                        &kept[kidx],
                                        all_extras_ref,
                                        win_start,
                                        idx_bits,
                                        idx_mask,
                                    ) == Ordering::Less
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
                        for &rec in &kept {
                            if (rec.packed >> 62) == 2 {
                                continue;
                            }
                            push_surv_event(
                                rec,
                                rec.uid,
                                all_extras_ref,
                                win_start,
                                idx_bits,
                                idx_mask,
                                &mut ev_compact,
                                &mut ev_wide,
                            );
                        }
                    } else {
                        for batch in kept.chunks(32) {
                            let mut slot_idxs = [usize::MAX; 32];
                            for (j, rec) in batch.iter().enumerate() {
                                if (rec.packed >> 62) != 2 {
                                    if let Some(sidx) = link_redirect_ref.bloom_hit_slot(rec.uid) {
                                        link_redirect_ref.prefetch_slot(sidx);
                                        slot_idxs[j] = sidx;
                                    }
                                }
                            }
                            for (j, &rec) in batch.iter().enumerate() {
                                if (rec.packed >> 62) == 2 {
                                    continue;
                                }
                                let sidx = slot_idxs[j];
                                let person = if sidx == usize::MAX {
                                    rec.uid
                                } else {
                                    link_redirect_ref.resolve_from_slot(rec.uid, sidx)
                                };
                                push_surv_event(
                                    rec,
                                    person,
                                    all_extras_ref,
                                    win_start,
                                    idx_bits,
                                    idx_mask,
                                    &mut ev_compact,
                                    &mut ev_wide,
                                );
                            }
                        }
                    }
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
    discard_mem(eid_runs, forget_drops);
    discard_mem(all_raw_slabs, forget_drops);
    discard_mem(all_extras, forget_drops);
    discard_mem(link_redirect, forget_drops);
    let dt_dedup = t_dedup.elapsed();

    // Stage 4: Sessionize, attribute, write user_daily shards & campaign_daily
    let t_sess = Instant::now();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_daily_dir).expect("mkdir campaign_daily");

    let (camp_stores, wrote_any_user) = if use_compact_ts {
        let mut all_ev_slabs: Vec<Vec<Vec<std::mem::MaybeUninit<SurvEvent>>>> =
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
        )
    } else {
        let mut all_ev_slabs: Vec<Vec<Vec<std::mem::MaybeUninit<SurvEventWide>>>> =
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
        let mut merged = vec![CampAgg::default(); total_slots];
        for st in &camp_stores {
            if let CampStore::Flat { data, .. } = st {
                for (i, agg) in data.iter().enumerate() {
                    if agg.sessions != 0 || agg.assisted_sessions != 0 {
                        merged[i].sessions += agg.sessions;
                        merged[i].purchasing_sessions += agg.purchasing_sessions;
                        merged[i].revenue_cents += agg.revenue_cents;
                        merged[i].assisted_sessions += agg.assisted_sessions;
                    }
                }
            }
        }
        for c in 0..num_camps {
            for d in 0..day_span {
                let agg = merged[c * day_span + d];
                if agg.sessions > 0 || agg.assisted_sessions > 0 {
                    cd_ids.push(&campaigns.ids[c]);
                    cd_channels.push(&campaigns.channels[c]);
                    cd_days.push(min_day + d as i32);
                    cd_sessions.push(agg.sessions);
                    cd_purchasing.push(agg.purchasing_sessions);
                    cd_revenues.push(agg.revenue_cents);
                    cd_assisted.push(agg.assisted_sessions);
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
