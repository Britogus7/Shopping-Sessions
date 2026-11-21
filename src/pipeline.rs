use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as MemOrdering};
use std::time::Instant;

use ahash::RandomState;
use arrow_array::builder::StringBuilder;
use arrow_array::{Date32Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hashbrown::{HashMap, HashTable};
use memmap2::{Mmap, MmapOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::csv::{
    for_each_data_line, parse_csv_line, parse_price_cents, parse_ts, parse_ts_fast,
    scan_csv_mmap_chunk, WindowDateTable, CLICK_REACH_MS, DAY_MS, LATE_LIMIT_MS, SESSION_GAP_MS,
};
use crate::links::{build_link_redirect, LinkRedirect};
use crate::pack::{mix64, OverflowInterner, OVERFLOW_BIT};
use std::sync::Arc;

const PERSON_BUCKETS: usize = 512;
const PERSON_MASK: usize = PERSON_BUCKETS - 1;
const BATCH_ROWS: usize = 65_536;

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
        let mut raw_prods: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut cats_set: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
        let mut max_p = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                if !pid.is_empty() && !cat.is_empty() {
                    if let Some(n) = parse_p_num(pid) {
                        if n > max_p {
                            max_p = n;
                        }
                    }
                    raw_prods.push((pid.to_vec(), cat.to_vec()));
                    cats_set.push(cat.to_vec());
                }
            }
        });
        cats_set.sort_unstable();
        cats_set.dedup();

        let unknown_cat = cats_set
            .binary_search(&b"unknown".to_vec())
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
            let cid = cats_set.binary_search(&cat).unwrap() as u32;
            if use_direct {
                if let Some(n) = parse_p_num(&pid) {
                    if n < direct.len() {
                        direct[n] = cid as u16;
                        continue;
                    }
                }
            }
            let idx = spans.len() as u32;
            let off = arena.len() as u32;
            arena.extend_from_slice(&pid);
            spans.push((off, pid.len() as u32));
            prod_cat.push(cid);
            let h = hasher.hash_one(&pid);
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
        let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut max_c = 0usize;

        for_each_data_line(&data, true, |line| {
            if let Some([cid, ch]) = parse_csv_line::<2>(line) {
                if !cid.is_empty() {
                    if let Some(n) = parse_c_num(cid) {
                        if n > max_c {
                            max_c = n;
                        }
                    }
                    raw.push((cid.to_vec(), ch.to_vec()));
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
            ids.push(String::from_utf8_lossy(&cid).into_owned());
            channels.push(String::from_utf8_lossy(&ch).into_owned());
            if let Some(n) = parse_c_num(&cid) {
                if n < direct.len() {
                    direct[n] = camp_idx;
                    continue;
                }
            }
            has_fallback = true;
            let h = hasher.hash_one(&cid);
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
// Contiguous L2 Staging Buffer for Low-DTLB Bucket Routing
// =========================================================================

struct StagedBuckets<T: Copy, const CAP: usize> {
    num_buckets: usize,
    staging: Vec<T>,
    counts: Vec<u16>,
    dest: Vec<Vec<T>>,
}

impl<T: Copy, const CAP: usize> StagedBuckets<T, CAP> {
    fn new(num_buckets: usize, zero: T) -> Self {
        Self {
            num_buckets,
            staging: vec![zero; num_buckets * CAP],
            counts: vec![0u16; num_buckets],
            dest: (0..num_buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[inline(always)]
    fn push(&mut self, b: usize, item: T) {
        let c = self.counts[b] as usize;
        let base = b * CAP;
        unsafe {
            *self.staging.get_unchecked_mut(base + c) = item;
        }
        let next = c + 1;
        if next == CAP {
            self.dest[b].extend_from_slice(&self.staging[base..base + CAP]);
            self.counts[b] = 0;
        } else {
            self.counts[b] = next as u16;
        }
    }

    fn finish(mut self) -> Vec<Vec<T>> {
        for b in 0..self.num_buckets {
            let c = self.counts[b] as usize;
            if c > 0 {
                let base = b * CAP;
                self.dest[b].extend_from_slice(&self.staging[base..base + c]);
            }
        }
        self.dest
    }
}

// =========================================================================
// Compact Event & Click Representations + Inline Ingest Packing
// =========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
struct RawEvent {
    eid: u64,
    uid: u64,
    packed: u64,
}

impl Default for RawEvent {
    fn default() -> Self {
        Self {
            eid: 0,
            uid: 0,
            packed: 0,
        }
    }
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
#[derive(Clone, Copy)]
struct SurvEvent {
    person: u64,
    ts_rel: u32,
    price_cat: u32, // 0 for view/cart; (cat_id << 24) | price_cents for purchase
}

impl Default for SurvEvent {
    fn default() -> Self {
        Self {
            person: 0,
            ts_rel: 0,
            price_cat: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct SurvClick {
    person: u64,
    cid: u64,
    ts_rel: u32,
    camp_idx: u32,
}

impl Default for SurvClick {
    fn default() -> Self {
        Self {
            person: 0,
            cid: 0,
            ts_rel: 0,
            camp_idx: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct SurvEventWide {
    person: u64,
    ts: i64,
    price_cents: u32,
    cat_id: u32,
}

impl Default for SurvEventWide {
    fn default() -> Self {
        Self {
            person: 0,
            ts: 0,
            price_cents: 0,
            cat_id: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct SurvClickWide {
    person: u64,
    cid: u64,
    ts: i64,
    camp_idx: u32,
}

impl Default for SurvClickWide {
    fn default() -> Self {
        Self {
            person: 0,
            cid: 0,
            ts: 0,
            camp_idx: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
    assisted_sessions: i32,
}

// =========================================================================
// Unified Session/Click Trait so Compact and Wide Paths Share Exact Logic
// =========================================================================

trait SessEvent: Copy + Send + Sync {
    fn person(&self) -> u64;
    fn ts_ms(&self, win_start: i64) -> i64;
    fn price_cat(&self) -> (i64, usize); // (0, 0) if non-purchase
    fn cmp_sort(&self, other: &Self) -> Ordering;
}

impl SessEvent for SurvEvent {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ts_ms(&self, win_start: i64) -> i64 {
        win_start + self.ts_rel as i64
    }
    #[inline(always)]
    fn price_cat(&self) -> (i64, usize) {
        if self.price_cat == 0 {
            (0, 0)
        } else {
            (
                (self.price_cat & 0x00FF_FFFF) as i64,
                (self.price_cat >> 24) as usize,
            )
        }
    }
    #[inline(always)]
    fn cmp_sort(&self, other: &Self) -> Ordering {
        self.person
            .cmp(&other.person)
            .then_with(|| self.ts_rel.cmp(&other.ts_rel))
    }
}

impl SessEvent for SurvEventWide {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ts_ms(&self, _win_start: i64) -> i64 {
        self.ts
    }
    #[inline(always)]
    fn price_cat(&self) -> (i64, usize) {
        (self.price_cents as i64, self.cat_id as usize)
    }
    #[inline(always)]
    fn cmp_sort(&self, other: &Self) -> Ordering {
        self.person
            .cmp(&other.person)
            .then_with(|| self.ts.cmp(&other.ts))
    }
}

trait SessClick: Copy + Send + Sync {
    fn person(&self) -> u64;
    fn ts_ms(&self, click_win_start: i64) -> i64;
    fn camp_idx(&self) -> u32;
    fn cmp_sort(&self, other: &Self, overflow: &OverflowInterner) -> Ordering;
}

impl SessClick for SurvClick {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ts_ms(&self, click_win_start: i64) -> i64 {
        click_win_start + self.ts_rel as i64
    }
    #[inline(always)]
    fn camp_idx(&self) -> u32 {
        self.camp_idx
    }
    #[inline(always)]
    fn cmp_sort(&self, other: &Self, overflow: &OverflowInterner) -> Ordering {
        self.person
            .cmp(&other.person)
            .then_with(|| self.ts_rel.cmp(&other.ts_rel))
            .then_with(|| overflow.cmp_key(self.cid, other.cid))
    }
}

impl SessClick for SurvClickWide {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ts_ms(&self, _click_win_start: i64) -> i64 {
        self.ts
    }
    #[inline(always)]
    fn camp_idx(&self) -> u32 {
        self.camp_idx
    }
    #[inline(always)]
    fn cmp_sort(&self, other: &Self, overflow: &OverflowInterner) -> Ordering {
        self.person
            .cmp(&other.person)
            .then_with(|| self.ts.cmp(&other.ts))
            .then_with(|| overflow.cmp_key(self.cid, other.cid))
    }
}

// =========================================================================
// Parquet Schemas & Streaming Batch Writers
// =========================================================================

fn user_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int32, false),
        Field::new("events", DataType::Int32, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn campaign_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int32, false),
        Field::new("purchasing_sessions", DataType::Int32, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int32, false),
    ]))
}

fn fast_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_size(256 * 1024)
        .build()
}

struct UserDailyBatch {
    uid: StringBuilder,
    days: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenues: Vec<i64>,
    top_cats: StringBuilder,
}

impl UserDailyBatch {
    fn new() -> Self {
        Self {
            uid: StringBuilder::with_capacity(BATCH_ROWS, BATCH_ROWS * 12),
            days: Vec::with_capacity(BATCH_ROWS),
            sessions: Vec::with_capacity(BATCH_ROWS),
            events: Vec::with_capacity(BATCH_ROWS),
            revenues: Vec::with_capacity(BATCH_ROWS),
            top_cats: StringBuilder::with_capacity(BATCH_ROWS, BATCH_ROWS * 10),
        }
    }

    #[inline(always)]
    fn push(
        &mut self,
        user_id: &str,
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat: Option<&str>,
    ) {
        self.uid.append_value(user_id);
        self.days.push(day);
        self.sessions.push(sessions);
        self.events.push(events);
        self.revenues.push(revenue);
        match top_cat {
            Some(c) => self.top_cats.append_value(c),
            None => self.top_cats.append_null(),
        }
    }

    fn flush(
        &mut self,
        writer: &mut Option<ArrowWriter<BufWriter<File>>>,
        out_path: &Path,
        schema: &Arc<Schema>,
    ) {
        if self.days.is_empty() {
            return;
        }
        let w = writer.get_or_insert_with(|| {
            let file =
                BufWriter::with_capacity(1 << 20, File::create(out_path).expect("create user_daily"));
            ArrowWriter::try_new(file, schema.clone(), Some(fast_writer_props()))
                .expect("arrow writer")
        });
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    std::mem::replace(
                        &mut self.uid,
                        StringBuilder::with_capacity(BATCH_ROWS, BATCH_ROWS * 12),
                    )
                    .finish(),
                ),
                Arc::new(Date32Array::from(std::mem::replace(
                    &mut self.days,
                    Vec::with_capacity(BATCH_ROWS),
                ))),
                Arc::new(Int32Array::from(std::mem::replace(
                    &mut self.sessions,
                    Vec::with_capacity(BATCH_ROWS),
                ))),
                Arc::new(Int32Array::from(std::mem::replace(
                    &mut self.events,
                    Vec::with_capacity(BATCH_ROWS),
                ))),
                Arc::new(Int64Array::from(std::mem::replace(
                    &mut self.revenues,
                    Vec::with_capacity(BATCH_ROWS),
                ))),
                Arc::new(
                    std::mem::replace(
                        &mut self.top_cats,
                        StringBuilder::with_capacity(BATCH_ROWS, BATCH_ROWS * 10),
                    )
                    .finish(),
                ),
            ],
        )
        .expect("record batch");
        w.write(&batch).expect("write batch");
    }
}

// =========================================================================
// Phase D Generic Sessionizer + Click Attributor + Shard Writer
// =========================================================================

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

fn process_person_buckets<E: SessEvent, C: SessClick>(
    num_threads: usize,
    bucket_ev_chunks: &mut [Vec<Vec<E>>],
    bucket_ck_chunks: &mut [Vec<Vec<C>>],
    overflow: &OverflowInterner,
    products: &ProductIndex,
    campaigns: &CampaignIndex,
    win_start: i64,
    click_win_start: i64,
    min_day: i32,
    day_span: usize,
    user_daily_dir: &Path,
) -> (Vec<CampStore>, bool) {
    let nt = num_threads.max(1);
    let next_pb = AtomicUsize::new(0);
    let num_cats = products.categories.len();
    let num_camps = campaigns.ids.len();
    let use_flat = day_span <= 128 && num_camps <= 20_000;
    let ud_schema = user_daily_schema();

    // Wrap mutable bucket slices in Mutex<Option<...>> for zero-unsafe disjoint extraction.
    let ev_cells: Vec<std::sync::Mutex<Vec<Vec<E>>>> = bucket_ev_chunks
        .iter_mut()
        .map(|v| std::sync::Mutex::new(std::mem::take(v)))
        .collect();
    let ck_cells: Vec<std::sync::Mutex<Vec<Vec<C>>>> = bucket_ck_chunks
        .iter_mut()
        .map(|v| std::sync::Mutex::new(std::mem::take(v)))
        .collect();

    let results: Vec<(CampStore, bool)> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let next_pb_ref = &next_pb;
        let ev_cells_ref = &ev_cells;
        let ck_cells_ref = &ck_cells;
        let ud_schema_ref = &ud_schema;
        for tid in 0..nt {
            handles.push(s.spawn(move || {
                let mut camp_store = CampStore::new(use_flat, num_camps, min_day, day_span);
                let mut writer: Option<ArrowWriter<BufWriter<File>>> = None;
                let part_path = user_daily_dir.join(format!("part-{tid:05}.parquet"));
                let mut batch = UserDailyBatch::new();

                let mut evs: Vec<E> = Vec::new();
                let mut cks: Vec<C> = Vec::new();
                let mut camp_in_win: Vec<u32> = vec![0u32; num_camps];
                let mut active_camps: Vec<u32> = Vec::with_capacity(32);
                let mut cat_rev: Vec<i64> = vec![0i64; num_cats];
                let mut cat_dirty: Vec<usize> = Vec::with_capacity(16);

                let mut buf9 = [0u8; 9];
                let mut tmp_vec: Vec<u8> = Vec::with_capacity(64);

                loop {
                    let pb = next_pb_ref.fetch_add(1, MemOrdering::Relaxed);
                    if pb >= PERSON_BUCKETS {
                        break;
                    }
                    let my_ev_chunks = { std::mem::take(&mut *ev_cells_ref[pb].lock().unwrap()) };
                    let my_ck_chunks = { std::mem::take(&mut *ck_cells_ref[pb].lock().unwrap()) };

                    let total_ev: usize = my_ev_chunks.iter().map(|c| c.len()).sum();
                    if total_ev == 0 {
                        drop(my_ck_chunks);
                        continue;
                    }
                    evs.clear();
                    evs.reserve(total_ev);
                    for c in my_ev_chunks {
                        evs.extend_from_slice(&c);
                    }
                    let total_ck: usize = my_ck_chunks.iter().map(|c| c.len()).sum();
                    cks.clear();
                    if total_ck > 0 {
                        cks.reserve(total_ck);
                        for c in my_ck_chunks {
                            cks.extend_from_slice(&c);
                        }
                    }

                    evs.sort_unstable_by(|a, b| a.cmp_sort(b));
                    if cks.len() > 1 {
                        cks.sort_unstable_by(|a, b| a.cmp_sort(b, overflow));
                    }

                    let mut ei = 0usize;
                    let mut ci = 0usize;
                    while ei < evs.len() {
                        let p_key = evs[ei].person();
                        let ev_s = ei;
                        ei += 1;
                        while ei < evs.len() && evs[ei].person() == p_key {
                            ei += 1;
                        }
                        let p_evs = &evs[ev_s..ei];

                        while ci < cks.len() && cks[ci].person() < p_key {
                            ci += 1;
                        }
                        let ck_s = ci;
                        while ci < cks.len() && cks[ci].person() == p_key {
                            ci += 1;
                        }
                        let p_cks = &cks[ck_s..ci];

                        let pname_bytes = overflow.decode_with(p_key, &mut buf9, &mut tmp_vec);
                        let pname_cow;
                        let pname: &str = if (p_key & OVERFLOW_BIT) == 0 {
                            unsafe { std::str::from_utf8_unchecked(pname_bytes) }
                        } else {
                            pname_cow = String::from_utf8_lossy(pname_bytes);
                            pname_cow.as_ref()
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
                            let sess_start = p_evs[i].ts_ms(win_start);
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
                                        Some(products.categories[best_cid].as_str())
                                    };
                                    batch.push(pname, cur_day, d_sessions, d_events, d_rev, top_cat);
                                    if batch.days.len() >= BATCH_ROWS {
                                        batch.flush(&mut writer, &part_path, ud_schema_ref);
                                    }
                                }
                                cur_day = sess_day;
                                d_sessions = 0;
                                d_events = 0;
                                d_rev = 0;
                            }

                            let mut sess_events = 1i32;
                            let mut sess_rev = 0i64;
                            let (p0, c0) = p_evs[i].price_cat();
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
                                let next_ts = p_evs[j].ts_ms(win_start);
                                if next_ts - prev_ts > SESSION_GAP_MS {
                                    break;
                                }
                                prev_ts = next_ts;
                                sess_events += 1;
                                let (pj, cj) = p_evs[j].price_cat();
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
                                while ck_right < p_cks.len()
                                    && p_cks[ck_right].ts_ms(click_win_start) <= sess_start
                                {
                                    let c = p_cks[ck_right].camp_idx();
                                    if camp_in_win[c as usize] == 0 {
                                        active_camps.push(c);
                                    }
                                    camp_in_win[c as usize] += 1;
                                    ck_right += 1;
                                }
                                let min_ck_ts = sess_start - CLICK_REACH_MS;
                                while ck_left < ck_right
                                    && p_cks[ck_left].ts_ms(click_win_start) < min_ck_ts
                                {
                                    let c = p_cks[ck_left].camp_idx();
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
                                let max_ts = p_cks[ck_right - 1].ts_ms(click_win_start);
                                let mut k = ck_right - 1;
                                while k > ck_left
                                    && p_cks[k - 1].ts_ms(click_win_start) == max_ts
                                {
                                    k -= 1;
                                }
                                let win_camp = p_cks[k].camp_idx();
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
                                Some(products.categories[best_cid].as_str())
                            };
                            batch.push(pname, cur_day, d_sessions, d_events, d_rev, top_cat);
                            if batch.days.len() >= BATCH_ROWS {
                                batch.flush(&mut writer, &part_path, ud_schema_ref);
                            }
                        }

                        for &c in &active_camps {
                            camp_in_win[c as usize] = 0;
                        }
                        active_camps.clear();
                    }
                }

                batch.flush(&mut writer, &part_path, ud_schema_ref);
                let wrote = if let Some(w) = writer {
                    w.close().expect("close user_daily parquet");
                    true
                } else {
                    false
                };
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

#[derive(Default)]
struct ReaderThreadOut {
    eid_buckets: Vec<Vec<RawEvent>>,
    extras: Vec<ExtraEvent>,
    ck_compact: Vec<Vec<SurvClick>>,
    ck_wide: Vec<Vec<SurvClickWide>>,
}

pub fn run_pipeline(data_dir: &Path, out_dir: &Path, num_threads: usize) {
    let t_total = Instant::now();
    let (win_start, win_end) = parse_manifest(&data_dir.join("manifest.json"));
    let click_win_start = win_start - CLICK_REACH_MS;
    let date_table = WindowDateTable::new(win_start);

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
        && (win_end - win_start) <= 2_000_000_000
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

    // Largest files first so tail chunks finish tightly together
    jobs.sort_unstable_by(|a, b| {
        b.file_len
            .cmp(&a.file_len)
            .then_with(|| a.start.cmp(&b.start))
    });

    let num_eid_buckets: usize = if total_event_bytes >= 3_000_000_000 {
        1024
    } else if total_event_bytes >= 300_000_000 {
        512
    } else {
        256
    };
    let eid_mask = num_eid_buckets - 1;

    let nt_read = num_threads.max(1);
    let tid_bits = if nt_read <= 1 {
        0
    } else {
        (nt_read - 1).ilog2() + 1
    };
    let idx_bits = 30 - tid_bits;
    let idx_mask = (1u64 << idx_bits) - 1;

    let next_job = AtomicUsize::new(0);
    let thread_outs: Vec<ReaderThreadOut> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt_read);
        let next_job_ref = &next_job;
        let jobs_ref = &jobs;
        let event_mmaps_ref = &event_mmaps;
        let click_mmaps_ref = &click_mmaps;
        let date_table_ref = &date_table;
        let products_ref = &products;
        let campaigns_ref = &campaigns;
        let overflow_ref = &overflow;
        let link_redirect_ref = &link_redirect;
        for tid in 0..nt_read {
            handles.push(s.spawn(move || {
                let mut eid_staged =
                    StagedBuckets::<RawEvent, 16>::new(num_eid_buckets, RawEvent::default());
                let mut extras: Vec<ExtraEvent> = Vec::new();
                let mut ck_compact_staged = if use_compact_ts {
                    Some(StagedBuckets::<SurvClick, 16>::new(
                        PERSON_BUCKETS,
                        SurvClick::default(),
                    ))
                } else {
                    None
                };
                let mut ck_wide_staged = if !use_compact_ts {
                    Some(StagedBuckets::<SurvClickWide, 16>::new(
                        PERSON_BUCKETS,
                        SurvClickWide::default(),
                    ))
                } else {
                    None
                };

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
                                |[eid, uid, ts_b, ingest_b, etype, pid, price_b]| {
                                    if eid.is_empty() {
                                        return;
                                    }
                                    let Some(ingest_ts) = parse_ts_fast(ingest_b, date_table_ref)
                                    else {
                                        return;
                                    };

                                    let mut valid = false;
                                    let mut valid_ts = 0i64;
                                    let mut price_cents = 0u32;
                                    let mut cat_id = 0u32;

                                    if !uid.is_empty() && !pid.is_empty() {
                                        if let Some(ts) = parse_ts_fast(ts_b, date_table_ref) {
                                            if ts >= win_start
                                                && ts < win_end
                                                && ingest_ts - ts <= LATE_LIMIT_MS
                                            {
                                                if etype == b"view" || etype == b"cart" {
                                                    valid = true;
                                                    valid_ts = ts;
                                                } else if etype == b"purchase" {
                                                    if let Some(p) = parse_price_cents(price_b) {
                                                        valid = true;
                                                        valid_ts = ts;
                                                        price_cents = p;
                                                        cat_id = products_ref.lookup(pid);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let eid_k = overflow_ref.encode(eid);
                                    let uid_k = if valid { overflow_ref.encode(uid) } else { 0 };
                                    let in_sec = inline_ingest_sec(ingest_ts, win_start);

                                    let packed = if !valid {
                                        if let Some(sec) = in_sec {
                                            (2u64 << 62) | ((sec as u32) as u64)
                                        } else {
                                            let eidx = extras.len();
                                            extras.push(ExtraEvent {
                                                ts: 0,
                                                ingest_ts,
                                                price_cents: 0,
                                                cat_id: 0,
                                            });
                                            let eref = ((tid as u64) << idx_bits) | (eidx as u64);
                                            (2u64 << 62)
                                                | (eref << 32)
                                                | ((i32::MIN as u32) as u64)
                                        }
                                    } else if use_compact_ts
                                        && price_cents == 0
                                        && in_sec.is_some()
                                    {
                                        let ts_rel = (valid_ts - win_start) as u64;
                                        let sec = in_sec.unwrap();
                                        (ts_rel << 32) | ((sec as u32) as u64)
                                    } else {
                                        let eidx = extras.len();
                                        extras.push(ExtraEvent {
                                            ts: valid_ts,
                                            ingest_ts,
                                            price_cents,
                                            cat_id,
                                        });
                                        let eref = ((tid as u64) << idx_bits) | (eidx as u64);
                                        let lo = match in_sec {
                                            Some(sec) => (sec as u32) as u64,
                                            None => (i32::MIN as u32) as u64,
                                        };
                                        (3u64 << 62) | (eref << 32) | lo
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
                            scan_csv_mmap_chunk::<4, _>(
                                &click_mmaps_ref[fidx],
                                job.start,
                                job.end,
                                |[cid, uid, camp_b, ts_b]| {
                                    if cid.is_empty() || uid.is_empty() {
                                        return;
                                    }
                                    let Some(ts) = parse_ts_fast(ts_b, date_table_ref) else {
                                        return;
                                    };
                                    if ts < click_win_start || ts >= win_end {
                                        return;
                                    }
                                    let Some(camp_idx) = campaigns_ref.lookup(camp_b) else {
                                        return;
                                    };
                                    let uid_k = overflow_ref.encode(uid);
                                    let person = link_redirect_ref.resolve(uid_k);
                                    let cid_k = overflow_ref.encode(cid);
                                    let pb = (mix64(person) as usize) & PERSON_MASK;
                                    if let Some(ref mut cks) = ck_compact_staged {
                                        cks.push(
                                            pb,
                                            SurvClick {
                                                person,
                                                cid: cid_k,
                                                ts_rel: (ts - click_win_start) as u32,
                                                camp_idx,
                                            },
                                        );
                                    } else if let Some(ref mut cks) = ck_wide_staged {
                                        cks.push(
                                            pb,
                                            SurvClickWide {
                                                person,
                                                cid: cid_k,
                                                ts,
                                                camp_idx,
                                            },
                                        );
                                    }
                                },
                            );
                        }
                    }
                }

                ReaderThreadOut {
                    eid_buckets: eid_staged.finish(),
                    extras,
                    ck_compact: ck_compact_staged.map_or_else(Vec::new, |s| s.finish()),
                    ck_wide: ck_wide_staged.map_or_else(Vec::new, |s| s.finish()),
                }
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(event_mmaps);
    drop(click_mmaps);
    drop(jobs);
    let dt_read = t_read.elapsed();

    // Transpose into per-bucket chunk lists so Stage 3 & Stage 4 own disjoint memory cleanly
    let mut eid_bucket_chunks: Vec<Vec<Vec<RawEvent>>> = (0..num_eid_buckets)
        .map(|_| Vec::with_capacity(nt_read))
        .collect();
    let mut ck_compact_buckets: Vec<Vec<Vec<SurvClick>>> = if use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut ck_wide_buckets: Vec<Vec<Vec<SurvClickWide>>> = if !use_compact_ts {
        (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect()
    } else {
        Vec::new()
    };
    let mut all_extras: Vec<Vec<ExtraEvent>> = Vec::with_capacity(nt_read);

    for mut tout in thread_outs {
        all_extras.push(std::mem::take(&mut tout.extras));
        for (b, chunk) in tout.eid_buckets.into_iter().enumerate() {
            if !chunk.is_empty() {
                eid_bucket_chunks[b].push(chunk);
            }
        }
        if use_compact_ts {
            for (pb, chunk) in tout.ck_compact.into_iter().enumerate() {
                if !chunk.is_empty() {
                    ck_compact_buckets[pb].push(chunk);
                }
            }
        } else {
            for (pb, chunk) in tout.ck_wide.into_iter().enumerate() {
                if !chunk.is_empty() {
                    ck_wide_buckets[pb].push(chunk);
                }
            }
        }
    }

    // Stage 3: ev_dedup -> resolve LinkRedirect -> route surviving events to 256 person buckets
    let t_dedup = Instant::now();
    let eid_cells: Vec<std::sync::Mutex<Vec<Vec<RawEvent>>>> = eid_bucket_chunks
        .into_iter()
        .map(std::sync::Mutex::new)
        .collect();
    let next_eb = AtomicUsize::new(0);

    let dedup_thread_outs: Vec<(Vec<Vec<SurvEvent>>, Vec<Vec<SurvEventWide>>)> =
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(nt_read);
            for _ in 0..nt_read {
                handles.push(s.spawn(|| {
                    let mut slots: Vec<u32> = Vec::new();
                    let mut kept: Vec<RawEvent> = Vec::new();
                    let mut ev_compact = if use_compact_ts {
                        Some(StagedBuckets::<SurvEvent, 16>::new(
                            PERSON_BUCKETS,
                            SurvEvent::default(),
                        ))
                    } else {
                        None
                    };
                    let mut ev_wide = if !use_compact_ts {
                        Some(StagedBuckets::<SurvEventWide, 16>::new(
                            PERSON_BUCKETS,
                            SurvEventWide::default(),
                        ))
                    } else {
                        None
                    };

                    loop {
                        let b = next_eb.fetch_add(1, MemOrdering::Relaxed);
                        if b >= num_eid_buckets {
                            break;
                        }
                        let chunks = std::mem::take(&mut *eid_cells[b].lock().unwrap());
                        if chunks.is_empty() {
                            continue;
                        }
                        let total_recs: usize = chunks.iter().map(|c| c.len()).sum();
                        kept.clear();

                        if total_recs < 1_000_000 {
                            let cap = (total_recs * 2).next_power_of_two().max(64);
                            if slots.len() < cap {
                                slots.resize(cap, 0);
                            }
                            slots[..cap].fill(0);
                            let mask = cap - 1;

                            for chunk in chunks {
                                for &rec in &chunk {
                                    let h = mix64(rec.eid);
                                    let tag = ((h >> 32) as u32) << 20;
                                    let mut pos = (h as usize) & mask;
                                    loop {
                                        let slot = unsafe { *slots.get_unchecked(pos) };
                                        if slot == 0 {
                                            let idx = kept.len();
                                            kept.push(rec);
                                            unsafe {
                                                *slots.get_unchecked_mut(pos) =
                                                    tag | ((idx as u32) + 1);
                                            }
                                            break;
                                        }
                                        if (slot & 0xFFF0_0000) == tag {
                                            let kidx = ((slot & 0x000F_FFFF) - 1) as usize;
                                            if kept[kidx].eid == rec.eid {
                                                if cmp_ingest(
                                                    &rec,
                                                    &kept[kidx],
                                                    &all_extras,
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
                            // Overflow fallback if a bucket ever gets >= 1M records
                            let mut ht: HashTable<usize> = HashTable::with_capacity(total_recs);
                            for chunk in chunks {
                                for &rec in &chunk {
                                    let h = mix64(rec.eid);
                                    if let Some(&kidx) =
                                        ht.find(h, |&idx| kept[idx].eid == rec.eid)
                                    {
                                        if cmp_ingest(
                                            &rec,
                                            &kept[kidx],
                                            &all_extras,
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

                        for &rec in &kept {
                            let kind = rec.packed >> 62;
                            if kind == 2 {
                                continue;
                            }
                            let person = link_redirect.resolve(rec.uid);
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
                                let eref = (rec.packed >> 32) & ((1u64 << 30) - 1);
                                let tid = (eref >> idx_bits) as usize;
                                let eidx = (eref & idx_mask) as usize;
                                let ex = all_extras[tid][eidx];
                                out.push(
                                    pb,
                                    SurvEventWide {
                                        person,
                                        ts: ex.ts,
                                        price_cents: ex.price_cents,
                                        cat_id: ex.cat_id,
                                    },
                                );
                            }
                        }
                    }

                    (
                        ev_compact.map_or_else(Vec::new, |s| s.finish()),
                        ev_wide.map_or_else(Vec::new, |s| s.finish()),
                    )
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
    drop(eid_cells);
    drop(all_extras);
    drop(link_redirect);
    let dt_dedup = t_dedup.elapsed();

    // Stage 4: Sessionize, attribute, write user_daily shards & campaign_daily
    let t_sess = Instant::now();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_daily_dir).expect("mkdir campaign_daily");

    let (camp_stores, wrote_any_user) = if use_compact_ts {
        let mut ev_buckets: Vec<Vec<Vec<SurvEvent>>> = (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect();
        for (eb, _) in dedup_thread_outs {
            for (pb, chunk) in eb.into_iter().enumerate() {
                if !chunk.is_empty() {
                    ev_buckets[pb].push(chunk);
                }
            }
        }
        process_person_buckets(
            nt_read,
            &mut ev_buckets,
            &mut ck_compact_buckets,
            &overflow,
            &products,
            &campaigns,
            win_start,
            click_win_start,
            min_day,
            day_span,
            &user_daily_dir,
        )
    } else {
        let mut ev_buckets: Vec<Vec<Vec<SurvEventWide>>> = (0..PERSON_BUCKETS)
            .map(|_| Vec::with_capacity(nt_read))
            .collect();
        for (_, eb) in dedup_thread_outs {
            for (pb, chunk) in eb.into_iter().enumerate() {
                if !chunk.is_empty() {
                    ev_buckets[pb].push(chunk);
                }
            }
        }
        process_person_buckets(
            nt_read,
            &mut ev_buckets,
            &mut ck_wide_buckets,
            &overflow,
            &products,
            &campaigns,
            win_start,
            click_win_start,
            min_day,
            day_span,
            &user_daily_dir,
        )
    };

    if !wrote_any_user {
        let ud_schema = user_daily_schema();
        let file = BufWriter::new(
            File::create(user_daily_dir.join("part-00000.parquet")).expect("create empty ud"),
        );
        let writer = ArrowWriter::try_new(file, ud_schema, Some(fast_writer_props()))
            .expect("empty ud writer");
        writer.close().expect("close empty ud");
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
        for st in camp_stores {
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

    let cd_schema = campaign_daily_schema();
    let cd_file = BufWriter::with_capacity(
        256 * 1024,
        File::create(camp_daily_dir.join("part-00000.parquet")).expect("create cd parquet"),
    );
    let mut cd_writer =
        ArrowWriter::try_new(cd_file, cd_schema.clone(), Some(fast_writer_props()))
            .expect("cd writer");
    if !cd_ids.is_empty() {
        let batch = RecordBatch::try_new(
            cd_schema,
            vec![
                Arc::new(StringArray::from_iter_values(cd_ids.iter().copied())),
                Arc::new(StringArray::from_iter_values(cd_channels.iter().copied())),
                Arc::new(Date32Array::from(cd_days)),
                Arc::new(Int32Array::from(cd_sessions)),
                Arc::new(Int32Array::from(cd_purchasing)),
                Arc::new(Int64Array::from(cd_revenues)),
                Arc::new(Int32Array::from(cd_assisted)),
            ],
        )
        .expect("cd batch");
        cd_writer.write(&batch).expect("write cd");
    }
    cd_writer.close().expect("close cd");

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
