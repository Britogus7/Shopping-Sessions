use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ahash::RandomState;
use arrow_array::builder::StringBuilder;
use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hashbrown::{HashMap, HashTable};
use memmap2::MmapOptions;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::csv::{
    for_each_data_line, parse_csv_line, parse_price_cents, parse_ts, parse_ts_cached, DateCache,
    CLICK_REACH_MS, DAY_MS, LATE_LIMIT_MS, SESSION_GAP_MS,
};
use crate::links::{build_link_index, split_mmap_chunks, LinkIndex};

const BUCKET_BITS: usize = 6;
const NUM_BUCKETS: usize = 1 << BUCKET_BITS;
const BUCKET_MASK: usize = NUM_BUCKETS - 1;
const BATCH_CAP: usize = 8192;

struct ProductIndex {
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    prod_cat: Vec<u32>,
    pub categories: Vec<String>, // sorted byte-wise ascending!
    pub unknown_cat: u32,
}

impl ProductIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read products.csv");
        let mut raw_prods: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut cats_set: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
        for_each_data_line(&data, true, |line| {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                if !pid.is_empty() && !cat.is_empty() {
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

        let hasher = RandomState::with_seeds(11, 22, 33, 44);
        let mut table = HashTable::with_capacity(raw_prods.len());
        let mut arena = Vec::with_capacity(raw_prods.len() * 10);
        let mut spans = Vec::with_capacity(raw_prods.len());
        let mut prod_cat = Vec::with_capacity(raw_prods.len());

        for (i, (pid, cat)) in raw_prods.into_iter().enumerate() {
            let cid = cats_set.binary_search(&cat).unwrap() as u32;
            let off = arena.len() as u32;
            let len = pid.len() as u32;
            arena.extend_from_slice(&pid);
            spans.push((off, len));
            prod_cat.push(cid);
            let h = hasher.hash_one(&pid);
            let idx = i as u32;
            table.insert_unique(h, idx, |&id| {
                let (o, l) = spans[id as usize];
                hasher.hash_one(&arena[o as usize..(o + l) as usize])
            });
        }

        Self {
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
    hasher: RandomState,
    table: HashTable<u32>,
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    pub ids: Vec<String>,
    pub channels: Vec<String>,
    pub organic_id: u32,
}

impl CampaignIndex {
    fn load(path: &Path) -> Self {
        let data = fs::read(path).expect("read campaigns.csv");
        let hasher = RandomState::with_seeds(55, 66, 77, 88);
        let mut table = HashTable::new();
        let mut arena = Vec::new();
        let mut spans: Vec<(u32, u32)> = Vec::new();
        let mut ids = Vec::new();
        let mut channels = Vec::new();

        for_each_data_line(&data, true, |line| {
            if let Some([cid, ch]) = parse_csv_line::<2>(line) {
                if !cid.is_empty() {
                    let idx = spans.len() as u32;
                    let off = arena.len() as u32;
                    arena.extend_from_slice(cid);
                    spans.push((off, cid.len() as u32));
                    ids.push(String::from_utf8_lossy(cid).into_owned());
                    channels.push(String::from_utf8_lossy(ch).into_owned());
                    let h = hasher.hash_one(cid);
                    table.insert_unique(h, idx, |&id| {
                        let (o, l) = spans[id as usize];
                        hasher.hash_one(&arena[o as usize..(o + l) as usize])
                    });
                }
            }
        });
        let organic_id = ids.len() as u32;
        ids.push("organic".to_string());
        channels.push("organic".to_string());

        Self {
            hasher,
            table,
            arena,
            spans,
            ids,
            channels,
            organic_id,
        }
    }

    #[inline(always)]
    fn lookup(&self, camp_id: &[u8]) -> Option<u32> {
        let h = self.hasher.hash_one(camp_id);
        self.table
            .find(h, |&id| {
                let (o, l) = self.spans[id as usize];
                &self.arena[o as usize..(o + l) as usize] == camp_id
            })
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

#[derive(Clone, Copy)]
struct RawEvent {
    hash_eid: u64,
    ts: i64,
    ingest_ts: i64,
    eid_off: u32,
    comp_id: u32, // u32::MAX if unlinked
    price_cents: u32,
    state: i32, // -1 invalid/late, 0 view/cart, 1+cat_id purchase
    eid_len: u16,
    uid_len: u16,
}

struct EventBatch {
    arena: Vec<u8>,
    records: Vec<RawEvent>,
}

impl EventBatch {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            records: Vec::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct SurvEvent {
    ts: i64,
    person_key: u32, // comp_id if uid_len == 0, else uid_off in SurvEventBatch.arena
    uid_len: u32,
    price_cents: u32,
    state: i32, // 0 view/cart, 1+cat_id purchase
}

struct SurvEventBatch {
    arena: Vec<u8>,
    records: Vec<SurvEvent>,
}

impl SurvEventBatch {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            records: Vec::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct RawClick {
    ts: i64,
    person_key: u32, // comp_id if uid_len == 0, else uid_off (followed immediately by click_id)
    uid_len: u16,
    cid_len: u16,
    cid_off: u32, // if uid_len == 0, offset of click_id; else uid_off + uid_len
    camp_idx: u32,
}

struct ClickBatch {
    arena: Vec<u8>,
    records: Vec<RawClick>,
}

impl ClickBatch {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            records: Vec::new(),
        }
    }
}

trait BatchBuffer: Sized {
    fn new_batch() -> Self;
    fn is_batch_empty(&self) -> bool;
}

impl BatchBuffer for EventBatch {
    fn new_batch() -> Self {
        EventBatch::new()
    }
    fn is_batch_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl BatchBuffer for SurvEventBatch {
    fn new_batch() -> Self {
        SurvEventBatch::new()
    }
    fn is_batch_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl BatchBuffer for ClickBatch {
    fn new_batch() -> Self {
        ClickBatch::new()
    }
    fn is_batch_empty(&self) -> bool {
        self.records.is_empty()
    }
}

struct ThreadBuckets<'a, B: BatchBuffer> {
    shards: Vec<B>,
    dest: &'a [Mutex<Vec<B>>],
}

impl<'a, B: BatchBuffer> ThreadBuckets<'a, B> {
    fn new(dest: &'a [Mutex<Vec<B>>]) -> Self {
        let mut shards = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            shards.push(B::new_batch());
        }
        Self { shards, dest }
    }
}

impl<B: BatchBuffer> Drop for ThreadBuckets<'_, B> {
    fn drop(&mut self) {
        for (i, b) in self.shards.iter_mut().enumerate() {
            if !b.is_batch_empty() {
                let taken = std::mem::replace(b, B::new_batch());
                self.dest[i].lock().unwrap().push(taken);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CompactEvent {
    ts: i64,
    price_cents: u32,
    state: i32,
}

#[derive(Clone, Copy)]
struct CompactClick {
    ts: i64,
    camp_idx: u32,
    batch_idx: u32,
    cid_off: u32,
    cid_len: u32,
}

#[derive(Clone, Copy, Default)]
struct CampAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

fn user_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn campaign_daily_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]))
}

fn write_user_daily_parquet(
    path: &Path,
    schema: Arc<Schema>,
    uid_builder: &mut StringBuilder,
    days: &[i32],
    sessions: &[i64],
    events: &[i64],
    revenues: &[i64],
    top_cats: &[Option<&str>],
) {
    let file = BufWriter::with_capacity(
        1 << 20,
        File::create(path).expect("create user_daily parquet"),
    );
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    if !days.is_empty() {
        let uid_arr = uid_builder.finish();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(uid_arr),
                Arc::new(Date32Array::from(days.to_vec())),
                Arc::new(Int64Array::from(sessions.to_vec())),
                Arc::new(Int64Array::from(events.to_vec())),
                Arc::new(Int64Array::from(revenues.to_vec())),
                Arc::new(StringArray::from(top_cats.to_vec())),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
    }
    writer.close().expect("close parquet");
}

fn write_campaign_daily_parquet(
    path: &Path,
    schema: Arc<Schema>,
    camp_ids: &[&str],
    channels: &[&str],
    days: &[i32],
    sessions: &[i64],
    purchasing: &[i64],
    revenues: &[i64],
    assisted: &[i64],
) {
    let file = BufWriter::with_capacity(
        256 * 1024,
        File::create(path).expect("create campaign_daily parquet"),
    );
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    if !camp_ids.is_empty() {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(camp_ids.iter().copied())),
                Arc::new(StringArray::from_iter_values(channels.iter().copied())),
                Arc::new(Date32Array::from(days.to_vec())),
                Arc::new(Int64Array::from(sessions.to_vec())),
                Arc::new(Int64Array::from(purchasing.to_vec())),
                Arc::new(Int64Array::from(revenues.to_vec())),
                Arc::new(Int64Array::from(assisted.to_vec())),
            ],
        )
        .expect("record batch");
        writer.write(&batch).expect("write batch");
    }
    writer.close().expect("close parquet");
}

pub fn run_pipeline(data_dir: &Path, out_dir: &Path) {
    let t_total = Instant::now();
    let (win_start, win_end) = parse_manifest(&data_dir.join("manifest.json"));
    let click_win_start = win_start - CLICK_REACH_MS;

    let min_day = win_start.div_euclid(DAY_MS) as i32;
    let max_day = (win_end - 1).max(win_start).div_euclid(DAY_MS) as i32;
    let day_span = (max_day - min_day + 1).max(1) as usize;

    let products = ProductIndex::load(&data_dir.join("products.csv"));
    let campaigns = CampaignIndex::load(&data_dir.join("campaigns.csv"));

    let link_files = list_csvs(&data_dir.join("links"));
    let event_files = list_csvs(&data_dir.join("events"));
    let click_files = list_csvs(&data_dir.join("clicks"));

    let t0 = Instant::now();
    let link_index: LinkIndex = build_link_index(&link_files);
    let dt_links = t0.elapsed();

    // =========================================================
    // PHASE B1: Parallel ingestion of events/*.csv -> 128 eid buckets
    // =========================================================
    let t_ev_read = Instant::now();
    let eid_hasher = RandomState::with_seeds(101, 202, 303, 404);
    let eid_buckets: Vec<Mutex<Vec<EventBatch>>> =
        (0..NUM_BUCKETS).map(|_| Mutex::new(Vec::new())).collect();

    let event_mmaps: Vec<memmap2::Mmap> = event_files
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            if len == 0 {
                return None;
            }
            unsafe { MmapOptions::new().map(&f).ok() }
        })
        .collect();

    let mut event_tasks: Vec<(&[u8], usize, usize)> = Vec::new();
    for m in &event_mmaps {
        let data: &[u8] = m.as_ref();
        for (s, e) in split_mmap_chunks(data, 4 * 1024 * 1024) {
            event_tasks.push((data, s, e));
        }
    }

    event_tasks.into_par_iter().for_each_init(
        || (ThreadBuckets::new(&eid_buckets), DateCache::default()),
        |(tb, date_cache), (data, s, e)| {
            let slice = &data[s..e];
            for_each_data_line(slice, false, |line| {
                let Some([eid, uid, ts_b, ingest_b, etype, pid, price_b]) = parse_csv_line::<7>(line)
                else {
                    return;
                };
                if eid.is_empty() {
                    return;
                }
                let Some(ingest_ts) = parse_ts_cached(ingest_b, date_cache) else {
                    return;
                };

                // Steps 3 & 4 evaluated early; invalid rows still participate in dedup!
                let mut valid_ts = 0i64;
                let mut price_cents = 0u32;
                let mut state: i32 = -1;
                if !uid.is_empty() && !pid.is_empty() {
                    if let Some(ts) = parse_ts_cached(ts_b, date_cache) {
                        if ts >= win_start && ts < win_end && (ingest_ts - ts) <= LATE_LIMIT_MS {
                            if etype == b"view" || etype == b"cart" {
                                valid_ts = ts;
                                state = 0;
                            } else if etype == b"purchase" {
                                if let Some(p) = parse_price_cents(price_b) {
                                    valid_ts = ts;
                                    price_cents = p;
                                    state = 1 + products.lookup(pid) as i32;
                                }
                            }
                        }
                    }
                }

                let h = eid_hasher.hash_one(eid);
                let b_idx = (h as usize) & BUCKET_MASK;
                let batch = &mut tb.shards[b_idx];
                let eid_off = batch.arena.len() as u32;
                batch.arena.extend_from_slice(eid);
                let eid_len = eid.len() as u16;

                let (comp_id, uid_len) = if state >= 0 {
                    if let Some(cid) = link_index.lookup(uid) {
                        (cid, 0u16)
                    } else {
                        batch.arena.extend_from_slice(uid);
                        (u32::MAX, uid.len() as u16)
                    }
                } else {
                    (u32::MAX, 0u16)
                };

                batch.records.push(RawEvent {
                    hash_eid: h,
                    ts: valid_ts,
                    ingest_ts,
                    eid_off,
                    comp_id,
                    price_cents,
                    state,
                    eid_len,
                    uid_len,
                });

                if batch.records.len() >= BATCH_CAP {
                    let full = std::mem::replace(batch, EventBatch::new());
                    eid_buckets[b_idx].lock().unwrap().push(full);
                }
            });
        },
    );
    drop(event_mmaps);
    let dt_ev_read = t_ev_read.elapsed();

    // =========================================================
    // PHASE B2: Dedup each eid bucket -> route valid survivors to 128 person buckets
    // =========================================================
    let t_ev_dedup = Instant::now();
    let person_event_buckets: Vec<Mutex<Vec<SurvEventBatch>>> =
        (0..NUM_BUCKETS).map(|_| Mutex::new(Vec::new())).collect();

    eid_buckets.into_par_iter().for_each_init(
        || ThreadBuckets::new(&person_event_buckets),
        |pb_out, mutex_batches| {
            let batches = mutex_batches.into_inner().unwrap();
            if batches.is_empty() {
                return;
            }
            let total_recs: usize = batches.iter().map(|b| b.records.len()).sum();
            // Store (batch_idx, rec_idx) in hash table keyed by event_id
            let mut dedup: HashTable<(u32, u32)> = HashTable::with_capacity(total_recs);

            for (bi, batch) in batches.iter().enumerate() {
                for (ri, rec) in batch.records.iter().enumerate() {
                    let eid = &batch.arena
                        [rec.eid_off as usize..(rec.eid_off as usize + rec.eid_len as usize)];
                    let found = dedup.find_mut(rec.hash_eid, |&(obi, ori)| {
                        let ob = &batches[obi as usize];
                        let orec = &ob.records[ori as usize];
                        if orec.hash_eid != rec.hash_eid || orec.eid_len != rec.eid_len {
                            return false;
                        }
                        let oeid = &ob.arena
                            [orec.eid_off as usize..(orec.eid_off as usize + orec.eid_len as usize)];
                        oeid == eid
                    });
                    if let Some(entry) = found {
                        let (obi, ori) = *entry;
                        let orec = &batches[obi as usize].records[ori as usize];
                        if rec.ingest_ts < orec.ingest_ts {
                            *entry = (bi as u32, ri as u32);
                        }
                    } else {
                        dedup.insert_unique(rec.hash_eid, (bi as u32, ri as u32), |&(obi, ori)| {
                            batches[obi as usize].records[ori as usize].hash_eid
                        });
                    }
                }
            }

            for (bi, ri) in dedup {
                let batch = &batches[bi as usize];
                let rec = &batch.records[ri as usize];
                if rec.state < 0 {
                    continue;
                }
                if rec.uid_len == 0 {
                    let pb = (rec.comp_id as usize) & BUCKET_MASK;
                    let out = &mut pb_out.shards[pb];
                    out.records.push(SurvEvent {
                        ts: rec.ts,
                        person_key: rec.comp_id,
                        uid_len: 0,
                        price_cents: rec.price_cents,
                        state: rec.state,
                    });
                    if out.records.len() >= BATCH_CAP {
                        let full = std::mem::replace(out, SurvEventBatch::new());
                        person_event_buckets[pb].lock().unwrap().push(full);
                    }
                } else {
                    let uid_start = rec.eid_off as usize + rec.eid_len as usize;
                    let uid = &batch.arena[uid_start..uid_start + rec.uid_len as usize];
                    let pb = (link_index.hash_bytes(uid) as usize) & BUCKET_MASK;
                    let out = &mut pb_out.shards[pb];
                    let off = out.arena.len() as u32;
                    out.arena.extend_from_slice(uid);
                    out.records.push(SurvEvent {
                        ts: rec.ts,
                        person_key: off,
                        uid_len: rec.uid_len as u32,
                        price_cents: rec.price_cents,
                        state: rec.state,
                    });
                    if out.records.len() >= BATCH_CAP {
                        let full = std::mem::replace(out, SurvEventBatch::new());
                        person_event_buckets[pb].lock().unwrap().push(full);
                    }
                }
            }
        },
    );

    // =========================================================
    // PHASE C: Parallel ingestion of clicks/*.csv -> 128 person buckets
    // =========================================================
    let dt_ev_dedup = t_ev_dedup.elapsed();
    let t_ck_read = Instant::now();
    let person_click_buckets: Vec<Mutex<Vec<ClickBatch>>> =
        (0..NUM_BUCKETS).map(|_| Mutex::new(Vec::new())).collect();

    let click_mmaps: Vec<memmap2::Mmap> = click_files
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            if len == 0 {
                return None;
            }
            unsafe { MmapOptions::new().map(&f).ok() }
        })
        .collect();

    let mut click_tasks: Vec<(&[u8], usize, usize)> = Vec::new();
    for m in &click_mmaps {
        let data: &[u8] = m.as_ref();
        for (s, e) in split_mmap_chunks(data, 4 * 1024 * 1024) {
            click_tasks.push((data, s, e));
        }
    }

    click_tasks.into_par_iter().for_each_init(
        || (ThreadBuckets::new(&person_click_buckets), DateCache::default()),
        |(pb_out, date_cache), (data, s, e)| {
            let slice = &data[s..e];
            for_each_data_line(slice, false, |line| {
                let Some([cid, uid, camp_b, ts_b]) = parse_csv_line::<4>(line) else {
                    return;
                };
                if cid.is_empty() || uid.is_empty() {
                    return;
                }
                let Some(ts) = parse_ts_cached(ts_b, date_cache) else {
                    return;
                };
                if ts < click_win_start || ts >= win_end {
                    return;
                }
                let Some(camp_idx) = campaigns.lookup(camp_b) else {
                    return;
                };

                if let Some(comp_id) = link_index.lookup(uid) {
                    let pb = (comp_id as usize) & BUCKET_MASK;
                    let out = &mut pb_out.shards[pb];
                    let cid_off = out.arena.len() as u32;
                    out.arena.extend_from_slice(cid);
                    out.records.push(RawClick {
                        ts,
                        person_key: comp_id,
                        uid_len: 0,
                        cid_len: cid.len() as u16,
                        cid_off,
                        camp_idx,
                    });
                    if out.records.len() >= BATCH_CAP {
                        let full = std::mem::replace(out, ClickBatch::new());
                        person_click_buckets[pb].lock().unwrap().push(full);
                    }
                } else {
                    let pb = (link_index.hash_bytes(uid) as usize) & BUCKET_MASK;
                    let out = &mut pb_out.shards[pb];
                    let uid_off = out.arena.len() as u32;
                    out.arena.extend_from_slice(uid);
                    let cid_off = out.arena.len() as u32;
                    out.arena.extend_from_slice(cid);
                    out.records.push(RawClick {
                        ts,
                        person_key: uid_off,
                        uid_len: uid.len() as u16,
                        cid_len: cid.len() as u16,
                        cid_off,
                        camp_idx,
                    });
                    if out.records.len() >= BATCH_CAP {
                        let full = std::mem::replace(out, ClickBatch::new());
                        person_click_buckets[pb].lock().unwrap().push(full);
                    }
                }
            });
        },
    );
    drop(click_mmaps);
    let dt_ck_read = t_ck_read.elapsed();

    // =========================================================
    // PHASE D: Sessionize & Attribute per person bucket, write user_daily shards
    // =========================================================
    let t_sess = Instant::now();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_daily_dir).expect("mkdir campaign_daily");

    let ud_schema = user_daily_schema();
    let cd_schema = campaign_daily_schema();

    let num_camps_total = campaigns.ids.len(); // includes organic at campaigns.organic_id
    let use_flat_camp_table = day_span <= 128;

    let bucket_pairs: Vec<(usize, (Mutex<Vec<SurvEventBatch>>, Mutex<Vec<ClickBatch>>))> =
        person_event_buckets
            .into_iter()
            .zip(person_click_buckets)
            .enumerate()
            .collect();

    struct BucketCampResult {
        flat: Vec<CampAgg>,
        map: HashMap<(u32, i32), CampAgg, RandomState>,
        wrote_user_file: bool,
    }

    let camp_results: Vec<BucketCampResult> = bucket_pairs
        .into_par_iter()
        .map(|(pb, (ev_mtx, ck_mtx))| {
            let ev_batches = ev_mtx.into_inner().unwrap();
            let ck_batches = ck_mtx.into_inner().unwrap();

            let mut flat = if use_flat_camp_table {
                vec![CampAgg::default(); num_camps_total * day_span]
            } else {
                Vec::new()
            };
            let mut camp_map: HashMap<(u32, i32), CampAgg, RandomState> =
                HashMap::with_hasher(RandomState::with_seeds(3, 5, 7, 9));

            let total_evs: usize = ev_batches.iter().map(|b| b.records.len()).sum();
            if total_evs == 0 {
                return BucketCampResult {
                    flat,
                    map: camp_map,
                    wrote_user_file: false,
                };
            }

            let num_linked = if link_index.num_comps > pb {
                ((link_index.num_comps - 1 - pb) >> BUCKET_BITS) + 1
            } else {
                0
            };

            let mut ev_counts: Vec<u32> = vec![0u32; num_linked];
            let mut unlinked_table: HashTable<u32> = HashTable::new();
            let mut unlinked_arena: Vec<u8> = Vec::new();
            let mut unlinked_spans: Vec<(u32, u32)> = Vec::new();
            let unlinked_hasher = RandomState::with_seeds(21, 22, 23, 24);

            let mut ev_lpids: Vec<u32> = Vec::with_capacity(total_evs);
            for b in &ev_batches {
                for rec in &b.records {
                    let lpid = if rec.uid_len == 0 {
                        (rec.person_key >> BUCKET_BITS) as u32
                    } else {
                        let uid = &b.arena
                            [rec.person_key as usize..(rec.person_key + rec.uid_len) as usize];
                        let h = unlinked_hasher.hash_one(uid);
                        if let Some(&uidx) = unlinked_table.find(h, |&idx| {
                            let (o, l) = unlinked_spans[idx as usize];
                            &unlinked_arena[o as usize..(o + l) as usize] == uid
                        }) {
                            num_linked as u32 + uidx
                        } else {
                            let uidx = unlinked_spans.len() as u32;
                            let o = unlinked_arena.len() as u32;
                            unlinked_arena.extend_from_slice(uid);
                            unlinked_spans.push((o, uid.len() as u32));
                            unlinked_table.insert_unique(h, uidx, |&idx| {
                                let (oo, ll) = unlinked_spans[idx as usize];
                                unlinked_hasher.hash_one(&unlinked_arena[oo as usize..(oo + ll) as usize])
                            });
                            ev_counts.push(0);
                            num_linked as u32 + uidx
                        }
                    };
                    ev_counts[lpid as usize] += 1;
                    ev_lpids.push(lpid);
                }
            }

            let num_persons = ev_counts.len();
            let mut ev_offsets: Vec<u32> = Vec::with_capacity(num_persons + 1);
            let mut acc = 0u32;
            for &c in &ev_counts {
                ev_offsets.push(acc);
                acc += c;
            }
            ev_offsets.push(acc);

            let mut ev_write = ev_offsets[..num_persons].to_vec();
            let mut compact_events: Vec<CompactEvent> = vec![
                CompactEvent {
                    ts: 0,
                    price_cents: 0,
                    state: 0,
                };
                total_evs
            ];
            let mut idx = 0usize;
            for b in &ev_batches {
                for rec in &b.records {
                    let lpid = ev_lpids[idx] as usize;
                    idx += 1;
                    let pos = ev_write[lpid] as usize;
                    ev_write[lpid] += 1;
                    compact_events[pos] = CompactEvent {
                        ts: rec.ts,
                        price_cents: rec.price_cents,
                        state: rec.state,
                    };
                }
            }
            drop(ev_lpids);
            drop(ev_write);
            drop(ev_batches);

            // Group surviving clicks ONLY for persons who have >= 1 event!
            let total_cks_raw: usize = ck_batches.iter().map(|b| b.records.len()).sum();
            let mut ck_counts: Vec<u32> = vec![0u32; num_persons];
            let mut kept_clicks: Vec<(u32, CompactClick)> = Vec::with_capacity(total_cks_raw);

            for (bi, b) in ck_batches.iter().enumerate() {
                for rec in &b.records {
                    let lpid = if rec.uid_len == 0 {
                        let p = (rec.person_key >> BUCKET_BITS) as usize;
                        if p >= num_persons || ev_counts[p] == 0 {
                            continue;
                        }
                        p as u32
                    } else {
                        let uid = &b.arena
                            [rec.person_key as usize..rec.person_key as usize + rec.uid_len as usize];
                        let h = unlinked_hasher.hash_one(uid);
                        let Some(&uidx) = unlinked_table.find(h, |&u| {
                            let (o, l) = unlinked_spans[u as usize];
                            &unlinked_arena[o as usize..(o + l) as usize] == uid
                        }) else {
                            continue;
                        };
                        num_linked as u32 + uidx
                    };
                    ck_counts[lpid as usize] += 1;
                    kept_clicks.push((
                        lpid,
                        CompactClick {
                            ts: rec.ts,
                            camp_idx: rec.camp_idx,
                            batch_idx: bi as u32,
                            cid_off: rec.cid_off,
                            cid_len: rec.cid_len as u32,
                        },
                    ));
                }
            }
            drop(unlinked_table);

            let mut ck_offsets: Vec<u32> = Vec::with_capacity(num_persons + 1);
            let mut cacc = 0u32;
            for &c in &ck_counts {
                ck_offsets.push(cacc);
                cacc += c;
            }
            ck_offsets.push(cacc);
            let mut ck_write = ck_offsets[..num_persons].to_vec();
            let mut compact_clicks: Vec<CompactClick> = vec![
                CompactClick {
                    ts: 0,
                    camp_idx: 0,
                    batch_idx: 0,
                    cid_off: 0,
                    cid_len: 0,
                };
                kept_clicks.len()
            ];
            for (lpid, ck) in kept_clicks {
                let pos = ck_write[lpid as usize] as usize;
                ck_write[lpid as usize] += 1;
                compact_clicks[pos] = ck;
            }
            drop(ck_write);
            drop(ck_counts);

            // Output columns for user_daily shard
            let mut ud_uid_builder = StringBuilder::with_capacity(num_persons * 2, num_persons * 24);
            let mut ud_days: Vec<i32> = Vec::new();
            let mut ud_sessions: Vec<i64> = Vec::new();
            let mut ud_events: Vec<i64> = Vec::new();
            let mut ud_revenues: Vec<i64> = Vec::new();
            let mut ud_top_cats: Vec<Option<&str>> = Vec::new();

            // O(1) sliding window assist tracker
            let mut camp_in_win: Vec<u32> = vec![0u32; campaigns.ids.len()];
            let mut active_camps: Vec<u32> = Vec::with_capacity(32);
            let mut day_cat_revs: Vec<(u32, i64)> = Vec::with_capacity(16);

            let get_camp_entry = |flat: &mut Vec<CampAgg>,
                                  camp_map: &mut HashMap<(u32, i32), CampAgg, RandomState>,
                                  camp_idx: u32,
                                  day: i32|
             -> *mut CampAgg {
                if use_flat_camp_table {
                    let d_off = (day - min_day) as usize;
                    let slot = (camp_idx as usize) * day_span + d_off;
                    &mut flat[slot] as *mut CampAgg
                } else {
                    camp_map.entry((camp_idx, day)).or_default() as *mut CampAgg
                }
            };

            for lpid in 0..num_persons {
                let ev_s = ev_offsets[lpid] as usize;
                let ev_e = ev_offsets[lpid + 1] as usize;
                if ev_s == ev_e {
                    continue;
                }
                let evs = &mut compact_events[ev_s..ev_e];
                evs.sort_unstable_by_key(|e| e.ts);

                let ck_s = ck_offsets[lpid] as usize;
                let ck_e = ck_offsets[lpid + 1] as usize;
                let cks = &mut compact_clicks[ck_s..ck_e];
                if cks.len() > 1 {
                    cks.sort_unstable_by(|a, b| match a.ts.cmp(&b.ts) {
                        Ordering::Equal => {
                            let ca = &ck_batches[a.batch_idx as usize].arena
                                [a.cid_off as usize..(a.cid_off + a.cid_len) as usize];
                            let cb = &ck_batches[b.batch_idx as usize].arena
                                [b.cid_off as usize..(b.cid_off + b.cid_len) as usize];
                            ca.cmp(cb)
                        }
                        ord => ord,
                    });
                }

                let person_name_bytes: &[u8] = if lpid < num_linked {
                    let comp_id = ((lpid << BUCKET_BITS) | pb) as u32;
                    link_index.comp_name(comp_id)
                } else {
                    let (o, l) = unlinked_spans[lpid - num_linked];
                    &unlinked_arena[o as usize..(o + l) as usize]
                };
                let person_name = String::from_utf8_lossy(person_name_bytes);

                let mut ck_left = 0usize;
                let mut ck_right = 0usize;

                let mut cur_day = i32::MIN;
                let mut d_sessions = 0i64;
                let mut d_events = 0i64;
                let mut d_rev = 0i64;
                day_cat_revs.clear();

                let mut i = 0usize;
                while i < evs.len() {
                    let sess_start = evs[i].ts;
                    let sess_day = sess_start.div_euclid(DAY_MS) as i32;
                    let mut sess_events = 1i64;
                    let mut sess_rev = 0i64;

                    if sess_day != cur_day {
                        if cur_day != i32::MIN {
                            let top_cat = if day_cat_revs.is_empty() {
                                None
                            } else {
                                let mut best_cid = u32::MAX;
                                let mut best_rev = -1i64;
                                for &(cid, r) in &day_cat_revs {
                                    if r > best_rev || (r == best_rev && cid < best_cid) {
                                        best_rev = r;
                                        best_cid = cid;
                                    }
                                }
                                Some(products.categories[best_cid as usize].as_str())
                            };
                            ud_uid_builder.append_value(person_name.as_ref());
                            ud_days.push(cur_day);
                            ud_sessions.push(d_sessions);
                            ud_events.push(d_events);
                            ud_revenues.push(d_rev);
                            ud_top_cats.push(top_cat);
                        }
                        cur_day = sess_day;
                        d_sessions = 0;
                        d_events = 0;
                        d_rev = 0;
                        day_cat_revs.clear();
                    }

                    if evs[i].state > 0 {
                        let cid = (evs[i].state - 1) as u32;
                        let p = evs[i].price_cents as i64;
                        sess_rev += p;
                        if let Some(entry) = day_cat_revs.iter_mut().find(|x| x.0 == cid) {
                            entry.1 += p;
                        } else {
                            day_cat_revs.push((cid, p));
                        }
                    }

                    let mut prev_ts = sess_start;
                    let mut j = i + 1;
                    while j < evs.len() && evs[j].ts - prev_ts <= SESSION_GAP_MS {
                        prev_ts = evs[j].ts;
                        sess_events += 1;
                        if evs[j].state > 0 {
                            let cid = (evs[j].state - 1) as u32;
                            let p = evs[j].price_cents as i64;
                            sess_rev += p;
                            if let Some(entry) = day_cat_revs.iter_mut().find(|x| x.0 == cid) {
                                entry.1 += p;
                            } else {
                                day_cat_revs.push((cid, p));
                            }
                        }
                        j += 1;
                    }
                    i = j;

                    d_sessions += 1;
                    d_events += sess_events;
                    d_rev += sess_rev;

                    // Slide click window [sess_start - CLICK_REACH_MS, sess_start]
                    if !cks.is_empty() {
                        while ck_right < cks.len() && cks[ck_right].ts <= sess_start {
                            let c = cks[ck_right].camp_idx;
                            if camp_in_win[c as usize] == 0 {
                                active_camps.push(c);
                            }
                            camp_in_win[c as usize] += 1;
                            ck_right += 1;
                        }
                        let min_ck_ts = sess_start - CLICK_REACH_MS;
                        while ck_left < ck_right && cks[ck_left].ts < min_ck_ts {
                            let c = cks[ck_left].camp_idx;
                            camp_in_win[c as usize] -= 1;
                            ck_left += 1;
                        }
                    }

                    if ck_left == ck_right {
                        active_camps.clear();
                        let agg = unsafe {
                            &mut *get_camp_entry(&mut flat, &mut camp_map, campaigns.organic_id, sess_day)
                        };
                        agg.sessions += 1;
                        if sess_rev > 0 {
                            agg.purchasing_sessions += 1;
                            agg.revenue_cents += sess_rev;
                        }
                    } else {
                        // Latest ts is at ck_right - 1; find earliest index with same ts (smallest click_id)
                        let max_ts = cks[ck_right - 1].ts;
                        let mut k = ck_right - 1;
                        while k > ck_left && cks[k - 1].ts == max_ts {
                            k -= 1;
                        }
                        let win_camp = cks[k].camp_idx;
                        let agg = unsafe {
                            &mut *get_camp_entry(&mut flat, &mut camp_map, win_camp, sess_day)
                        };
                        agg.sessions += 1;
                        if sess_rev > 0 {
                            agg.purchasing_sessions += 1;
                            agg.revenue_cents += sess_rev;
                        }

                        // Compact active_camps and record assists
                        let mut write_idx = 0usize;
                        for read_idx in 0..active_camps.len() {
                            let c = active_camps[read_idx];
                            if camp_in_win[c as usize] > 0 {
                                active_camps[write_idx] = c;
                                write_idx += 1;
                                let c_agg = unsafe {
                                    &mut *get_camp_entry(&mut flat, &mut camp_map, c, sess_day)
                                };
                                c_agg.assisted_sessions += 1;
                            }
                        }
                        active_camps.truncate(write_idx);
                    }
                }

                // Flush last day for person
                if cur_day != i32::MIN {
                    let top_cat = if day_cat_revs.is_empty() {
                        None
                    } else {
                        let mut best_cid = u32::MAX;
                        let mut best_rev = -1i64;
                        for &(cid, r) in &day_cat_revs {
                            if r > best_rev || (r == best_rev && cid < best_cid) {
                                best_rev = r;
                                best_cid = cid;
                            }
                        }
                        Some(products.categories[best_cid as usize].as_str())
                    };
                    ud_uid_builder.append_value(person_name.as_ref());
                    ud_days.push(cur_day);
                    ud_sessions.push(d_sessions);
                    ud_events.push(d_events);
                    ud_revenues.push(d_rev);
                    ud_top_cats.push(top_cat);
                }

                // Reset active_camps for next person
                for &c in &active_camps {
                    camp_in_win[c as usize] = 0;
                }
                active_camps.clear();
            }

            let wrote_user_file = !ud_days.is_empty();
            if wrote_user_file {
                let p = user_daily_dir.join(format!("part-{pb:05}.parquet"));
                write_user_daily_parquet(
                    &p,
                    ud_schema.clone(),
                    &mut ud_uid_builder,
                    &ud_days,
                    &ud_sessions,
                    &ud_events,
                    &ud_revenues,
                    &ud_top_cats,
                );
            }

            BucketCampResult {
                flat,
                map: camp_map,
                wrote_user_file,
            }
        })
        .collect();

    // Ensure at least one Parquet file exists in user_daily/ even when empty
    if !camp_results.iter().any(|r| r.wrote_user_file) {
        let mut empty_uid = StringBuilder::new();
        write_user_daily_parquet(
            &user_daily_dir.join("part-00000.parquet"),
            ud_schema,
            &mut empty_uid,
            &[],
            &[],
            &[],
            &[],
            &[],
        );
    }

    // Merge campaign_daily results and write part-00000.parquet
    let mut cd_ids: Vec<&str> = Vec::new();
    let mut cd_channels: Vec<&str> = Vec::new();
    let mut cd_days: Vec<i32> = Vec::new();
    let mut cd_sessions: Vec<i64> = Vec::new();
    let mut cd_purchasing: Vec<i64> = Vec::new();
    let mut cd_revenues: Vec<i64> = Vec::new();
    let mut cd_assisted: Vec<i64> = Vec::new();

    if use_flat_camp_table {
        let total_slots = num_camps_total * day_span;
        let mut merged = vec![CampAgg::default(); total_slots];
        for r in &camp_results {
            for (i, agg) in r.flat.iter().enumerate() {
                if agg.sessions != 0 || agg.assisted_sessions != 0 {
                    merged[i].sessions += agg.sessions;
                    merged[i].purchasing_sessions += agg.purchasing_sessions;
                    merged[i].revenue_cents += agg.revenue_cents;
                    merged[i].assisted_sessions += agg.assisted_sessions;
                }
            }
        }
        for c in 0..num_camps_total {
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
        for r in &camp_results {
            for (&k, agg) in &r.map {
                let e = merged.entry(k).or_default();
                e.sessions += agg.sessions;
                e.purchasing_sessions += agg.purchasing_sessions;
                e.revenue_cents += agg.revenue_cents;
                e.assisted_sessions += agg.assisted_sessions;
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
        cd_schema,
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
        "PROFILE links={:.3}s ev_read={:.3}s ev_dedup={:.3}s ck_read={:.3}s sess_write={:.3}s total={:.3}s",
        dt_links.as_secs_f64(),
        dt_ev_read.as_secs_f64(),
        dt_ev_dedup.as_secs_f64(),
        dt_ck_read.as_secs_f64(),
        dt_sess.as_secs_f64(),
        t_total.elapsed().as_secs_f64()
    );
}
