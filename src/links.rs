//! Direct 2-pass lock-free DSU link resolver (with 3-bit register-blocked Bloom filter)
//! and cold bucketed fallback for adversarially dense short-ID inputs.

use std::cmp::Ordering;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering as MemOrdering};

use memmap2::{Mmap, MmapOptions};

use crate::csv::scan_csv_mmap_chunk;
use crate::huge::HugeZeroed;
use crate::pack::{mix64, OverflowInterner, EMPTY_KEY};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RedirectSlot {
    pub key: u64,
    pub val: u64,
}

#[repr(C)]
struct AtomicRedirectSlot {
    key: AtomicU64,
    val: AtomicU64,
}

const _: () = {
    assert!(std::mem::size_of::<AtomicRedirectSlot>() == std::mem::size_of::<RedirectSlot>());
    assert!(std::mem::align_of::<AtomicRedirectSlot>() == std::mem::align_of::<RedirectSlot>());
    assert!(std::mem::size_of::<AtomicRedirectSlot>() == 16);
    assert!(std::mem::align_of::<AtomicRedirectSlot>() == 8);
};

const LINK_BLOOM_MAX_SHIFT: u32 = 16; // 1<<16 words = 512 KB

pub struct LinkRedirect {
    bitset: HugeZeroed<u64>,
    wmask: usize,
    slots: HugeZeroed<RedirectSlot>,
    smask: usize,
}

#[inline(always)]
fn bloom_word_and_mask(h: u64, wmask: usize) -> (usize, u64) {
    let w = (h as usize) & wmask;
    let m = (1u64 << ((h >> 46) & 63))
        | (1u64 << ((h >> 52) & 63))
        | (1u64 << ((h >> 58) & 63));
    (w, m)
}

#[inline(always)]
fn prefetch_addr<T>(ptr: *const T) {
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

impl LinkRedirect {
    pub fn empty() -> Self {
        Self {
            bitset: HugeZeroed::new(0),
            wmask: 0,
            slots: HugeZeroed::new(0),
            smask: 0,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline(always)]
    pub fn prefetch_bloom_for(&self, k: u64) -> u64 {
        let h = mix64(k);
        if !self.bitset.is_empty() {
            let w = (h as usize) & self.wmask;
            prefetch_addr(unsafe { self.bitset.as_ptr().add(w) });
        }
        h
    }

    #[inline(always)]
    pub fn bloom_hit_slot_with_hash(&self, h: u64) -> Option<usize> {
        if self.slots.is_empty() {
            return None;
        }
        let (w, m) = bloom_word_and_mask(h, self.wmask);
        let word = unsafe { *self.bitset.get_unchecked(w) };
        if (word & m) != m {
            return None;
        }
        Some(((h >> 20) as usize) & self.smask)
    }

    #[inline(always)]
    pub fn bloom_hit_slot_hash(&self, k: u64) -> (u64, Option<usize>) {
        let h = mix64(k);
        (h, self.bloom_hit_slot_with_hash(h))
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn bloom_hit_slot(&self, k: u64) -> Option<usize> {
        self.bloom_hit_slot_hash(k).1
    }

    #[inline(always)]
    pub fn prefetch_slot(&self, idx: usize) {
        prefetch_addr(unsafe { self.slots.as_ptr().add(idx) });
    }

    #[inline(always)]
    pub fn resolve_from_slot(&self, k: u64, mut idx: usize) -> u64 {
        loop {
            let slot = unsafe { *self.slots.get_unchecked(idx) };
            if slot.key == k {
                return slot.val;
            }
            if slot.key == EMPTY_KEY {
                return k;
            }
            idx = (idx + 1) & self.smask;
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[inline(always)]
    pub fn resolve(&self, k: u64) -> u64 {
        let Some(idx) = self.bloom_hit_slot(k) else {
            return k;
        };
        self.resolve_from_slot(k, idx)
    }
}

#[inline(always)]
fn intern_node(
    keys: &[AtomicU64],
    mask: usize,
    k: u64,
    local_added: &mut usize,
    global_nodes: &AtomicUsize,
    max_nodes: usize,
    overflowed: &AtomicBool,
) -> Option<u32> {
    let mut pos = (mix64(k) as usize) & mask;
    let mut probes = 0usize;
    loop {
        let cur = unsafe { keys.get_unchecked(pos) }.load(MemOrdering::Acquire);
        if cur == k {
            return Some(pos as u32);
        }
        if cur == EMPTY_KEY {
            match unsafe { keys.get_unchecked(pos) }.compare_exchange(
                EMPTY_KEY,
                k,
                MemOrdering::AcqRel,
                MemOrdering::Acquire,
            ) {
                Ok(_) => {
                    *local_added += 1;
                    if *local_added >= 16 {
                        let prev = global_nodes.fetch_add(*local_added, MemOrdering::Relaxed);
                        let added = *local_added;
                        *local_added = 0;
                        if prev + added > max_nodes {
                            overflowed.store(true, MemOrdering::Relaxed);
                            return None;
                        }
                    }
                    return Some(pos as u32);
                }
                Err(actual) => {
                    if actual == k {
                        return Some(pos as u32);
                    }
                }
            }
        }
        probes += 1;
        if probes > 1024 {
            if overflowed.load(MemOrdering::Relaxed) || probes > mask {
                overflowed.store(true, MemOrdering::Relaxed);
                return None;
            }
        }
        pos = (pos + 1) & mask;
    }
}

#[inline(always)]
fn find_root_zero(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p_raw = unsafe { parent.get_unchecked(x as usize) }.load(MemOrdering::Relaxed);
        if p_raw == 0 {
            let p2 = unsafe { parent.get_unchecked(x as usize) }.load(MemOrdering::Acquire);
            if p2 == 0 {
                return x;
            }
            x = p2 - 1;
            continue;
        }
        let p = p_raw - 1;
        let gp_raw = unsafe { parent.get_unchecked(p as usize) }.load(MemOrdering::Relaxed);
        if gp_raw != 0 {
            let _ = unsafe { parent.get_unchecked(x as usize) }.compare_exchange_weak(
                p_raw,
                gp_raw,
                MemOrdering::Relaxed,
                MemOrdering::Relaxed,
            );
        }
        x = p;
    }
}

#[inline(always)]
fn union_min_zero(
    parent: &[AtomicU32],
    keys: &[AtomicU64],
    overflow: &OverflowInterner,
    a: u32,
    b: u32,
) {
    loop {
        let mut u = find_root_zero(parent, a);
        let mut v = find_root_zero(parent, b);
        if u == v {
            return;
        }
        let ku = unsafe { keys.get_unchecked(u as usize) }.load(MemOrdering::Acquire);
        let kv = unsafe { keys.get_unchecked(v as usize) }.load(MemOrdering::Acquire);
        if overflow.cmp_key(kv, ku) == Ordering::Less {
            std::mem::swap(&mut u, &mut v);
        }
        if unsafe { parent.get_unchecked(v as usize) }
            .compare_exchange(0, u + 1, MemOrdering::AcqRel, MemOrdering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

fn finalize_redirect_from_pairs<F>(num_redirects: usize, fill: F) -> LinkRedirect
where
    F: FnOnce(&[AtomicRedirectSlot], &[AtomicU64], usize, usize),
{
    if num_redirects == 0 {
        return LinkRedirect::empty();
    }
    let slots_cap = (num_redirects * 10 / 7)
        .max(num_redirects + 64)
        .next_power_of_two()
        .max(64);
    let smask = slots_cap - 1;
    let atomic_slots: HugeZeroed<AtomicRedirectSlot> = HugeZeroed::new(slots_cap);

    let num_words = (num_redirects / 4)
        .next_power_of_two()
        .clamp(64, 1 << LINK_BLOOM_MAX_SHIFT);
    let wmask = num_words - 1;
    let atomic_bitset: HugeZeroed<AtomicU64> = HugeZeroed::new(num_words);

    fill(&atomic_slots, &atomic_bitset, smask, wmask);

    LinkRedirect {
        bitset: atomic_bitset.reinterpret::<u64>(),
        wmask,
        slots: atomic_slots.reinterpret::<RedirectSlot>(),
        smask,
    }
}

#[inline(always)]
fn insert_redirect_slot_with_hash(
    asl: &[AtomicRedirectSlot],
    abs: &[AtomicU64],
    smask: usize,
    wmask: usize,
    key: u64,
    canon: u64,
    h: u64,
) {
    let (w, m) = bloom_word_and_mask(h, wmask);
    unsafe { abs.get_unchecked(w) }.fetch_or(m, MemOrdering::Relaxed);

    let mut idx = ((h >> 20) as usize) & smask;
    loop {
        let s_ref = unsafe { asl.get_unchecked(idx) };
        if s_ref
            .key
            .compare_exchange(EMPTY_KEY, key, MemOrdering::Relaxed, MemOrdering::Relaxed)
            .is_ok()
        {
            s_ref.val.store(canon, MemOrdering::Relaxed);
            break;
        }
        idx = (idx + 1) & smask;
    }
}

#[inline(always)]
fn insert_redirect_slot(
    asl: &[AtomicRedirectSlot],
    abs: &[AtomicU64],
    smask: usize,
    wmask: usize,
    key: u64,
    canon: u64,
) {
    insert_redirect_slot_with_hash(asl, abs, smask, wmask, key, canon, mix64(key));
}

#[inline]
fn flush_edge_batch(
    batch: &[(u64, u64)],
    keys_ref: &[AtomicU64],
    parent_ref: &[AtomicU32],
    mask: usize,
    overflow: &OverflowInterner,
    local_added: &mut usize,
    global_nodes_ref: &AtomicUsize,
    max_nodes: usize,
    overflowed_ref: &AtomicBool,
) {
    for &(ka, kb) in batch {
        prefetch_addr(unsafe { keys_ref.as_ptr().add((mix64(ka) as usize) & mask) });
        prefetch_addr(unsafe { keys_ref.as_ptr().add((mix64(kb) as usize) & mask) });
    }
    for &(ka, kb) in batch {
        if overflowed_ref.load(MemOrdering::Relaxed) {
            return;
        }
        let Some(ia) = intern_node(
            keys_ref,
            mask,
            ka,
            local_added,
            global_nodes_ref,
            max_nodes,
            overflowed_ref,
        ) else {
            return;
        };
        let Some(ib) = intern_node(
            keys_ref,
            mask,
            kb,
            local_added,
            global_nodes_ref,
            max_nodes,
            overflowed_ref,
        ) else {
            return;
        };
        union_min_zero(parent_ref, keys_ref, overflow, ia, ib);
    }
}

pub fn build_link_redirect(
    link_files: &[PathBuf],
    num_threads: usize,
    overflow: &OverflowInterner,
) -> LinkRedirect {
    if link_files.is_empty() {
        return LinkRedirect::empty();
    }

    let mmaps: Vec<Mmap> = link_files
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len();
            if len == 0 {
                return None;
            }
            let m = unsafe { MmapOptions::new().map(&f).ok()? };
            Some(m)
        })
        .collect();

    let mut total_link_bytes = 0usize;
    let mut jobs: Vec<(usize, usize, usize)> = Vec::new();
    const CHUNK_BYTES: usize = 256 * 1024;
    for (fidx, m) in mmaps.iter().enumerate() {
        let len = m.len();
        total_link_bytes += len;
        let mut pos = 0usize;
        while pos < len {
            let end = (pos + CHUNK_BYTES).min(len);
            jobs.push((fidx, pos, end));
            pos = end;
        }
    }

    if jobs.is_empty() {
        return LinkRedirect::empty();
    }

    let nt = num_threads.max(1);
    let cap = ((total_link_bytes / 12).max(4096)).next_power_of_two();
    let mask = cap - 1;
    let max_nodes = (cap * 3) / 4;

    let keys: HugeZeroed<AtomicU64> = HugeZeroed::new(cap);
    let parent: HugeZeroed<AtomicU32> = HugeZeroed::new(cap);
    let global_nodes = AtomicUsize::new(0);
    let overflowed = AtomicBool::new(false);
    let has_edges = AtomicBool::new(false);
    let next_job = AtomicUsize::new(0);

    // Pass 1: Read CSV chunks and execute batched-prefetch lock-free DSU directly inside flat hash table
    std::thread::scope(|s| {
        let read_nt = nt.min(jobs.len());
        let mut handles = Vec::with_capacity(read_nt);
        let mmaps_ref = &mmaps;
        let jobs_ref = &jobs;
        let next_job_ref = &next_job;
        let keys_ref: &[AtomicU64] = &keys;
        let parent_ref: &[AtomicU32] = &parent;
        let global_nodes_ref = &global_nodes;
        let overflowed_ref = &overflowed;
        let has_edges_ref = &has_edges;

        for _ in 0..read_nt {
            handles.push(s.spawn(move || {
                let mut local_added = 0usize;
                let mut local_edges = false;
                let mut edge_batch = [(0u64, 0u64); 32];
                let mut batch_len = 0usize;
                loop {
                    if overflowed_ref.load(MemOrdering::Relaxed) {
                        break;
                    }
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&(fidx, start, end)) = jobs_ref.get(jid) else {
                        break;
                    };
                    scan_csv_mmap_chunk::<2, _>(&mmaps_ref[fidx], start, end, |&[a, b]| {
                        if overflowed_ref.load(MemOrdering::Relaxed) {
                            return;
                        }
                        if !a.is_empty() && !b.is_empty() {
                            let ka = overflow.encode(a);
                            let kb = overflow.encode(b);
                            if ka != kb {
                                local_edges = true;
                                edge_batch[batch_len] = (ka, kb);
                                batch_len += 1;
                                if batch_len == 32 {
                                    flush_edge_batch(
                                        &edge_batch,
                                        keys_ref,
                                        parent_ref,
                                        mask,
                                        overflow,
                                        &mut local_added,
                                        global_nodes_ref,
                                        max_nodes,
                                        overflowed_ref,
                                    );
                                    batch_len = 0;
                                }
                            }
                        }
                    });
                    if batch_len > 0 && !overflowed_ref.load(MemOrdering::Relaxed) {
                        flush_edge_batch(
                            &edge_batch[..batch_len],
                            keys_ref,
                            parent_ref,
                            mask,
                            overflow,
                            &mut local_added,
                            global_nodes_ref,
                            max_nodes,
                            overflowed_ref,
                        );
                    }
                    batch_len = 0;
                }
                if local_edges {
                    has_edges_ref.store(true, MemOrdering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    if overflowed.load(MemOrdering::Relaxed) {
        drop(keys);
        drop(parent);
        return build_link_redirect_fallback(&mmaps, &jobs, nt, overflow);
    }

    if !has_edges.load(MemOrdering::Relaxed) {
        if total_link_bytes < 256 * 1024 * 1024 {
            std::mem::forget(keys);
            std::mem::forget(parent);
            std::mem::forget(mmaps);
        }
        return LinkRedirect::empty();
    }

    // Pass 2a: Parallel path compression over non-root nodes in 0..cap
    let chunk_n = (cap + nt - 1) / nt;
    let redirect_count = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        for t in 0..nt {
            let start = t * chunk_n;
            if start >= cap {
                break;
            }
            let end = (start + chunk_n).min(cap);
            let p_ref: &[AtomicU32] = &parent;
            let rc_ref = &redirect_count;
            handles.push(s.spawn(move || {
                let mut local_redir = 0usize;
                for i in start..end {
                    let p_raw = unsafe { p_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                    if p_raw != 0 {
                        let mut r = (p_raw - 1) as usize;
                        loop {
                            let pr = unsafe { p_ref.get_unchecked(r) }.load(MemOrdering::Relaxed);
                            if pr == 0 {
                                break;
                            }
                            r = (pr - 1) as usize;
                        }
                        if r + 1 != p_raw as usize {
                            unsafe { p_ref.get_unchecked(i) }
                                .store((r + 1) as u32, MemOrdering::Relaxed);
                        }
                        local_redir += 1;
                    }
                }
                rc_ref.fetch_add(local_redir, MemOrdering::Relaxed);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let num_redirects = redirect_count.into_inner();
    let res = finalize_redirect_from_pairs(num_redirects, |asl, abs, smask, wmask| {
        // Pass 2b: Batched-prefetch scatter (key -> canonical_min_key) into LinkRedirect in parallel
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(nt);
            for t in 0..nt {
                let start = t * chunk_n;
                if start >= cap {
                    break;
                }
                let end = (start + chunk_n).min(cap);
                let p_ref: &[AtomicU32] = &parent;
                let k_ref: &[AtomicU64] = &keys;
                handles.push(s.spawn(move || {
                    let mut batch = [(0u64, 0usize, 0u64); 32];
                    let mut blen = 0usize;
                    for i in start..end {
                        let p_raw = unsafe { p_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                        if p_raw != 0 {
                            let key = unsafe { k_ref.get_unchecked(i) }.load(MemOrdering::Relaxed);
                            let h = mix64(key);
                            let root_idx = (p_raw - 1) as usize;
                            prefetch_addr(unsafe { k_ref.as_ptr().add(root_idx) });
                            prefetch_addr(unsafe { abs.as_ptr().add((h as usize) & wmask) });
                            prefetch_addr(unsafe {
                                asl.as_ptr().add(((h >> 20) as usize) & smask)
                            });
                            batch[blen] = (key, root_idx, h);
                            blen += 1;
                            if blen == 32 {
                                for &(k, ridx, kh) in &batch {
                                    let canon = unsafe { k_ref.get_unchecked(ridx) }
                                        .load(MemOrdering::Relaxed);
                                    insert_redirect_slot_with_hash(
                                        asl, abs, smask, wmask, k, canon, kh,
                                    );
                                }
                                blen = 0;
                            }
                        }
                    }
                    if blen > 0 {
                        for &(k, ridx, kh) in &batch[..blen] {
                            let canon =
                                unsafe { k_ref.get_unchecked(ridx) }.load(MemOrdering::Relaxed);
                            insert_redirect_slot_with_hash(asl, abs, smask, wmask, k, canon, kh);
                        }
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    if total_link_bytes < 256 * 1024 * 1024 {
        std::mem::forget(keys);
        std::mem::forget(parent);
        std::mem::forget(mmaps);
    }
    res
}

// =========================================================================
// Cold Fallback: Bucketed DSU for Ultra-Dense / Short-ID Adversarial Links
// =========================================================================

struct BucketDict {
    slots: Vec<(u64, u32)>,
    mask: usize,
    base_id: u32,
}

impl BucketDict {
    #[inline(always)]
    fn lookup_global(&self, k: u64) -> u32 {
        let mut idx = ((mix64(k) >> 16) as usize) & self.mask;
        loop {
            let (sk, local_id) = unsafe { *self.slots.get_unchecked(idx) };
            if sk == k {
                return self.base_id + local_id;
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn find_root_dense(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(MemOrdering::Relaxed);
        if p == x {
            let p2 = parent[x as usize].load(MemOrdering::Acquire);
            if p2 == x {
                return x;
            }
            x = p2;
            continue;
        }
        let gp = parent[p as usize].load(MemOrdering::Relaxed);
        if gp != p {
            let _ = parent[x as usize].compare_exchange_weak(
                p,
                gp,
                MemOrdering::Relaxed,
                MemOrdering::Relaxed,
            );
        }
        x = p;
    }
}

#[inline]
fn union_min_dense(
    parent: &[AtomicU32],
    keys: &[u64],
    overflow: &OverflowInterner,
    a: u32,
    b: u32,
) {
    loop {
        let mut u = find_root_dense(parent, a);
        let mut v = find_root_dense(parent, b);
        if u == v {
            return;
        }
        if overflow.cmp_key(keys[v as usize], keys[u as usize]) == Ordering::Less {
            std::mem::swap(&mut u, &mut v);
        }
        if parent[v as usize]
            .compare_exchange(v, u, MemOrdering::AcqRel, MemOrdering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

struct ThreadLinkData {
    edges: Vec<(u64, u64)>,
    key_buckets: Vec<Vec<u64>>,
}

struct BucketDedupResult {
    unique_keys: Vec<u64>,
    slots: Vec<(u64, u32)>,
    mask: usize,
}

#[cold]
fn build_link_redirect_fallback(
    mmaps: &[Mmap],
    jobs: &[(usize, usize, usize)],
    num_threads: usize,
    overflow: &OverflowInterner,
) -> LinkRedirect {
    let nt = num_threads.max(1).min(jobs.len());
    let next_job = AtomicUsize::new(0);

    let thread_data: Vec<ThreadLinkData> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let next_job_ref = &next_job;
        for _ in 0..nt {
            handles.push(s.spawn(move || {
                let mut edges: Vec<(u64, u64)> = Vec::new();
                let mut key_buckets: Vec<Vec<u64>> = (0..64).map(|_| Vec::new()).collect();
                loop {
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&(fidx, start, end)) = jobs.get(jid) else {
                        break;
                    };
                    scan_csv_mmap_chunk::<2, _>(&mmaps[fidx], start, end, |&[a, b]| {
                        if !a.is_empty() && !b.is_empty() {
                            let ka = overflow.encode(a);
                            let kb = overflow.encode(b);
                            if ka != kb {
                                edges.push((ka, kb));
                                key_buckets[(mix64(ka) as usize) & 63].push(ka);
                                key_buckets[(mix64(kb) as usize) & 63].push(kb);
                            }
                        }
                    });
                }
                ThreadLinkData { edges, key_buckets }
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let total_edges: usize = thread_data.iter().map(|t| t.edges.len()).sum();
    if total_edges == 0 {
        return LinkRedirect::empty();
    }

    let mut bucket_inputs: Vec<Vec<Vec<u64>>> = (0..64).map(|_| Vec::with_capacity(nt)).collect();
    let mut thread_edges: Vec<Vec<(u64, u64)>> = Vec::with_capacity(nt);
    for td in thread_data {
        thread_edges.push(td.edges);
        for (b, kb) in td.key_buckets.into_iter().enumerate() {
            if !kb.is_empty() {
                bucket_inputs[b].push(kb);
            }
        }
    }

    let next_bucket = AtomicUsize::new(0);
    let cell_refs: Vec<std::sync::Mutex<Option<BucketDedupResult>>> =
        (0..64).map(|_| std::sync::Mutex::new(None)).collect();

    std::thread::scope(|s| {
        let dedup_threads = num_threads.max(1).min(64);
        let mut handles = Vec::with_capacity(dedup_threads);
        for _ in 0..dedup_threads {
            handles.push(s.spawn(|| loop {
                let b = next_bucket.fetch_add(1, MemOrdering::Relaxed);
                if b >= 64 {
                    break;
                }
                let raw_vecs = &bucket_inputs[b];
                if raw_vecs.is_empty() {
                    *cell_refs[b].lock().unwrap() = Some(BucketDedupResult {
                        unique_keys: Vec::new(),
                        slots: Vec::new(),
                        mask: 0,
                    });
                    continue;
                }
                let raw_cnt: usize = raw_vecs.iter().map(|v| v.len()).sum();
                let mut cap = 64usize.max(((raw_cnt * 3) / 2).next_power_of_two());
                let mut slots = vec![(EMPTY_KEY, 0u32); cap];
                let mut mask = cap - 1;
                let mut unique_keys: Vec<u64> = Vec::with_capacity(raw_cnt / 2);

                for v in raw_vecs {
                    for &k in v {
                        let mut idx = ((mix64(k) >> 16) as usize) & mask;
                        let mut found = false;
                        loop {
                            let (sk, _) = slots[idx];
                            if sk == k {
                                found = true;
                                break;
                            }
                            if sk == EMPTY_KEY {
                                break;
                            }
                            idx = (idx + 1) & mask;
                        }
                        if !found {
                            let lid = unique_keys.len() as u32;
                            unique_keys.push(k);
                            slots[idx] = (k, lid);
                            if unique_keys.len() * 2 >= cap {
                                let new_cap = cap * 2;
                                let new_mask = new_cap - 1;
                                let mut new_slots = vec![(EMPTY_KEY, 0u32); new_cap];
                                for &(ok, olid) in &slots {
                                    if ok != EMPTY_KEY {
                                        let mut nidx = ((mix64(ok) >> 16) as usize) & new_mask;
                                        while new_slots[nidx].0 != EMPTY_KEY {
                                            nidx = (nidx + 1) & new_mask;
                                        }
                                        new_slots[nidx] = (ok, olid);
                                    }
                                }
                                slots = new_slots;
                                cap = new_cap;
                                mask = new_mask;
                            }
                        }
                    }
                }

                *cell_refs[b].lock().unwrap() = Some(BucketDedupResult {
                    unique_keys,
                    slots,
                    mask,
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(bucket_inputs);

    let mut num_nodes = 0usize;
    let mut dicts: Vec<BucketDict> = Vec::with_capacity(64);
    let mut unique_per_bucket: Vec<Vec<u64>> = Vec::with_capacity(64);
    for m in cell_refs {
        let res = m.into_inner().unwrap().unwrap();
        let cnt = res.unique_keys.len();
        dicts.push(BucketDict {
            slots: res.slots,
            mask: res.mask,
            base_id: num_nodes as u32,
        });
        num_nodes += cnt;
        unique_per_bucket.push(res.unique_keys);
    }

    let mut all_keys: Vec<u64> = Vec::with_capacity(num_nodes);
    for uk in &unique_per_bucket {
        all_keys.extend_from_slice(uk);
    }
    drop(unique_per_bucket);

    let parent: Vec<AtomicU32> = (0..num_nodes as u32).map(AtomicU32::new).collect();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(thread_edges.len());
        let dicts_ref = &dicts;
        let parent_ref = &parent;
        let keys_ref = &all_keys;
        for edges in &thread_edges {
            handles.push(s.spawn(move || {
                for &(ka, kb) in edges {
                    let ia = dicts_ref[(mix64(ka) as usize) & 63].lookup_global(ka);
                    let ib = dicts_ref[(mix64(kb) as usize) & 63].lookup_global(kb);
                    union_min_dense(parent_ref, keys_ref, overflow, ia, ib);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(thread_edges);
    drop(dicts);

    let mut roots: Vec<u32> = vec![0u32; num_nodes];
    let redirect_count = AtomicUsize::new(0);
    let flat_threads = num_threads.max(1).min((num_nodes / 1024).max(1));
    let chunk_n = (num_nodes + flat_threads - 1) / flat_threads;

    std::thread::scope(|s| {
        let mut offset = 0usize;
        let mut handles = Vec::new();
        for slice in roots.chunks_mut(chunk_n) {
            let start_i = offset;
            offset += slice.len();
            let p_ref = &parent;
            let rc_ref = &redirect_count;
            handles.push(s.spawn(move || {
                let mut local_redir = 0usize;
                for (j, slot) in slice.iter_mut().enumerate() {
                    let i = start_i + j;
                    let mut r = p_ref[i].load(MemOrdering::Relaxed) as usize;
                    loop {
                        let pr = p_ref[r].load(MemOrdering::Relaxed) as usize;
                        if pr == r {
                            break;
                        }
                        r = pr;
                    }
                    *slot = r as u32;
                    if r != i {
                        local_redir += 1;
                    }
                }
                rc_ref.fetch_add(local_redir, MemOrdering::Relaxed);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(parent);

    let num_redirects = redirect_count.into_inner();
    finalize_redirect_from_pairs(num_redirects, |asl, abs, smask, wmask| {
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            let mut offset = 0usize;
            for chunk in roots.chunks(chunk_n) {
                let start_i = offset;
                offset += chunk.len();
                let ak = &all_keys;
                handles.push(s.spawn(move || {
                    for (j, &r) in chunk.iter().enumerate() {
                        let i = start_i + j;
                        if (r as usize) != i {
                            insert_redirect_slot(asl, abs, smask, wmask, ak[i], ak[r as usize]);
                        }
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn link_redirect_chains_kiosk_mixed_and_self_links() {
        let dir = std::env::temp_dir().join("shopping_links_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let f1 = dir.join("part-00000.csv");
        let f2 = dir.join("part-00001.csv");
        let f3 = dir.join("part-00002.csv");

        {
            let mut w1 = File::create(&f1).unwrap();
            writeln!(w1, "user_id_a,user_id_b").unwrap();
            for i in (0..399).rev() {
                writeln!(w1, "c{i:03},c{:03}", i + 1).unwrap();
            }
            writeln!(w1, "solo,solo").unwrap();
            writeln!(w1, "u1,").unwrap();
            writeln!(w1, ",u2").unwrap();
            writeln!(w1, "u3,u4,u5").unwrap();
            writeln!(w1, "\"a1\",\"a2\"").unwrap();
            writeln!(w1, "a2,a1").unwrap();
        }
        {
            let mut w2 = File::create(&f2).unwrap();
            writeln!(w2, "user_id_a,user_id_b").unwrap();
            for i in 0..200 {
                writeln!(w2, "very_long_kiosk_member_{i:04},kiosk_shared").unwrap();
            }
            writeln!(w2, "kiosk_shared,0_hub").unwrap();
        }
        {
            let mut w3 = File::create(&f3).unwrap();
            writeln!(w3, "user_id_a,user_id_b").unwrap();
        }

        let overflow = OverflowInterner::new();
        let lr = build_link_redirect(&[f1.clone(), f2.clone(), f3.clone()], 4, &overflow);

        // Also validate cold fallback produces identical redirects
        let mmaps: Vec<Mmap> = [f1, f2, f3]
            .iter()
            .map(|p| unsafe { MmapOptions::new().map(&File::open(p).unwrap()).unwrap() })
            .collect();
        let jobs: Vec<(usize, usize, usize)> = mmaps
            .iter()
            .enumerate()
            .map(|(i, m)| (i, 0, m.len()))
            .collect();
        let lr_fb = build_link_redirect_fallback(&mmaps, &jobs, 4, &overflow);

        for checker in [&lr, &lr_fb] {
            let canon_c = overflow.encode(b"c000");
            for i in 0..400 {
                let k = overflow.encode(format!("c{i:03}").as_bytes());
                assert_eq!(checker.resolve(k), canon_c, "chain idx {i}");
            }
            let solo = overflow.encode(b"solo");
            assert_eq!(checker.resolve(solo), solo);
            let u1 = overflow.encode(b"u1");
            assert_eq!(checker.resolve(u1), u1);
            assert_eq!(
                checker.resolve(overflow.encode(b"a2")),
                overflow.encode(b"a1")
            );
            let hub = overflow.encode(b"0_hub");
            assert_eq!(checker.resolve(overflow.encode(b"kiosk_shared")), hub);
            for i in 0..200 {
                let mk = overflow.encode(format!("very_long_kiosk_member_{i:04}").as_bytes());
                assert_eq!(checker.resolve(mk), hub, "kiosk member {i}");
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn link_redirect_dense_triggers_fallback_cleanly() {
        let dir = std::env::temp_dir().join("shopping_links_dense_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("dense.csv");
        {
            let mut w = File::create(&f).unwrap();
            writeln!(w, "user_id_a,user_id_b").unwrap();
            // 3200 unique nodes inside ~25 KB => forces overflowed fallback on cap=4096 (max_nodes=3072)
            for i in 0..1600usize {
                writeln!(w, "x{i},y{i}").unwrap();
            }
            writeln!(w, "x0,00_root").unwrap();
            writeln!(w, "y0,x1").unwrap();
        }
        let overflow = OverflowInterner::new();
        let lr = build_link_redirect(&[f], 4, &overflow);
        let root = overflow.encode(b"00_root");
        assert_eq!(lr.resolve(overflow.encode(b"x0")), root);
        assert_eq!(lr.resolve(overflow.encode(b"y0")), root);
        assert_eq!(lr.resolve(overflow.encode(b"x1")), root);
        assert_eq!(lr.resolve(overflow.encode(b"y1")), root);
        assert_eq!(
            lr.resolve(overflow.encode(b"y1599")),
            overflow.encode(b"x1599")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
