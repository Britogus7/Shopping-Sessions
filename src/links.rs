//! Parallel SIMD link parsing, 64-bucket L2-resident node deduplication,
//! lock-free union-by-min-key DSU, and compact `LinkRedirect` lookup table.

use std::cmp::Ordering;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering as MemOrdering};

use memmap2::{Mmap, MmapOptions};

use crate::csv::scan_csv_mmap_chunk;
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
};

pub struct LinkRedirect {
    bitset: Vec<u64>,
    bmask: usize,
    slots: Vec<RedirectSlot>,
    smask: usize,
}

impl LinkRedirect {
    pub fn empty() -> Self {
        Self {
            bitset: Vec::new(),
            bmask: 0,
            slots: Vec::new(),
            smask: 0,
        }
    }

    #[inline(always)]
    pub fn resolve(&self, k: u64) -> u64 {
        if self.slots.is_empty() {
            return k;
        }
        let h = mix64(k);
        let bit_idx = (h as usize) & self.bmask;
        let word = unsafe { *self.bitset.get_unchecked(bit_idx >> 6) };
        if ((word >> (bit_idx & 63)) & 1) == 0 {
            return k;
        }
        let mut idx = (h as usize) & self.smask;
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
}

struct BucketDict {
    slots: Vec<(u64, u32)>,
    mask: usize,
    base_id: u32,
}

impl BucketDict {
    #[inline(always)]
    fn lookup_global(&self, k: u64) -> u32 {
        let mut idx = (mix64(k) as usize) & self.mask;
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
fn find_root(parent: &[AtomicU32], mut x: u32) -> u32 {
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
fn union_min(parent: &[AtomicU32], keys: &[u64], overflow: &OverflowInterner, a: u32, b: u32) {
    loop {
        let mut u = find_root(parent, a);
        let mut v = find_root(parent, b);
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
            #[cfg(unix)]
            let _ = m.advise(memmap2::Advice::Sequential);
            Some(m)
        })
        .collect();

    let mut jobs: Vec<(usize, usize, usize)> = Vec::new();
    const CHUNK_BYTES: usize = 1024 * 1024;
    for (fidx, m) in mmaps.iter().enumerate() {
        let len = m.len();
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

    let nt = num_threads.max(1).min(jobs.len());
    let next_job = AtomicUsize::new(0);

    let thread_data: Vec<ThreadLinkData> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(nt);
        let mmaps_ref = &mmaps;
        let jobs_ref = &jobs;
        let next_job_ref = &next_job;
        for _ in 0..nt {
            handles.push(s.spawn(move || {
                let mut edges: Vec<(u64, u64)> = Vec::new();
                let mut key_buckets: Vec<Vec<u64>> = (0..64).map(|_| Vec::new()).collect();
                loop {
                    let jid = next_job_ref.fetch_add(1, MemOrdering::Relaxed);
                    let Some(&(fidx, start, end)) = jobs_ref.get(jid) else {
                        break;
                    };
                    scan_csv_mmap_chunk::<2, _>(&mmaps_ref[fidx], start, end, |[a, b]| {
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
    drop(mmaps);
    drop(jobs);

    let total_edges: usize = thread_data.iter().map(|t| t.edges.len()).sum();
    if total_edges == 0 {
        return LinkRedirect::empty();
    }

    // Parallel node deduplication across the 64 key buckets.
    // Transpose key_buckets cleanly into 64 owned bucket lists so every worker mutates disjoint slices.
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
                let mut cap = 64usize.max((raw_cnt / 2).next_power_of_two());
                let mut slots = vec![(EMPTY_KEY, 0u32); cap];
                let mut mask = cap - 1;
                let mut unique_keys: Vec<u64> = Vec::with_capacity(raw_cnt / 2);

                for v in raw_vecs {
                    for &k in v {
                        let mut idx = (mix64(k) as usize) & mask;
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
                                        let mut nidx = (mix64(ok) as usize) & new_mask;
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

    // Parallel Lock-Free Union-by-Min-Key
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
                    union_min(parent_ref, keys_ref, overflow, ia, ib);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });
    drop(thread_edges);
    drop(dicts);

    // Flatten roots in parallel & count non-minimal nodes needing redirection
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
    if num_redirects == 0 {
        return LinkRedirect::empty();
    }

    let slots_cap = (num_redirects * 3 / 2)
        .max(num_redirects + 64)
        .next_power_of_two()
        .max(64);
    let smask = slots_cap - 1;
    let atomic_slots: Vec<AtomicRedirectSlot> = (0..slots_cap)
        .map(|_| AtomicRedirectSlot {
            key: AtomicU64::new(EMPTY_KEY),
            val: AtomicU64::new(0),
        })
        .collect();

    let num_bits = (num_redirects * 16).next_power_of_two().clamp(4096, 1 << 24);
    let bmask = num_bits - 1;
    let atomic_bitset: Vec<AtomicU64> = (0..(num_bits >> 6)).map(|_| AtomicU64::new(0)).collect();

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        let mut offset = 0usize;
        for chunk in roots.chunks(chunk_n) {
            let start_i = offset;
            offset += chunk.len();
            let ak = &all_keys;
            let asl = &atomic_slots;
            let abs = &atomic_bitset;
            handles.push(s.spawn(move || {
                for (j, &r) in chunk.iter().enumerate() {
                    let i = start_i + j;
                    if (r as usize) != i {
                        let key = ak[i];
                        let canon = ak[r as usize];
                        let h = mix64(key);
                        let bit_idx = (h as usize) & bmask;
                        abs[bit_idx >> 6].fetch_or(1u64 << (bit_idx & 63), MemOrdering::Relaxed);

                        let mut idx = (h as usize) & smask;
                        loop {
                            let s_ref = &asl[idx];
                            if s_ref
                                .key
                                .compare_exchange(
                                    EMPTY_KEY,
                                    key,
                                    MemOrdering::Relaxed,
                                    MemOrdering::Relaxed,
                                )
                                .is_ok()
                            {
                                s_ref.val.store(canon, MemOrdering::Relaxed);
                                break;
                            }
                            idx = (idx + 1) & smask;
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    let mut md_slots = ManuallyDrop::new(atomic_slots);
    let slots = unsafe {
        Vec::from_raw_parts(
            md_slots.as_mut_ptr() as *mut RedirectSlot,
            md_slots.len(),
            md_slots.capacity(),
        )
    };
    let mut md_bs = ManuallyDrop::new(atomic_bitset);
    let bitset = unsafe {
        Vec::from_raw_parts(
            md_bs.as_mut_ptr() as *mut u64,
            md_bs.len(),
            md_bs.capacity(),
        )
    };

    LinkRedirect {
        bitset,
        bmask,
        slots,
        smask,
    }
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
            // 400-node chain in reversed shuffled order -> smallest is "c000"
            for i in (0..399).rev() {
                writeln!(w1, "c{i:03},c{:03}", i + 1).unwrap();
            }
            // Self-link + repeated reverse + empty sides (dropped) + malformed (dropped)
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
            // Star (kiosk) connecting long overflow IDs with a short inline ID "0_hub"
            for i in 0..200 {
                writeln!(w2, "very_long_kiosk_member_{i:04},kiosk_shared").unwrap();
            }
            writeln!(w2, "kiosk_shared,0_hub").unwrap();
        }
        {
            // Header-only file
            let mut w3 = File::create(&f3).unwrap();
            writeln!(w3, "user_id_a,user_id_b").unwrap();
        }

        let overflow = OverflowInterner::new();
        let lr = build_link_redirect(&[f1, f2, f3], 4, &overflow);

        // Chain check
        let canon_c = overflow.encode(b"c000");
        for i in 0..400 {
            let k = overflow.encode(format!("c{i:03}").as_bytes());
            assert_eq!(lr.resolve(k), canon_c, "chain idx {i}");
        }

        // Self-link should resolve to itself
        let solo = overflow.encode(b"solo");
        assert_eq!(lr.resolve(solo), solo);

        // Unlinked IDs resolve to themselves
        let u1 = overflow.encode(b"u1");
        assert_eq!(lr.resolve(u1), u1);

        // Repeated pair
        assert_eq!(lr.resolve(overflow.encode(b"a2")), overflow.encode(b"a1"));

        // Kiosk star mixed overflow + inline canonical smallest
        let hub = overflow.encode(b"0_hub");
        assert_eq!(lr.resolve(overflow.encode(b"kiosk_shared")), hub);
        for i in 0..200 {
            let mk = overflow.encode(format!("very_long_kiosk_member_{i:04}").as_bytes());
            assert_eq!(lr.resolve(mk), hub, "kiosk member {i}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
