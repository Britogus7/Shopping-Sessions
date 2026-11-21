use std::fs::File;
use std::path::PathBuf;

use ahash::RandomState;
use hashbrown::HashTable;
use memmap2::MmapOptions;
use rayon::prelude::*;

use crate::csv::{for_each_data_line, parse_csv_line};

pub struct LinkIndex {
    hasher: RandomState,
    // Hash table mapping node_id -> bucket by user_id bytes
    table: HashTable<u32>,
    node_arena: Vec<u8>,
    node_spans: Vec<(u32, u32)>,
    node_comp: Vec<u32>,
    // Dense component canonical names (byte-wise smallest ID in each component)
    comp_arena: Vec<u8>,
    comp_spans: Vec<(u32, u32)>,
    pub num_comps: usize,
}

impl LinkIndex {
    #[inline(always)]
    pub fn hash_bytes(&self, b: &[u8]) -> u64 {
        self.hasher.hash_one(b)
    }

    #[inline(always)]
    pub fn lookup(&self, user_id: &[u8]) -> Option<u32> {
        if self.num_comps == 0 {
            return None;
        }
        let h = self.hash_bytes(user_id);
        self.table
            .find(h, |&nid| {
                let (off, len) = self.node_spans[nid as usize];
                &self.node_arena[off as usize..(off + len) as usize] == user_id
            })
            .map(|&nid| self.node_comp[nid as usize])
    }

    #[inline(always)]
    pub fn comp_name(&self, comp_id: u32) -> &[u8] {
        let (off, len) = self.comp_spans[comp_id as usize];
        &self.comp_arena[off as usize..(off + len) as usize]
    }
}

struct ParsedLinkChunk {
    arena: Vec<u8>,
    pairs: Vec<(u32, u32, u32, u32)>, // (off_a, len_a, off_b, len_b)
}

pub fn split_mmap_chunks(data: &[u8], chunk_bytes: usize) -> Vec<(usize, usize)> {
    let Some(first_nl) = memchr::memchr(b'\n', data) else {
        return Vec::new();
    };
    let data_start = first_nl + 1;
    let total = data.len();
    if data_start >= total {
        return Vec::new();
    }
    let mut cuts = vec![data_start];
    let mut pos = data_start + chunk_bytes;
    while pos < total {
        let aligned = match memchr::memchr(b'\n', &data[pos..]) {
            Some(rel) => pos + rel + 1,
            None => total,
        };
        if aligned > *cuts.last().unwrap() {
            cuts.push(aligned);
        }
        if aligned >= total {
            break;
        }
        pos = aligned + chunk_bytes;
    }
    if *cuts.last().unwrap() < total {
        cuts.push(total);
    }
    cuts.windows(2).map(|w| (w[0], w[1])).collect()
}

pub fn build_link_index(link_files: &[PathBuf]) -> LinkIndex {
    let hasher = RandomState::with_seeds(1, 2, 3, 4);

    // Mmap all non-empty link files and parse chunks in parallel
    let mmaps: Vec<memmap2::Mmap> = link_files
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

    let mut tasks: Vec<(&[u8], usize, usize)> = Vec::new();
    for m in &mmaps {
        let data: &[u8] = m.as_ref();
        for (s, e) in split_mmap_chunks(data, 2 * 1024 * 1024) {
            tasks.push((data, s, e));
        }
    }

    let parsed_chunks: Vec<ParsedLinkChunk> = tasks
        .into_par_iter()
        .map(|(data, s, e)| {
            let slice = &data[s..e];
            let mut arena = Vec::with_capacity(slice.len() / 2);
            let mut pairs = Vec::with_capacity(slice.len() / 24);
            for_each_data_line(slice, false, |line| {
                if let Some([a, b]) = parse_csv_line::<2>(line) {
                    if !a.is_empty() && !b.is_empty() {
                        let oa = arena.len() as u32;
                        arena.extend_from_slice(a);
                        let la = a.len() as u32;
                        let ob = arena.len() as u32;
                        arena.extend_from_slice(b);
                        let lb = b.len() as u32;
                        pairs.push((oa, la, ob, lb));
                    }
                }
            });
            ParsedLinkChunk { arena, pairs }
        })
        .collect();

    let total_pairs: usize = parsed_chunks.iter().map(|c| c.pairs.len()).sum();
    if total_pairs == 0 {
        return LinkIndex {
            hasher,
            table: HashTable::new(),
            node_arena: Vec::new(),
            node_spans: Vec::new(),
            node_comp: Vec::new(),
            comp_arena: Vec::new(),
            comp_spans: Vec::new(),
            num_comps: 0,
        };
    }

    let mut table: HashTable<u32> = HashTable::with_capacity(total_pairs);
    let mut node_arena: Vec<u8> = Vec::with_capacity(total_pairs * 12);
    let mut node_spans: Vec<(u32, u32)> = Vec::with_capacity(total_pairs);
    let mut parent: Vec<u32> = Vec::with_capacity(total_pairs);
    let mut size: Vec<u32> = Vec::with_capacity(total_pairs);
    let mut min_node: Vec<u32> = Vec::with_capacity(total_pairs);

    #[inline(always)]
    fn intern(
        s: &[u8],
        hasher: &RandomState,
        table: &mut HashTable<u32>,
        node_arena: &mut Vec<u8>,
        node_spans: &mut Vec<(u32, u32)>,
        parent: &mut Vec<u32>,
        size: &mut Vec<u32>,
        min_node: &mut Vec<u32>,
    ) -> u32 {
        let h = hasher.hash_one(s);
        if let Some(&id) = table.find(h, |&nid| {
            let (off, len) = node_spans[nid as usize];
            &node_arena[off as usize..(off + len) as usize] == s
        }) {
            return id;
        }
        let id = node_spans.len() as u32;
        let off = node_arena.len() as u32;
        node_arena.extend_from_slice(s);
        node_spans.push((off, s.len() as u32));
        parent.push(id);
        size.push(1);
        min_node.push(id);
        table.insert_unique(h, id, |&nid| {
            let (o, l) = node_spans[nid as usize];
            hasher.hash_one(&node_arena[o as usize..(o + l) as usize])
        });
        id
    }

    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }

    for chunk in &parsed_chunks {
        for &(oa, la, ob, lb) in &chunk.pairs {
            let sa = &chunk.arena[oa as usize..(oa + la) as usize];
            let sb = &chunk.arena[ob as usize..(ob + lb) as usize];
            let ia = intern(
                sa,
                &hasher,
                &mut table,
                &mut node_arena,
                &mut node_spans,
                &mut parent,
                &mut size,
                &mut min_node,
            );
            let ib = intern(
                sb,
                &hasher,
                &mut table,
                &mut node_arena,
                &mut node_spans,
                &mut parent,
                &mut size,
                &mut min_node,
            );
            let mut ra = find(&mut parent, ia);
            let mut rb = find(&mut parent, ib);
            if ra != rb {
                if size[ra as usize] < size[rb as usize] {
                    std::mem::swap(&mut ra, &mut rb);
                }
                parent[rb as usize] = ra;
                size[ra as usize] += size[rb as usize];
                let ma = min_node[ra as usize];
                let mb = min_node[rb as usize];
                let (oa_m, la_m) = node_spans[ma as usize];
                let (ob_m, lb_m) = node_spans[mb as usize];
                if &node_arena[ob_m as usize..(ob_m + lb_m) as usize]
                    < &node_arena[oa_m as usize..(oa_m + la_m) as usize]
                {
                    min_node[ra as usize] = mb;
                }
            }
        }
    }

    let n = node_spans.len();
    let mut root_to_comp = vec![u32::MAX; n];
    let mut node_comp = vec![0u32; n];
    let mut comp_arena = Vec::with_capacity(n * 4);
    let mut comp_spans = Vec::new();
    let mut num_comps = 0u32;

    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let mut cid = root_to_comp[r];
        if cid == u32::MAX {
            cid = num_comps;
            num_comps += 1;
            root_to_comp[r] = cid;
            let mn = min_node[r] as usize;
            let (off, len) = node_spans[mn];
            let c_off = comp_arena.len() as u32;
            comp_arena.extend_from_slice(&node_arena[off as usize..(off + len) as usize]);
            comp_spans.push((c_off, len));
        }
        node_comp[i] = cid;
    }

    LinkIndex {
        hasher,
        table,
        node_arena,
        node_spans,
        node_comp,
        comp_arena,
        comp_spans,
        num_comps: num_comps as usize,
    }
}
