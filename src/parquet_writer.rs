//! Minimal, zero-copy Parquet v1 UNCOMPRESSED + PLAIN writer using Thrift Compact Protocol.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const _: () = assert!(cfg!(target_endian = "little"));

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_BYTE_ARRAY: i32 = 6;

const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;

const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;

const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

const CODEC_UNCOMPRESSED: i32 = 0;
const PAGE_DATA_PAGE: i32 = 0;

struct ThriftBuf(Vec<u8>);

impl ThriftBuf {
    fn new() -> Self {
        Self(Vec::with_capacity(1024))
    }

    #[inline(always)]
    fn write_varint_u32(&mut self, mut n: u32) {
        while n >= 0x80 {
            self.0.push(((n as u8) & 0x7F) | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    #[inline(always)]
    fn write_varint_u64(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.0.push(((n as u8) & 0x7F) | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    #[inline(always)]
    fn write_zigzag_i32(&mut self, n: i32) {
        let z = ((n << 1) ^ (n >> 31)) as u32;
        self.write_varint_u32(z);
    }

    #[inline(always)]
    fn write_zigzag_i64(&mut self, n: i64) {
        let z = ((n << 1) ^ (n >> 63)) as u64;
        self.write_varint_u64(z);
    }

    #[inline(always)]
    fn write_field_hdr(&mut self, last_id: &mut i16, id: i16, type_id: u8) {
        let delta = id - *last_id;
        if delta > 0 && delta <= 15 {
            self.0.push(((delta as u8) << 4) | type_id);
        } else {
            self.0.push(type_id);
            self.write_zigzag_i32(id as i32);
        }
        *last_id = id;
    }

    #[inline(always)]
    fn write_field_i32(&mut self, last_id: &mut i16, id: i16, val: i32) {
        self.write_field_hdr(last_id, id, T_I32);
        self.write_zigzag_i32(val);
    }

    #[inline(always)]
    fn write_field_i64(&mut self, last_id: &mut i16, id: i16, val: i64) {
        self.write_field_hdr(last_id, id, T_I64);
        self.write_zigzag_i64(val);
    }

    #[inline(always)]
    fn write_field_bytes(&mut self, last_id: &mut i16, id: i16, bytes: &[u8]) {
        self.write_field_hdr(last_id, id, T_BINARY);
        self.write_varint_u32(bytes.len() as u32);
        self.0.extend_from_slice(bytes);
    }

    #[inline(always)]
    fn write_list_hdr(&mut self, elem_type: u8, len: usize) {
        if len < 15 {
            self.0.push(((len as u8) << 4) | elem_type);
        } else {
            self.0.push(0xF0 | elem_type);
            self.write_varint_u32(len as u32);
        }
    }

    #[inline(always)]
    fn write_struct_end(&mut self) {
        self.0.push(0);
    }
}

#[derive(Clone, Copy)]
enum ColLogical {
    None,
    String,
    Date,
}

#[derive(Clone, Copy)]
struct ColSpec {
    name: &'static str,
    phys_type: i32,
    rep_type: i32,
    logical: ColLogical,
}

const USER_DAILY_COLS: [ColSpec; 6] = [
    ColSpec {
        name: "user_id",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "day",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::Date,
    },
    ColSpec {
        name: "sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "events",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "revenue_cents",
        phys_type: PT_INT64,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "top_category",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_OPTIONAL,
        logical: ColLogical::String,
    },
];

const CAMPAIGN_DAILY_COLS: [ColSpec; 7] = [
    ColSpec {
        name: "campaign_id",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "channel",
        phys_type: PT_BYTE_ARRAY,
        rep_type: REP_REQUIRED,
        logical: ColLogical::String,
    },
    ColSpec {
        name: "day",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::Date,
    },
    ColSpec {
        name: "sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "purchasing_sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "revenue_cents",
        phys_type: PT_INT64,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
    ColSpec {
        name: "assisted_sessions",
        phys_type: PT_INT32,
        rep_type: REP_REQUIRED,
        logical: ColLogical::None,
    },
];

struct ColumnMetaRecord {
    col_idx: usize,
    data_page_offset: i64,
    total_size: i64,
    num_values: i64,
    optional: bool,
}

struct RowGroupRecord {
    cols: Vec<ColumnMetaRecord>,
    total_byte_size: i64,
    num_rows: i64,
}

fn encode_data_page_header(num_values: i32, payload_size: i32, out: &mut ThriftBuf) {
    out.0.clear();
    let mut last = 0i16;
    out.write_field_i32(&mut last, 1, PAGE_DATA_PAGE);
    out.write_field_i32(&mut last, 2, payload_size);
    out.write_field_i32(&mut last, 3, payload_size);
    out.write_field_hdr(&mut last, 5, T_STRUCT);
    let mut dlast = 0i16;
    out.write_field_i32(&mut dlast, 1, num_values);
    out.write_field_i32(&mut dlast, 2, ENC_PLAIN);
    out.write_field_i32(&mut dlast, 3, ENC_RLE);
    out.write_field_i32(&mut dlast, 4, ENC_RLE);
    out.write_struct_end();
    out.write_struct_end();
}

fn encode_file_metadata(schema_cols: &[ColSpec], total_rows: i64, rgs: &[RowGroupRecord]) -> Vec<u8> {
    let mut tb = ThriftBuf::new();
    let mut f_last = 0i16;

    // 1: version = 1
    tb.write_field_i32(&mut f_last, 1, 1);

    // 2: schema
    tb.write_field_hdr(&mut f_last, 2, T_LIST);
    tb.write_list_hdr(T_STRUCT, schema_cols.len() + 1);

    // Root schema element: only name (4) and num_children (5)
    {
        let mut slast = 0i16;
        tb.write_field_bytes(&mut slast, 4, b"schema");
        tb.write_field_i32(&mut slast, 5, schema_cols.len() as i32);
        tb.write_struct_end();
    }

    for col in schema_cols {
        let mut slast = 0i16;
        tb.write_field_i32(&mut slast, 1, col.phys_type);
        tb.write_field_i32(&mut slast, 3, col.rep_type);
        tb.write_field_bytes(&mut slast, 4, col.name.as_bytes());
        match col.logical {
            ColLogical::String => {
                tb.write_field_i32(&mut slast, 6, CT_UTF8);
                tb.write_field_hdr(&mut slast, 10, T_STRUCT);
                let mut ulast = 0i16;
                tb.write_field_hdr(&mut ulast, 1, T_STRUCT);
                tb.write_struct_end();
                tb.write_struct_end();
            }
            ColLogical::Date => {
                tb.write_field_i32(&mut slast, 6, CT_DATE);
                tb.write_field_hdr(&mut slast, 10, T_STRUCT);
                let mut ulast = 0i16;
                tb.write_field_hdr(&mut ulast, 6, T_STRUCT);
                tb.write_struct_end();
                tb.write_struct_end();
            }
            ColLogical::None => {}
        }
        tb.write_struct_end();
    }

    // 3: num_rows
    tb.write_field_i64(&mut f_last, 3, total_rows);

    // 4: row_groups
    tb.write_field_hdr(&mut f_last, 4, T_LIST);
    tb.write_list_hdr(T_STRUCT, rgs.len());
    for rg in rgs {
        let mut rlast = 0i16;
        tb.write_field_hdr(&mut rlast, 1, T_LIST);
        tb.write_list_hdr(T_STRUCT, rg.cols.len());
        for cm in &rg.cols {
            let spec = &schema_cols[cm.col_idx];
            let mut clast = 0i16;
            tb.write_field_i64(&mut clast, 2, cm.data_page_offset);
            tb.write_field_hdr(&mut clast, 3, T_STRUCT);
            {
                let mut mlast = 0i16;
                tb.write_field_i32(&mut mlast, 1, spec.phys_type);
                tb.write_field_hdr(&mut mlast, 2, T_LIST);
                if cm.optional {
                    tb.write_list_hdr(T_I32, 2);
                    tb.write_zigzag_i32(ENC_RLE);
                    tb.write_zigzag_i32(ENC_PLAIN);
                } else {
                    tb.write_list_hdr(T_I32, 1);
                    tb.write_zigzag_i32(ENC_PLAIN);
                }
                tb.write_field_hdr(&mut mlast, 3, T_LIST);
                tb.write_list_hdr(T_BINARY, 1);
                tb.write_varint_u32(spec.name.len() as u32);
                tb.0.extend_from_slice(spec.name.as_bytes());
                tb.write_field_i32(&mut mlast, 4, CODEC_UNCOMPRESSED);
                tb.write_field_i64(&mut mlast, 5, cm.num_values);
                tb.write_field_i64(&mut mlast, 6, cm.total_size);
                tb.write_field_i64(&mut mlast, 7, cm.total_size);
                tb.write_field_i64(&mut mlast, 9, cm.data_page_offset);
                tb.write_struct_end();
            }
            tb.write_struct_end();
        }
        tb.write_field_i64(&mut rlast, 2, rg.total_byte_size);
        tb.write_field_i64(&mut rlast, 3, rg.num_rows);
        tb.write_struct_end();
    }

    // 6: created_by
    tb.write_field_bytes(&mut f_last, 6, b"shopping-sessions");
    tb.write_struct_end();
    tb.0
}

#[inline(always)]
fn as_le_bytes<T>(slice: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, std::mem::size_of_val(slice)) }
}

/// Encode bit-width=1 definition levels bitmap using Parquet hybrid RLE/BIT_PACKED format
/// prefixed by the 4-byte little-endian byte length of the RLE stream.
fn encode_def_levels_bitwidth1(num_values: usize, def_bits: &[u8], out: &mut Vec<u8>) {
    out.clear();
    let total_bytes = (num_values + 7) >> 3;
    let mut off = 0usize;
    while off < total_bytes {
        let chunk = (total_bytes - off).min(63);
        out.push(((chunk << 1) | 1) as u8);
        out.extend_from_slice(&def_bits[off..off + chunk]);
        off += chunk;
    }
}

fn write_required_column(
    writer: &mut BufWriter<File>,
    file_pos: &mut i64,
    hdr_buf: &mut ThriftBuf,
    col_idx: usize,
    num_rows: i32,
    payload: &[u8],
) -> ColumnMetaRecord {
    let data_page_offset = *file_pos;
    encode_data_page_header(num_rows, payload.len() as i32, hdr_buf);
    writer.write_all(&hdr_buf.0).expect("write page hdr");
    writer.write_all(payload).expect("write col payload");
    let total_size = (hdr_buf.0.len() + payload.len()) as i64;
    *file_pos += total_size;
    ColumnMetaRecord {
        col_idx,
        data_page_offset,
        total_size,
        num_values: num_rows as i64,
        optional: false,
    }
}

const MAX_RG_ROWS: usize = 524_288;
const MAX_VAR_BYTES: usize = 128 * 1024 * 1024;

pub struct UserDailyParquetWriter {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    file_pos: i64,
    total_rows: i64,
    rgs: Vec<RowGroupRecord>,
    hdr_buf: ThriftBuf,
    encoded_cats: Vec<Vec<u8>>,

    rg_rows: usize,
    col_uid: Vec<u8>,
    col_day: Vec<i32>,
    col_sessions: Vec<i32>,
    col_events: Vec<i32>,
    col_rev: Vec<i64>,
    def_bits: Vec<u8>,
    rle_buf: Vec<u8>,
    top_cat_vals: Vec<u8>,
}

impl UserDailyParquetWriter {
    pub fn new(path: PathBuf, categories: &[String]) -> Self {
        let mut encoded_cats = Vec::with_capacity(categories.len());
        for c in categories {
            let b = c.as_bytes();
            let mut buf = Vec::with_capacity(4 + b.len());
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
            encoded_cats.push(buf);
        }
        Self {
            path,
            writer: None,
            file_pos: 0,
            total_rows: 0,
            rgs: Vec::new(),
            hdr_buf: ThriftBuf::new(),
            encoded_cats,
            rg_rows: 0,
            col_uid: Vec::new(),
            col_day: Vec::new(),
            col_sessions: Vec::new(),
            col_events: Vec::new(),
            col_rev: Vec::new(),
            def_bits: Vec::new(),
            rle_buf: Vec::new(),
            top_cat_vals: Vec::new(),
        }
    }

    #[cold]
    fn reserve_initial(&mut self) {
        self.col_uid.reserve(4 * 1024 * 1024);
        self.col_day.reserve(300_000);
        self.col_sessions.reserve(300_000);
        self.col_events.reserve(300_000);
        self.col_rev.reserve(300_000);
        self.def_bits.reserve(40_000);
        self.top_cat_vals.reserve(512 * 1024);
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn push(
        &mut self,
        uid_bytes: &[u8],
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat_idx: Option<usize>,
    ) {
        if self.col_day.capacity() == 0 {
            self.reserve_initial();
        }
        self.col_uid
            .extend_from_slice(&(uid_bytes.len() as u32).to_le_bytes());
        self.col_uid.extend_from_slice(uid_bytes);
        self.push_tail_fields(day, sessions, events, revenue, top_cat_idx);
    }

    #[inline(always)]
    pub fn push_prefixed_uid(
        &mut self,
        uid_prefixed: &[u8],
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat_idx: Option<usize>,
    ) {
        if self.col_day.capacity() == 0 {
            self.reserve_initial();
        }
        self.col_uid.extend_from_slice(uid_prefixed);
        self.push_tail_fields(day, sessions, events, revenue, top_cat_idx);
    }

    #[inline(always)]
    fn push_tail_fields(
        &mut self,
        day: i32,
        sessions: i32,
        events: i32,
        revenue: i64,
        top_cat_idx: Option<usize>,
    ) {
        self.col_day.push(day);
        self.col_sessions.push(sessions);
        self.col_events.push(events);
        self.col_rev.push(revenue);

        let idx = self.rg_rows;
        if (idx & 7) == 0 {
            self.def_bits.push(0);
        }
        if let Some(cid) = top_cat_idx {
            let byte_idx = idx >> 3;
            unsafe {
                *self.def_bits.get_unchecked_mut(byte_idx) |= 1u8 << (idx & 7);
            }
            self.top_cat_vals
                .extend_from_slice(&self.encoded_cats[cid]);
        }

        self.rg_rows = idx + 1;
        if self.rg_rows >= MAX_RG_ROWS
            || self.col_uid.len() >= MAX_VAR_BYTES
            || self.top_cat_vals.len() >= MAX_VAR_BYTES
        {
            self.flush_rg();
        }
    }

    #[cold]
    pub fn flush_rg(&mut self) {
        if self.rg_rows == 0 {
            return;
        }
        let w = self.writer.get_or_insert_with(|| {
            let f = File::create(&self.path).expect("create user_daily parquet");
            let mut bw = BufWriter::with_capacity(1 << 20, f);
            bw.write_all(b"PAR1").expect("write PAR1");
            self.file_pos = 4;
            bw
        });

        let nr = self.rg_rows as i32;
        let mut cols = Vec::with_capacity(6);
        let mut rg_bytes = 0i64;

        let c0 = write_required_column(
            w,
            &mut self.file_pos,
            &mut self.hdr_buf,
            0,
            nr,
            &self.col_uid,
        );
        rg_bytes += c0.total_size;
        cols.push(c0);

        let c1 = write_required_column(
            w,
            &mut self.file_pos,
            &mut self.hdr_buf,
            1,
            nr,
            as_le_bytes(&self.col_day),
        );
        rg_bytes += c1.total_size;
        cols.push(c1);

        let c2 = write_required_column(
            w,
            &mut self.file_pos,
            &mut self.hdr_buf,
            2,
            nr,
            as_le_bytes(&self.col_sessions),
        );
        rg_bytes += c2.total_size;
        cols.push(c2);

        let c3 = write_required_column(
            w,
            &mut self.file_pos,
            &mut self.hdr_buf,
            3,
            nr,
            as_le_bytes(&self.col_events),
        );
        rg_bytes += c3.total_size;
        cols.push(c3);

        let c4 = write_required_column(
            w,
            &mut self.file_pos,
            &mut self.hdr_buf,
            4,
            nr,
            as_le_bytes(&self.col_rev),
        );
        rg_bytes += c4.total_size;
        cols.push(c4);

        // Optional column 5: top_category
        encode_def_levels_bitwidth1(self.rg_rows, &self.def_bits, &mut self.rle_buf);
        let rle_len = self.rle_buf.len() as u32;
        let payload_len = (4 + self.rle_buf.len() + self.top_cat_vals.len()) as i32;
        let data_page_offset = self.file_pos;
        encode_data_page_header(nr, payload_len, &mut self.hdr_buf);
        w.write_all(&self.hdr_buf.0).expect("write top_cat hdr");
        w.write_all(&rle_len.to_le_bytes())
            .expect("write top_cat rle len");
        w.write_all(&self.rle_buf).expect("write top_cat rle");
        w.write_all(&self.top_cat_vals)
            .expect("write top_cat vals");
        let total_c5 = (self.hdr_buf.0.len() as i64) + (payload_len as i64);
        self.file_pos += total_c5;
        rg_bytes += total_c5;
        cols.push(ColumnMetaRecord {
            col_idx: 5,
            data_page_offset,
            total_size: total_c5,
            num_values: nr as i64,
            optional: true,
        });

        self.rgs.push(RowGroupRecord {
            cols,
            total_byte_size: rg_bytes,
            num_rows: nr as i64,
        });
        self.total_rows += nr as i64;
        self.rg_rows = 0;
        self.col_uid.clear();
        self.col_day.clear();
        self.col_sessions.clear();
        self.col_events.clear();
        self.col_rev.clear();
        self.def_bits.clear();
        self.top_cat_vals.clear();
    }

    pub fn close(mut self) -> bool {
        self.flush_rg();
        let Some(mut w) = self.writer else {
            return false;
        };
        let meta = encode_file_metadata(&USER_DAILY_COLS, self.total_rows, &self.rgs);
        let meta_len = meta.len() as u32;
        w.write_all(&meta).expect("write metadata");
        w.write_all(&meta_len.to_le_bytes())
            .expect("write metadata len");
        w.write_all(b"PAR1").expect("write footer PAR1");
        w.flush().expect("flush user_daily parquet");
        true
    }
}

pub fn write_empty_user_daily(path: &Path) {
    let f = File::create(path).expect("create empty user_daily parquet");
    let mut w = BufWriter::new(f);
    w.write_all(b"PAR1").expect("write PAR1");
    let meta = encode_file_metadata(&USER_DAILY_COLS, 0, &[]);
    let meta_len = meta.len() as u32;
    w.write_all(&meta).expect("write empty metadata");
    w.write_all(&meta_len.to_le_bytes())
        .expect("write empty metadata len");
    w.write_all(b"PAR1").expect("write empty footer PAR1");
    w.flush().expect("flush empty user_daily");
}

pub fn write_campaign_daily_parquet(
    path: &Path,
    ids: &[&str],
    channels: &[&str],
    days: &[i32],
    sessions: &[i32],
    purchasing: &[i32],
    revenues: &[i64],
    assisted: &[i32],
) {
    let f = File::create(path).expect("create campaign_daily parquet");
    let mut w = BufWriter::with_capacity(256 * 1024, f);
    w.write_all(b"PAR1").expect("write cd PAR1");
    let mut file_pos = 4i64;
    let total_rows = days.len();
    let mut rgs = Vec::new();
    let mut hdr_buf = ThriftBuf::new();

    if total_rows > 0 {
        let mut encoded_ids: Vec<u8> = Vec::with_capacity(total_rows * 12);
        for &s in ids {
            let b = s.as_bytes();
            encoded_ids.extend_from_slice(&(b.len() as u32).to_le_bytes());
            encoded_ids.extend_from_slice(b);
        }
        let mut encoded_ch: Vec<u8> = Vec::with_capacity(total_rows * 12);
        for &s in channels {
            let b = s.as_bytes();
            encoded_ch.extend_from_slice(&(b.len() as u32).to_le_bytes());
            encoded_ch.extend_from_slice(b);
        }

        let nr = total_rows as i32;
        let mut cols = Vec::with_capacity(7);
        let mut rg_bytes = 0i64;

        for (col_idx, payload) in [
            encoded_ids.as_slice(),
            encoded_ch.as_slice(),
            as_le_bytes(days),
            as_le_bytes(sessions),
            as_le_bytes(purchasing),
            as_le_bytes(revenues),
            as_le_bytes(assisted),
        ]
        .into_iter()
        .enumerate()
        {
            let cm = write_required_column(&mut w, &mut file_pos, &mut hdr_buf, col_idx, nr, payload);
            rg_bytes += cm.total_size;
            cols.push(cm);
        }

        rgs.push(RowGroupRecord {
            cols,
            total_byte_size: rg_bytes,
            num_rows: nr as i64,
        });
    }

    let meta = encode_file_metadata(&CAMPAIGN_DAILY_COLS, total_rows as i64, &rgs);
    let meta_len = meta.len() as u32;
    w.write_all(&meta).expect("write cd metadata");
    w.write_all(&meta_len.to_le_bytes())
        .expect("write cd metadata len");
    w.write_all(b"PAR1").expect("write cd footer PAR1");
    w.flush().expect("flush cd parquet");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_empty_all_null_and_mixed_user_daily() {
        let dir = std::env::temp_dir().join("shopping_pq_writer_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p_empty = dir.join("empty.parquet");
        write_empty_user_daily(&p_empty);
        let b_empty = std::fs::read(&p_empty).unwrap();
        assert!(b_empty.starts_with(b"PAR1"));
        assert!(b_empty.ends_with(b"PAR1"));

        let cats = vec!["apparel".to_string(), "books".to_string(), "unknown".to_string()];
        let p_mix = dir.join("mixed.parquet");
        let mut wr = UserDailyParquetWriter::new(p_mix.clone(), &cats);
        // Write 1025 rows spanning multiple RLE bit-packed runs (> 504 values)
        for i in 0..1025usize {
            let uid = format!("u_{i}");
            let cat = if i % 3 == 0 { None } else { Some(i % cats.len()) };
            wr.push(uid.as_bytes(), 21_000 + (i as i32 % 14), 1, 5, 1000, cat);
        }
        assert!(wr.close());
        let b_mix = std::fs::read(&p_mix).unwrap();
        assert!(b_mix.starts_with(b"PAR1"));
        assert!(b_mix.ends_with(b"PAR1"));

        let p_cd = dir.join("cd.parquet");
        write_campaign_daily_parquet(
            &p_cd,
            &["c_1", "organic"],
            &["social", "organic"],
            &[21_000, 21_000],
            &[5, 2],
            &[2, 0],
            &[500, 0],
            &[6, 0],
        );
        let b_cd = std::fs::read(&p_cd).unwrap();
        assert!(b_cd.starts_with(b"PAR1"));
        assert!(b_cd.ends_with(b"PAR1"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
