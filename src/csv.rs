//! Spec-exact SIMD CSV scanner, pread chunk reader, L1 window date table,
//! and SWAR timestamp / price parsers.

use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};

pub const DAY_MS: i64 = 86_400_000;
pub const SESSION_GAP_MS: i64 = 1_800_000;
pub const CLICK_REACH_MS: i64 = 7 * DAY_MS;
pub const LATE_LIMIT_MS: i64 = DAY_MS;

/// Unquote a single CSV field according to `rules.md`:
/// - No value ever holds a comma, quote, or newline.
/// - A field starting with `"` must also end with `"` (len >= 2) and contain no inner `"`.
/// - A field not starting with `"` must contain no `"`.
#[inline(always)]
pub fn unquote_field(field: &[u8]) -> Option<&[u8]> {
    if let Some(&first) = field.first() {
        if first == b'"' {
            if field.len() < 2 || field[field.len() - 1] != b'"' {
                return None;
            }
            let inner = &field[1..field.len() - 1];
            if memchr::memchr(b'"', inner).is_some() {
                return None;
            }
            Some(inner)
        } else {
            if memchr::memchr(b'"', field).is_some() {
                return None;
            }
            Some(field)
        }
    } else {
        Some(field)
    }
}

/// Split a CSV line (without trailing `\n`) into exact `N` unquoted fields.
/// Returns `None` if empty line, wrong field count, or malformed quotes.
#[inline(always)]
pub fn parse_csv_line<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    if memchr::memchr(b'"', line).is_none() {
        for i in 0..(N - 1) {
            let rel = memchr::memchr(b',', &line[start..])?;
            let end = start + rel;
            out[i] = &line[start..end];
            start = end + 1;
        }
        let last = &line[start..];
        if memchr::memchr(b',', last).is_some() {
            return None;
        }
        out[N - 1] = last;
        return Some(out);
    }
    for i in 0..(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        let end = start + rel;
        out[i] = unquote_field(&line[start..end])?;
        start = end + 1;
    }
    let last = &line[start..];
    if memchr::memchr(b',', last).is_some() {
        return None;
    }
    out[N - 1] = unquote_field(last)?;
    Some(out)
}

/// Iterate lines in an in-memory CSV byte slice (used for small files like products/campaigns).
#[inline]
pub fn for_each_data_line<F>(data: &[u8], skip_header: bool, mut f: F)
where
    F: FnMut(&[u8]),
{
    let mut pos = 0usize;
    if skip_header {
        match memchr::memchr(b'\n', data) {
            Some(nl) => pos = nl + 1,
            None => return,
        }
    }
    while pos < data.len() {
        match memchr::memchr(b'\n', &data[pos..]) {
            Some(rel) => {
                f(&data[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                f(&data[pos..]);
                break;
            }
        }
    }
}

#[inline(always)]
fn scan_csv_block_inner<'a, const N: usize, F>(
    slice: &'a [u8],
    blk_pos: usize,
    nl_m: u64,
    cm_m: u64,
    qt_m: u64,
    fields: &mut [&'a [u8]; N],
    line_start: &mut usize,
    field_idx: &mut usize,
    field_start: &mut usize,
    field_quotes: &mut u32,
    f: &mut F,
) where
    F: FnMut([&'a [u8]; N]),
{
    let mut delim_m = nl_m | cm_m;
    let mut cursor_in_blk = 0u32;

    while delim_m != 0 {
        let bit = delim_m.trailing_zeros();
        let dpos = blk_pos + bit as usize;
        let is_comma = ((cm_m >> bit) & 1) != 0;

        if is_comma {
            if *field_idx < N - 1 {
                let range_mask = ((1u64 << bit) - 1) & (u64::MAX << cursor_in_blk);
                let q = *field_quotes + (qt_m & range_mask).count_ones();
                let fs = *field_start;
                if q == 0 {
                    fields[*field_idx] = unsafe { slice.get_unchecked(fs..dpos) };
                    *field_idx += 1;
                    *field_start = dpos + 1;
                    *field_quotes = 0;
                } else if q == 2
                    && dpos - fs >= 2
                    && unsafe { *slice.get_unchecked(fs) } == b'"'
                    && unsafe { *slice.get_unchecked(dpos - 1) } == b'"'
                {
                    fields[*field_idx] = unsafe { slice.get_unchecked(fs + 1..dpos - 1) };
                    *field_idx += 1;
                    *field_start = dpos + 1;
                    *field_quotes = 0;
                } else {
                    *field_idx = usize::MAX;
                }
            } else {
                *field_idx = usize::MAX;
            }
        } else {
            // Newline delimiter
            if *field_idx == N - 1 && dpos > *line_start {
                let range_mask = ((1u64 << bit) - 1) & (u64::MAX << cursor_in_blk);
                let q = *field_quotes + (qt_m & range_mask).count_ones();
                let fs = *field_start;
                let ok = if q == 0 {
                    fields[N - 1] = unsafe { slice.get_unchecked(fs..dpos) };
                    true
                } else if q == 2
                    && dpos - fs >= 2
                    && unsafe { *slice.get_unchecked(fs) } == b'"'
                    && unsafe { *slice.get_unchecked(dpos - 1) } == b'"'
                {
                    fields[N - 1] = unsafe { slice.get_unchecked(fs + 1..dpos - 1) };
                    true
                } else {
                    false
                };
                if ok {
                    f(*fields);
                }
            }
            *line_start = dpos + 1;
            *field_idx = 0;
            *field_start = *line_start;
            *field_quotes = 0;
        }

        cursor_in_blk = bit + 1;
        delim_m &= delim_m - 1;
    }

    if *field_idx != usize::MAX && cursor_in_blk < 64 {
        *field_quotes += (qt_m >> cursor_in_blk).count_ones();
    }
}

#[inline(always)]
fn scalar_block_masks(blk: &[u8]) -> (u64, u64, u64) {
    let mut nl_m = 0u64;
    let mut cm_m = 0u64;
    let mut qt_m = 0u64;
    for (i, &b) in blk.iter().enumerate() {
        let bit = 1u64 << i;
        if b == b'\n' {
            nl_m |= bit;
        } else if b == b',' {
            cm_m |= bit;
        } else if b == b'"' {
            qt_m |= bit;
        }
    }
    (nl_m, cm_m, qt_m)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vl")]
unsafe fn scan_csv_slice_avx512<const N: usize, F: FnMut([&[u8]; N])>(
    slice: &[u8],
    f: &mut F,
) {
    use std::arch::x86_64::*;

    let v_nl = _mm512_set1_epi8(b'\n' as i8);
    let v_cm = _mm512_set1_epi8(b',' as i8);
    let v_qt = _mm512_set1_epi8(b'"' as i8);

    let mut fields = [&[][..]; N];
    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_start = 0usize;
    let mut field_quotes = 0u32;

    let len = slice.len();
    let mut pos = 0usize;
    let ptr = slice.as_ptr();
    while pos + 64 <= len {
        let blk = _mm512_loadu_si512(ptr.add(pos) as *const _);
        let nl_m = _mm512_cmpeq_epi8_mask(blk, v_nl);
        let cm_m = _mm512_cmpeq_epi8_mask(blk, v_cm);
        let qt_m = _mm512_cmpeq_epi8_mask(blk, v_qt);
        scan_csv_block_inner::<N, F>(
            slice,
            pos,
            nl_m,
            cm_m,
            qt_m,
            &mut fields,
            &mut line_start,
            &mut field_idx,
            &mut field_start,
            &mut field_quotes,
            f,
        );
        pos += 64;
    }
    if pos < len {
        let (nl_m, cm_m, qt_m) = scalar_block_masks(&slice[pos..]);
        scan_csv_block_inner::<N, F>(
            slice,
            pos,
            nl_m,
            cm_m,
            qt_m,
            &mut fields,
            &mut line_start,
            &mut field_idx,
            &mut field_start,
            &mut field_quotes,
            f,
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_csv_slice_avx2<const N: usize, F: FnMut([&[u8]; N])>(slice: &[u8], f: &mut F) {
    use std::arch::x86_64::*;

    let v_nl = _mm256_set1_epi8(b'\n' as i8);
    let v_cm = _mm256_set1_epi8(b',' as i8);
    let v_qt = _mm256_set1_epi8(b'"' as i8);

    let mut fields = [&[][..]; N];
    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_start = 0usize;
    let mut field_quotes = 0u32;

    let len = slice.len();
    let mut pos = 0usize;
    let ptr = slice.as_ptr();
    while pos + 64 <= len {
        let lo = _mm256_loadu_si256(ptr.add(pos) as *const __m256i);
        let hi = _mm256_loadu_si256(ptr.add(pos + 32) as *const __m256i);

        let nl_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_nl)) as u32 as u64;
        let nl_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_nl)) as u32 as u64;
        let cm_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_cm)) as u32 as u64;
        let cm_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_cm)) as u32 as u64;
        let qt_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_qt)) as u32 as u64;
        let qt_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_qt)) as u32 as u64;

        scan_csv_block_inner::<N, F>(
            slice,
            pos,
            nl_lo | (nl_hi << 32),
            cm_lo | (cm_hi << 32),
            qt_lo | (qt_hi << 32),
            &mut fields,
            &mut line_start,
            &mut field_idx,
            &mut field_start,
            &mut field_quotes,
            f,
        );
        pos += 64;
    }
    if pos < len {
        let (nl_m, cm_m, qt_m) = scalar_block_masks(&slice[pos..]);
        scan_csv_block_inner::<N, F>(
            slice,
            pos,
            nl_m,
            cm_m,
            qt_m,
            &mut fields,
            &mut line_start,
            &mut field_idx,
            &mut field_start,
            &mut field_quotes,
            f,
        );
    }
}

fn scan_csv_slice_scalar<const N: usize, F: FnMut([&[u8]; N])>(slice: &[u8], f: &mut F) {
    let mut fields = [&[][..]; N];
    let mut line_start = 0usize;
    let mut field_idx = 0usize;
    let mut field_start = 0usize;
    let mut field_quotes = 0u32;

    let mut pos = 0usize;
    while pos < slice.len() {
        let end = (pos + 64).min(slice.len());
        let (nl_m, cm_m, qt_m) = scalar_block_masks(&slice[pos..end]);
        scan_csv_block_inner::<N, F>(
            slice,
            pos,
            nl_m,
            cm_m,
            qt_m,
            &mut fields,
            &mut line_start,
            &mut field_idx,
            &mut field_start,
            &mut field_quotes,
            f,
        );
        pos = end;
    }
}

/// Scan every valid CSV data line owned by byte range `[start, end)` in a memory-mapped file slice
/// (lines starting at offset `L` with `start <= L < end`, skipping line 0 header).
#[inline]
pub fn scan_csv_mmap_chunk<const N: usize, F: FnMut([&[u8]; N])>(
    data: &[u8],
    start: usize,
    end: usize,
    mut f: F,
) {
    if start >= end || start >= data.len() {
        return;
    }
    let end = end.min(data.len());
    let start_pos = if start == 0 {
        let Some(nl) = memchr::memchr(b'\n', data) else {
            return;
        };
        nl + 1
    } else {
        let Some(rel) = memchr::memchr(b'\n', &data[start - 1..]) else {
            return;
        };
        start + rel
    };
    if start_pos >= end {
        return;
    }
    if let Some(rel) = memchr::memchr(b'\n', &data[end - 1..]) {
        let stop = (end - 1) + rel;
        scan_csv_slice::<N, F>(&data[start_pos..=stop], &mut f);
    } else if let Some(last_nl) = memchr::memrchr(b'\n', &data[start_pos..]) {
        let nl_abs = start_pos + last_nl;
        scan_csv_slice::<N, F>(&data[start_pos..=nl_abs], &mut f);
        let tail = &data[nl_abs + 1..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                f(row);
            }
        }
    } else {
        let tail = &data[start_pos..];
        if !tail.is_empty() {
            if let Some(row) = parse_csv_line::<N>(tail) {
                f(row);
            }
        }
    }
}

/// Scan every `\n`-terminated CSV line in `slice` using AVX-512 / AVX2 (64B/iter) or scalar dispatch.
#[inline]
pub fn scan_csv_slice<const N: usize, F: FnMut([&[u8]; N])>(slice: &[u8], f: &mut F) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512vl")
        {
            unsafe { scan_csv_slice_avx512::<N, F>(slice, f) };
            return;
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { scan_csv_slice_avx2::<N, F>(slice, f) };
            return;
        }
    }
    scan_csv_slice_scalar::<N, F>(slice, f);
}

#[cfg(unix)]
#[allow(dead_code)]
#[inline]
fn read_exact_or_eof(file: &File, mut offset: u64, dst: &mut [u8]) -> (usize, bool) {
    let mut filled = 0usize;
    while filled < dst.len() {
        match file.read_at(&mut dst[filled..], offset) {
            Ok(0) => return (filled, true),
            Ok(n) => {
                filled += n;
                offset += n as u64;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read_at failed: {e}"),
        }
    }
    (filled, false)
}

#[cfg(not(unix))]
#[inline]
fn read_exact_or_eof(file: &File, offset: u64, dst: &mut [u8]) -> (usize, bool) {
    let mut f = file;
    f.seek(SeekFrom::Start(offset)).expect("seek");
    let mut filled = 0usize;
    while filled < dst.len() {
        match f.read(&mut dst[filled..]) {
            Ok(0) => return (filled, true),
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read failed: {e}"),
        }
    }
    (filled, false)
}

const READ_BLOCK: usize = 512 * 1024;

/// Read and parse every valid CSV data line owned by file byte range `[start, end)`
/// (starting at offset `L` with `start <= L < end`, skipping line 0 header).
/// Zero upfront newline pre-scanning: each worker seeks directly to `start - 1`.
#[allow(dead_code)]
pub fn read_csv_chunk<const N: usize, F: FnMut([&[u8]; N])>(
    file: &File,
    start: u64,
    end: u64,
    buf: &mut Vec<u8>,
    mut f: F,
) {
    if start >= end {
        return;
    }

    buf.clear();
    let mut buf_file_off = if start == 0 { 0 } else { start - 1 };
    let mut hit_eof = false;

    // Step 1: read until the first '\n' (header if start == 0, else prior chunk's line tail).
    let first_nl = loop {
        let prev_len = buf.len();
        buf.resize(prev_len + READ_BLOCK, 0);
        let (n, eof) = read_exact_or_eof(file, buf_file_off + prev_len as u64, &mut buf[prev_len..]);
        buf.truncate(prev_len + n);
        if eof {
            hit_eof = true;
        }
        if let Some(rel) = memchr::memchr(b'\n', &buf[prev_len..]) {
            break Some(prev_len + rel);
        }
        if eof {
            break None;
        }
    };

    let Some(nl_idx) = first_nl else {
        return;
    };

    let data_start_off = buf_file_off + (nl_idx as u64) + 1;
    if data_start_off >= end {
        return;
    }

    let rem = buf.len() - (nl_idx + 1);
    buf.copy_within(nl_idx + 1.., 0);
    buf.truncate(rem);
    buf_file_off = data_start_off;

    // Step 2: stream complete owned lines from buf_file_off .. < end.
    loop {
        if buf_file_off >= end {
            break;
        }
        let end_in_buf = end - buf_file_off;

        if (buf.len() as u64) >= end_in_buf {
            let rel_end = end_in_buf as usize;
            if let Some(rel) = memchr::memchr(b'\n', &buf[rel_end - 1..]) {
                let stop = rel_end - 1 + rel;
                scan_csv_slice::<N, F>(&buf[..=stop], &mut f);
                break;
            } else if hit_eof {
                if let Some(last_nl) = memchr::memrchr(b'\n', buf) {
                    scan_csv_slice::<N, F>(&buf[..=last_nl], &mut f);
                    let tail = &buf[last_nl + 1..];
                    if !tail.is_empty() {
                        if let Some(row) = parse_csv_line::<N>(tail) {
                            f(row);
                        }
                    }
                } else if !buf.is_empty() {
                    if let Some(row) = parse_csv_line::<N>(buf) {
                        f(row);
                    }
                }
                break;
            }
        } else if !buf.is_empty() {
            if let Some(last_nl) = memchr::memrchr(b'\n', buf) {
                scan_csv_slice::<N, F>(&buf[..=last_nl], &mut f);
                let tail_len = buf.len() - (last_nl + 1);
                buf.copy_within(last_nl + 1.., 0);
                buf.truncate(tail_len);
                buf_file_off += (last_nl + 1) as u64;
                if buf_file_off >= end {
                    break;
                }
            }
            if hit_eof {
                // Only reached if file ended without trailing '\n'
                if !buf.is_empty() {
                    if let Some(row) = parse_csv_line::<N>(buf) {
                        f(row);
                    }
                }
                break;
            }
        } else if hit_eof {
            break;
        }

        // Need more bytes from file.
        let prev_len = buf.len();
        buf.resize(prev_len + READ_BLOCK, 0);
        let (n, eof) = read_exact_or_eof(file, buf_file_off + prev_len as u64, &mut buf[prev_len..]);
        buf.truncate(prev_len + n);
        if eof {
            hit_eof = true;
        }
    }
}

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    let da = a.wrapping_sub(b'0');
    let db = b.wrapping_sub(b'0');
    if da <= 9 && db <= 9 {
        Some((da as i64) * 10 + (db as i64))
    } else {
        None
    }
}

#[inline(always)]
fn d4(b: &[u8]) -> Option<i64> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2v = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 && d2v <= 9 && d3 <= 9 {
        Some((d0 as i64) * 1000 + (d1 as i64) * 100 + (d2v as i64) * 10 + (d3 as i64))
    } else {
        None
    }
}

#[inline(always)]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn civil_year_from_days(days: i64) -> i64 {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    year_of_era + era * 400 + i64::from(month <= 2)
}

#[inline(always)]
fn max_day_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// 9 KB L1-resident lookup table covering `[start_year - 1 ..= start_year + 1]`.
/// Invalid calendar dates (e.g., Feb 29 non-leap, Feb 30, day 0) stay `i64::MIN`.
pub struct WindowDateTable {
    base_year: i64,
    days: [i64; 3 * 12 * 32],
}

impl WindowDateTable {
    pub fn new(win_start_ms: i64) -> Self {
        let start_year = civil_year_from_days(win_start_ms.div_euclid(DAY_MS));
        let base_year = start_year - 1;
        let mut days = [i64::MIN; 3 * 12 * 32];
        for yo in 0..3i64 {
            let y = base_year + yo;
            for m in 1..=12i64 {
                let md = max_day_in_month(y, m);
                for d in 1..=md {
                    let idx = ((yo * 12 + (m - 1)) * 32 + d) as usize;
                    days[idx] = days_from_civil(y, m, d) * DAY_MS;
                }
            }
        }
        Self { base_year, days }
    }
}

#[inline(always)]
fn parse_iso_datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = d4(&b[0..4])?;
    let month = d2(b[5], b[6])?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let day = d2(b[8], b[9])?;
    if day < 1 || day > max_day_in_month(year, month) {
        return None;
    }
    let hour = d2(b[11], b[12])?;
    let minute = d2(b[14], b[15])?;
    let second = d2(b[17], b[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

#[inline(always)]
fn parse_iso_datetime_table(b: &[u8], table: &WindowDateTable) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let hour = d2(b[11], b[12])?;
    let minute = d2(b[14], b[15])?;
    let second = d2(b[17], b[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let time_ms = (hour * 3_600 + minute * 60 + second) * 1_000;
    let year = d4(&b[0..4])?;
    let month = d2(b[5], b[6])?;
    let day = d2(b[8], b[9])?;
    let yo = year - table.base_year;
    if (0..3).contains(&yo) && (1..=12).contains(&month) && (0..=31).contains(&day) {
        let idx = ((yo * 12 + (month - 1)) * 32 + day) as usize;
        let day_ms = unsafe { *table.days.get_unchecked(idx) };
        if day_ms == i64::MIN {
            return None;
        }
        return Some(day_ms + time_ms);
    }
    if !(1..=12).contains(&month) || day < 1 || day > max_day_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day) * DAY_MS + time_ms)
}

#[inline(always)]
pub fn parse_8_digits_swar(chunk: [u8; 8]) -> Option<u64> {
    let v = u64::from_le_bytes(chunk);
    if ((v | !v.wrapping_add(0x5050_5050_5050_5050) | v.wrapping_add(0x4646_4646_4646_4646))
        & 0x8080_8080_8080_8080)
        != 0
    {
        return None;
    }
    let mut x = v - 0x3030_3030_3030_3030;
    x = (x * 10 + (x >> 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x * 100 + (x >> 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x * 10_000 + (x >> 32)) & 0x0000_0000_FFFF_FFFF;
    Some(x)
}

#[inline(always)]
pub fn parse_4_digits_swar(chunk: [u8; 4]) -> Option<u64> {
    let v = u32::from_le_bytes(chunk);
    if ((v | !v.wrapping_add(0x5050_5050) | v.wrapping_add(0x4646_4646)) & 0x8080_8080) != 0 {
        return None;
    }
    let mut x = v - 0x3030_3030;
    x = (x * 10 + (x >> 8)) & 0x00FF_00FF;
    x = (x * 100 + (x >> 16)) & 0x0000_FFFF;
    Some(x as u64)
}

/// Fast timestamp parser using L1 `WindowDateTable` + SWAR for 13-digit F3 epochs.
#[inline(always)]
pub fn parse_ts_fast(b: &[u8], table: &WindowDateTable) -> Option<i64> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        return parse_iso_datetime_table(b, table);
    }
    if len == 13 {
        let hi = parse_8_digits_swar(unsafe { *(b.as_ptr() as *const [u8; 8]) })?;
        let mid = parse_4_digits_swar(unsafe { *(b.as_ptr().add(8) as *const [u8; 4]) })?;
        let d12 = b[12].wrapping_sub(b'0');
        if d12 > 9 {
            return None;
        }
        return Some((hi * 100_000 + mid * 10 + d12 as u64) as i64);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let utc_base = parse_iso_datetime_table(b, table)?;
        let oh = d2(b[20], b[21])?;
        let om = d2(b[23], b[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset_ms = (oh * 60 + om) * 60_000;
        return if b[19] == b'+' {
            Some(utc_base - offset_ms)
        } else {
            Some(utc_base + offset_ms)
        };
    }
    parse_ts_f3_general(b)
}

#[cold]
fn parse_ts_f3_general(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 0 {
        return None;
    }
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
    }
    let mut start = 0usize;
    while start < len && b[start] == b'0' {
        start += 1;
    }
    let sig = &b[start..];
    if sig.is_empty() {
        return Some(0);
    }
    if sig.len() > 18 {
        return None;
    }
    let mut val = 0i64;
    for &c in sig {
        val = val * 10 + (c - b'0') as i64;
    }
    Some(val)
}

/// Standalone timestamp parser (used for `manifest.json` and unit checks).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        return parse_iso_datetime(b);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let utc_base = parse_iso_datetime(b)?;
        let oh = d2(b[20], b[21])?;
        let om = d2(b[23], b[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset_ms = (oh * 60 + om) * 60_000;
        return if b[19] == b'+' {
            Some(utc_base - offset_ms)
        } else {
            Some(utc_base + offset_ms)
        };
    }
    parse_ts_f3_general(b)
}

/// Single-pass ASCII purchase price parser (`1..=10_000_000`).
/// Supports arbitrary leading zeros while rejecting any non-ASCII-digit or out-of-range value.
#[inline(always)]
pub fn parse_price_cents(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut val: u32 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + d as u32;
        if val > 10_000_000 {
            return None;
        }
    }
    if val >= 1 {
        Some(val)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn swar_digit_verification_exhaustive_single_byte() {
        let mut buf8 = *b"00000000";
        for b in 0u8..=255 {
            for pos in 0..8 {
                buf8[pos] = b;
                let got = parse_8_digits_swar(buf8);
                if b.is_ascii_digit() {
                    let expected = (b - b'0') as u64 * 10u64.pow((7 - pos) as u32);
                    assert_eq!(got, Some(expected));
                } else {
                    assert_eq!(got, None, "byte {b:#x} at pos {pos}");
                }
                buf8[pos] = b'0';
            }
        }
    }

    #[test]
    fn window_date_table_and_timestamps() {
        let ws = parse_ts(b"2028-02-20T00:00:00Z").unwrap();
        let tbl = WindowDateTable::new(ws);
        // Valid leap day in 2028, invalid in 2027/2029, Feb 30, offsets, F3 leading zeros
        let cases = [
            b"2028-02-29T12:34:56Z".as_slice(),
            b"2027-02-29T12:34:56Z",
            b"2028-02-30T00:00:00Z",
            b"2028-02-28T23:59:59+23:59",
            b"2028-02-28T23:59:59-00:00",
            b"2028-02-28T23:59:59+24:00",
            b"2028-02-28T23:59:59+05:60",
            b"1835481600000",
            b"0001835481600000",
            b"86400000",
            b"123456789012345678901",
            b"2028-02-28t00:00:00Z",
            b"2028-02-28T00:00:00z",
        ];
        for y in 2020..=2035 {
            let s = format!("{y:04}-12-31T23:59:59Z");
            assert_eq!(parse_ts_fast(s.as_bytes(), &tbl), parse_ts(s.as_bytes()));
        }
        for c in cases {
            assert_eq!(
                parse_ts_fast(c, &tbl),
                parse_ts(c),
                "mismatch on {:?}",
                String::from_utf8_lossy(c)
            );
        }
    }

    #[test]
    fn simd_and_chunked_reader_match_reference() {
        let mut content = String::from("a,b,c\n");
        let lines = vec![
            r#"u1,"u2",p1"#,
            "",
            r#""","","""#,
            "u1,u2,p1\r",
            r#"u1,"bad"x,p1"#,
            r#""unclosed,u2,p1"#,
            "only_two,fields",
            "too,many,fields,here",
            "long_user_identifier_0123456789abcdef0123456789abcdef,second_very_long_field_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,p99",
            r#"u9,"u10","end_no_nl""#,
        ];
        for (i, l) in lines.iter().enumerate() {
            content.push_str(l);
            if i + 1 < lines.len() {
                content.push('\n');
            }
        }
        let bytes = content.as_bytes();
        let mut expected: Vec<[Vec<u8>; 3]> = Vec::new();
        for_each_data_line(bytes, true, |l| {
            if let Some([a, b, c]) = parse_csv_line::<3>(l) {
                expected.push([a.to_vec(), b.to_vec(), c.to_vec()]);
            }
        });

        let dir = std::env::temp_dir();
        let path = dir.join("shopping_csv_chunk_test.csv");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(bytes).unwrap();
        }
        let f = File::open(&path).unwrap();
        let total = bytes.len() as u64;
        for chunk_sz in [1u64, 2, 3, 5, 17, 63, 64, 65, 256, 4096] {
            let mut got: Vec<[Vec<u8>; 3]> = Vec::new();
            let mut buf = Vec::new();
            let mut s = 0u64;
            while s < total {
                let e = (s + chunk_sz).min(total);
                read_csv_chunk::<3, _>(&f, s, e, &mut buf, |[a, b, c]| {
                    got.push([a.to_vec(), b.to_vec(), c.to_vec()]);
                });
                s = e;
            }
            assert_eq!(got, expected, "read_csv_chunk chunk_sz={chunk_sz}");

            let mut got_mmap: Vec<[Vec<u8>; 3]> = Vec::new();
            let mut s2 = 0usize;
            while s2 < bytes.len() {
                let e2 = (s2 + chunk_sz as usize).min(bytes.len());
                scan_csv_mmap_chunk::<3, _>(bytes, s2, e2, |[a, b, c]| {
                    got_mmap.push([a.to_vec(), b.to_vec(), c.to_vec()]);
                });
                s2 = e2;
            }
            assert_eq!(got_mmap, expected, "scan_csv_mmap_chunk chunk_sz={chunk_sz}");
        }
        let _ = std::fs::remove_file(&path);
    }
}
