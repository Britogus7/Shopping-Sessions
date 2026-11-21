//! Spec-exact CSV dialect, timestamp (F1/F2/F3), and price parsers.

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

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') as i64) * 10 + ((b - b'0') as i64))
    } else {
        None
    }
}

#[inline(always)]
fn d4(b: &[u8]) -> Option<i64> {
    if b[0].is_ascii_digit()
        && b[1].is_ascii_digit()
        && b[2].is_ascii_digit()
        && b[3].is_ascii_digit()
    {
        Some(
            ((b[0] - b'0') as i64) * 1000
                + ((b[1] - b'0') as i64) * 100
                + ((b[2] - b'0') as i64) * 10
                + ((b[3] - b'0') as i64),
        )
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

/// Thread-local 1-entry cache for valid YYYY-MM-DD prefixes -> epoch days * 86_400_000.
#[derive(Clone, Copy)]
pub struct DateCache {
    ymd: [u8; 10],
    day_ms: i64,
}

impl Default for DateCache {
    fn default() -> Self {
        Self {
            ymd: [0u8; 10],
            day_ms: 0,
        }
    }
}

#[inline(always)]
fn parse_iso_datetime_cached(b: &[u8], cache: &mut DateCache) -> Option<i64> {
    if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let hour = d2(b[11], b[12])?;
    let minute = d2(b[14], b[15])?;
    let second = d2(b[17], b[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let time_ms = (hour * 3_600 + minute * 60 + second) * 1_000;
    if b[0..10] == cache.ymd {
        return Some(cache.day_ms + time_ms);
    }
    if b[4] != b'-' || b[7] != b'-' {
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
    let day_ms = days_from_civil(year, month, day) * 86_400_000;
    cache.ymd.copy_from_slice(&b[0..10]);
    cache.day_ms = day_ms;
    Some(day_ms + time_ms)
}

#[inline]
pub fn parse_ts_cached(b: &[u8], cache: &mut DateCache) -> Option<i64> {
    let len = b.len();
    if len == 20 && b[19] == b'Z' {
        return parse_iso_datetime_cached(b, cache);
    }
    if len == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let utc_base = parse_iso_datetime_cached(b, cache)?;
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
    if len == 0 {
        return None;
    }
    let mut i = 0;
    while i < len {
        if !b[i].is_ascii_digit() {
            return None;
        }
        i += 1;
    }
    let mut start = 0;
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
    let mut val: i64 = 0;
    for &c in sig {
        val = val * 10 + (c - b'0') as i64;
    }
    Some(val)
}

/// Parse timestamp in F1 (`...Z`), F2 (`...±HH:MM`), or F3 (ASCII digits epoch ms).
/// Returns `None` if malformed or overflows 18 digits (which is always outside any realistic window).
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
    if len == 0 {
        return None;
    }
    // Check F3: ASCII digits only
    let mut i = 0;
    while i < len {
        if !b[i].is_ascii_digit() {
            return None;
        }
        i += 1;
    }
    // Strip leading zeros
    let mut start = 0;
    while start < len && b[start] == b'0' {
        start += 1;
    }
    let sig = &b[start..];
    if sig.is_empty() {
        return Some(0);
    }
    if sig.len() > 18 {
        // Too huge for any window; treat as out-of-window.
        return None;
    }
    let mut val: i64 = 0;
    for &c in sig {
        val = val * 10 + (c - b'0') as i64;
    }
    Some(val)
}

/// Parse valid purchase price: ASCII digits only, value in 1..=10_000_000.
#[inline(always)]
pub fn parse_price_cents(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
    }
    let mut start = 0;
    while start < b.len() && b[start] == b'0' {
        start += 1;
    }
    let sig = &b[start..];
    if sig.is_empty() || sig.len() > 8 {
        return None;
    }
    let mut val: u32 = 0;
    for &c in sig {
        val = val * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&val) {
        Some(val)
    } else {
        None
    }
}

/// Iterate lines in a byte slice after skipping the CSV header row.
/// Only `\n` ends a line; `\r` is preserved as data.
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
