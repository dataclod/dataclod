use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

pub fn make_ts(val: i64) -> Option<NaiveDateTime> {
    NaiveDateTime::from_timestamp_opt(val, 0)
}

pub fn make_ts_millis(val: i64) -> Option<NaiveDateTime> {
    NaiveDateTime::from_timestamp_millis(val)
}

pub fn make_ts_micros(val: i64) -> Option<NaiveDateTime> {
    NaiveDateTime::from_timestamp_micros(val)
}

pub fn make_ts_nanos(val: i64) -> Option<NaiveDateTime> {
    let secs = val.div_euclid(1_000_000_000);
    let nsecs = val.rem_euclid(1_000_000_000) as u32;
    NaiveDateTime::from_timestamp_opt(secs, nsecs)
}

pub fn make_date32(val: i32) -> Option<DateTime<Utc>> {
    NaiveDate::from_num_days_from_ce_opt(val)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| Utc.from_utc_datetime(&dt))
}

pub fn make_date64(val: i64) -> Option<DateTime<Utc>> {
    NaiveDateTime::from_timestamp_millis(val).map(|dt| Utc.from_utc_datetime(&dt))
}

pub fn make_time32(val: i32) -> Option<NaiveTime> {
    let secs = val.rem_euclid(86_400) as u32;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, 0)
}

pub fn make_time32_millis(val: i32) -> Option<NaiveTime> {
    let millis = val.rem_euclid(86_400_000);
    let secs = millis.div_euclid(1000) as u32;
    let nano = millis.rem_euclid(1000) as u32 * 1_000_000;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
}

pub fn make_time64_micros(val: i64) -> Option<NaiveTime> {
    let micros = val.rem_euclid(86_400_000_000);
    let secs = micros.div_euclid(1_000_000) as u32;
    let nano = micros.rem_euclid(1_000_000) as u32 * 1000;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
}

pub fn make_time64_nanos(val: i64) -> Option<NaiveTime> {
    let nanos = val.rem_euclid(86_400_000_000_000);
    let secs = nanos.div_euclid(1_000_000_000) as u32;
    let nano = nanos.rem_euclid(1_000_000_000) as u32;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
}
