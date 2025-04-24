use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

const UNIX_EPOCH_DAY: i64 = 719_163;

pub fn make_ts(val: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp(val, 0).map(|dt| dt.naive_utc())
}

pub fn make_ts_millis(val: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_millis(val).map(|dt| dt.naive_utc())
}

pub fn make_ts_micros(val: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(val).map(|dt| dt.naive_utc())
}

pub fn make_ts_nanos(val: i64) -> Option<NaiveDateTime> {
    let secs = val.div_euclid(1_000_000_000);
    let nsecs = val.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
}

pub fn make_date32(val: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(val + UNIX_EPOCH_DAY as i32)
}

pub fn make_date64(val: i64) -> Option<NaiveDate> {
    let days = val.div_euclid(86_400_000_000) + UNIX_EPOCH_DAY;
    NaiveDate::from_num_days_from_ce_opt(days as i32)
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
