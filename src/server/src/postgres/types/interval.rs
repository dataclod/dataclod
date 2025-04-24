use std::error::Error;

use bytes::{Buf, BufMut, BytesMut};
use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use pgwire::types::ToSqlText;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

#[derive(Debug)]
pub(crate) struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl std::fmt::Display for PgInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} months {} days {} microseconds",
            self.months, self.days, self.microseconds
        )
    }
}

impl From<i32> for PgInterval {
    fn from(year_month: i32) -> Self {
        PgInterval {
            months: year_month,
            days: 0,
            microseconds: 0,
        }
    }
}

impl From<IntervalDayTime> for PgInterval {
    fn from(day_time: IntervalDayTime) -> Self {
        PgInterval {
            months: 0,
            days: day_time.days,
            microseconds: day_time.milliseconds as i64 * 1000,
        }
    }
}

impl From<IntervalMonthDayNano> for PgInterval {
    fn from(month_day_nano: IntervalMonthDayNano) -> Self {
        PgInterval {
            months: month_day_nano.months,
            days: month_day_nano.days,
            microseconds: month_day_nano.nanoseconds / 1000,
        }
    }
}

impl<'a> FromSql<'a> for PgInterval {
    fn from_sql(_ty: &Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let microseconds = raw.get_i64();
        let days = raw.get_i32();
        let months = raw.get_i32();
        Ok(PgInterval {
            months,
            days,
            microseconds,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::INTERVAL)
    }
}

impl ToSql for PgInterval {
    to_sql_checked!();

    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_i64(self.microseconds);
        out.put_i32(self.days);
        out.put_i32(self.months);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(ty, &Type::INTERVAL)
    }
}

impl ToSqlText for PgInterval {
    fn to_sql_text(
        &self, _ty: &Type, out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let str = self.to_string();
        out.put_slice(str.as_bytes());
        Ok(IsNull::No)
    }
}
