use std::error::Error;

use bytes::{BufMut, BytesMut};
use pgwire::types::ToSqlText;
use postgres_types::{IsNull, ToSql, Type, to_sql_checked};
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct PgDecimal {
    num: i128,
    precision: u8,
    scale: i8,
}

impl PgDecimal {
    pub fn new(num: i128, precision: u8, scale: i8) -> Self {
        Self {
            num,
            precision,
            scale,
        }
    }
}

impl ToSql for PgDecimal {
    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let dec = Decimal::from(self.num);
        dec.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(ty, &Type::NUMERIC)
    }
}

impl ToSqlText for PgDecimal {
    fn to_sql_text(
        &self, _ty: &Type, out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let str = format_decimal_str(&self.num.to_string(), self.precision as usize, self.scale);
        out.put_slice(str.as_bytes());
        Ok(IsNull::No)
    }
}

fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };
    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_owned()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        // Decimal separator is in the middle of the string
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        // String has to be padded
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}
