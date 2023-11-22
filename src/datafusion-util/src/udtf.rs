use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Signature;
use datafusion::scalar::ScalarValue;

#[async_trait]
pub trait TableUDF: Sync + Send {
    fn name(&self) -> &str;

    async fn create_provider(
        &self, ctx: &dyn TableFuncContextProvider, args: Vec<FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>>;

    fn signature(&self) -> Option<Signature>;
}

pub trait TableFuncContextProvider: Sync + Send {
    fn get_session_state(&self) -> SessionState;
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum FuncParamValue {
    Ident(String),
    Scalar(ScalarValue),
    Array(Vec<FuncParamValue>),
}

impl fmt::Display for FuncParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::Scalar(s) => write!(f, "{s}"),
            Self::Array(vals) => {
                write!(f, "(")?;
                write!(
                    f,
                    "{}",
                    vals.iter()
                        .map(|val| val.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
                .unwrap();
                write!(f, ")")
            }
        }
    }
}

impl FuncParamValue {
    pub fn param_into<T>(self) -> Result<T>
    where
        T: FromFuncParamValue,
    {
        T::from_param(self)
    }
}

pub trait FromFuncParamValue: Sized {
    fn from_param(value: FuncParamValue) -> Result<Self>;

    fn from_param_ref(value: &FuncParamValue) -> Result<Self> {
        Self::from_param(value.clone())
    }

    fn is_param_valid(value: &FuncParamValue) -> bool;
}

impl FromFuncParamValue for i64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => {
                match s {
                    ScalarValue::Int8(Some(v)) => Ok(v as i64),
                    ScalarValue::Int16(Some(v)) => Ok(v as i64),
                    ScalarValue::Int32(Some(v)) => Ok(v as i64),
                    ScalarValue::Int64(Some(v)) => Ok(v),
                    ScalarValue::UInt8(Some(v)) => Ok(v as i64),
                    ScalarValue::UInt16(Some(v)) => Ok(v as i64),
                    ScalarValue::UInt32(Some(v)) => Ok(v as i64),
                    ScalarValue::UInt64(Some(v)) => Ok(v as i64), // TODO: Handle overflow?
                    other => Err(anyhow!("Invalid scalar value for i64: {:?}", other)),
                }
            }

            other => Err(anyhow!("Invalid value for i64: {:?}", other)),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Int8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int64(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt64(Some(_)))
        )
    }
}
