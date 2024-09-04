use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use wasm_bindgen::prelude::*;

use crate::error::Result;
use crate::ArrowWasmError;

#[wasm_bindgen]
pub struct Vector {
    chunks: Vec<ArrayRef>,
    field: FieldRef,
}

impl Vector {
    pub fn try_new(chunks: Vec<ArrayRef>, field: FieldRef) -> Result<Self> {
        if !chunks
            .iter()
            .all(|chunk| chunk.data_type().equals_datatype(field.data_type()))
        {
            return Err(ArrowWasmError::InternalError(
                "All chunks must have same data type".to_string(),
            ));
        }
        Ok(Self { chunks, field })
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    /// Create a new PyChunkedArray from a vec of [ArrayRef]s, inferring their data type
    /// automatically.
    pub fn from_array_refs(chunks: Vec<ArrayRef>) -> Result<Self> {
        if chunks.is_empty() {
            return Err(ArrowError::SchemaError(
                "Cannot infer data type from empty Vec<ArrayRef>".to_string(),
            )
            .into());
        }

        if !chunks
            .windows(2)
            .all(|w| w[0].data_type() == w[1].data_type())
        {
            return Err(ArrowError::SchemaError("Mismatched data types".to_string()).into());
        }

        let field = Field::new("", chunks.first().unwrap().data_type().clone(), true);
        Self::try_new(chunks, Arc::new(field))
    }
}

impl TryFrom<Vec<ArrayRef>> for Vector {
    type Error = ArrowWasmError;

    fn try_from(value: Vec<ArrayRef>) -> Result<Self> {
        Self::from_array_refs(value)
    }
}

impl AsRef<[ArrayRef]> for Vector {
    fn as_ref(&self) -> &[ArrayRef] {
        &self.chunks
    }
}
