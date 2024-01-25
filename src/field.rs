use arrow_schema::DataType;
use std::collections::HashMap;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;

#[wasm_bindgen(typescript_custom_section)]
const TS_FieldMetadata: &'static str = r#"
export type FieldMetadata = Map<string, string>;
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "FieldMetadata")]
    pub type FieldMetadata;
}

#[wasm_bindgen]
pub struct Field(pub(crate) arrow_schema::FieldRef);

#[wasm_bindgen]
impl Field {
    /// Export this field to an `FFIArrowSchema`` object, which can be read with arrow-js-ffi.
    #[wasm_bindgen]
    pub fn to_ffi(&self) -> WasmResult<crate::ffi::FFIArrowSchema> {
        Ok(crate::ffi::FFIArrowSchema::try_from(self)?)
    }

    /// Returns the `Field`'s name.
    #[wasm_bindgen]
    pub fn name(&self) -> String {
        self.0.name().clone()
    }

    /// Sets the name of this `Field` and returns a new object
    #[wasm_bindgen]
    pub fn with_name(&mut self, name: String) -> WasmResult<Field> {
        let field = self.0.as_ref().clone();
        Ok(field.with_name(name).into())
    }

    #[wasm_bindgen]
    pub fn data_type(&self) -> JsValue {
        let dt = self.0.data_type();
        match dt {
            DataType::Null => crate::datatype::Null.into(),
            DataType::Boolean => crate::datatype::Boolean.into(),
            DataType::Int8 => crate::datatype::Int8.into(),
            DataType::Int16 => crate::datatype::Int16.into(),
            DataType::Int32 => crate::datatype::Int32.into(),
            DataType::Int64 => crate::datatype::Int64.into(),
            DataType::UInt8 => crate::datatype::UInt8.into(),
            DataType::UInt16 => crate::datatype::UInt16.into(),
            DataType::UInt32 => crate::datatype::UInt32.into(),
            DataType::UInt64 => crate::datatype::UInt64.into(),
            DataType::Float16 => crate::datatype::Float16.into(),
            DataType::Float32 => crate::datatype::Float32.into(),
            DataType::Float64 => crate::datatype::Float64.into(),
            DataType::Timestamp(unit, tz) => {
                crate::datatype::Timestamp::new(unit.clone(), tz.clone()).into()
            }
            DataType::Date32 => crate::datatype::Date32.into(),
            DataType::Date64 => crate::datatype::Date64.into(),
            DataType::Time32(unit) => crate::datatype::Time32::new(unit.clone()).into(),
            DataType::Time64(unit) => crate::datatype::Time64::new(unit.clone()).into(),
            DataType::Duration(unit) => crate::datatype::Duration::new(unit.clone()).into(),
            DataType::Interval(unit) => crate::datatype::Interval::new(unit.clone()).into(),
            DataType::Binary => crate::datatype::Binary.into(),
            DataType::FixedSizeBinary(size) => crate::datatype::FixedSizeBinary::new(*size).into(),
            DataType::LargeBinary => crate::datatype::LargeBinary.into(),
            DataType::Utf8 => crate::datatype::Utf8.into(),
            DataType::LargeUtf8 => crate::datatype::LargeUtf8.into(),
            DataType::List(field) => crate::datatype::List::new(field.clone()).into(),
            DataType::FixedSizeList(field, size) => {
                crate::datatype::FixedSizeList::new(field.clone(), *size).into()
            }
            DataType::LargeList(field) => crate::datatype::LargeList::new(field.clone()).into(),
            DataType::Struct(fields) => crate::datatype::Struct::new(fields.clone()).into(),
            DataType::Union(fields, mode) => {
                crate::datatype::Union::new(fields.clone(), *mode).into()
            }
            DataType::Dictionary(key_type, value_type) => {
                crate::datatype::Dictionary::new(key_type.clone(), value_type.clone()).into()
            }
            DataType::Decimal128(precision, scale) => {
                crate::datatype::Decimal128::new(*precision, *scale).into()
            }
            DataType::Decimal256(precision, scale) => {
                crate::datatype::Decimal256::new(*precision, *scale).into()
            }
            DataType::Map(field, sorted) => {
                crate::datatype::Map_::new(field.clone(), *sorted).into()
            }
            DataType::RunEndEncoded(run_ends, values) => {
                crate::datatype::RunEndEncoded::new(run_ends.clone(), values.clone()).into()
            }
        }
    }

    /// Indicates whether this [`Field`] supports null values.
    #[wasm_bindgen]
    pub fn is_nullable(&self) -> bool {
        self.0.is_nullable()
    }

    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<FieldMetadata> {
        Ok(serde_wasm_bindgen::to_value(self.0.metadata())?.into())
    }

    /// Sets the metadata of this `Field` to be `metadata` and returns a new object
    #[wasm_bindgen]
    pub fn with_metadata(&mut self, metadata: FieldMetadata) -> WasmResult<Field> {
        let metadata: HashMap<String, String> = serde_wasm_bindgen::from_value(metadata.into())?;
        let field = self.0.as_ref().clone();
        Ok(field.with_metadata(metadata).into())
    }
}

impl From<arrow_schema::Field> for Field {
    fn from(value: arrow_schema::Field) -> Self {
        Self(Arc::new(value))
    }
}

impl From<&arrow_schema::Field> for Field {
    fn from(value: &arrow_schema::Field) -> Self {
        Self(Arc::new(value.clone()))
    }
}

impl From<arrow_schema::FieldRef> for Field {
    fn from(value: arrow_schema::FieldRef) -> Self {
        Self(value)
    }
}

impl From<&arrow_schema::FieldRef> for Field {
    fn from(value: &arrow_schema::FieldRef) -> Self {
        Self(value.clone())
    }
}
