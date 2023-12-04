use arrow_schema::DataType;
use std::collections::HashMap;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

use crate::arrow1::error::WasmResult;

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
    pub fn to_ffi(&self) -> WasmResult<crate::arrow1::ffi::FFIArrowSchema> {
        Ok(crate::arrow1::ffi::FFIArrowSchema::try_from(self)?)
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
            DataType::Null => crate::arrow1::datatype::Null.into(),
            DataType::Boolean => crate::arrow1::datatype::Boolean.into(),
            DataType::Int8 => crate::arrow1::datatype::Int8.into(),
            DataType::Int16 => crate::arrow1::datatype::Int16.into(),
            DataType::Int32 => crate::arrow1::datatype::Int32.into(),
            DataType::Int64 => crate::arrow1::datatype::Int64.into(),
            DataType::UInt8 => crate::arrow1::datatype::UInt8.into(),
            DataType::UInt16 => crate::arrow1::datatype::UInt16.into(),
            DataType::UInt32 => crate::arrow1::datatype::UInt32.into(),
            DataType::UInt64 => crate::arrow1::datatype::UInt64.into(),
            DataType::Float16 => crate::arrow1::datatype::Float16.into(),
            DataType::Float32 => crate::arrow1::datatype::Float32.into(),
            DataType::Float64 => crate::arrow1::datatype::Float64.into(),
            DataType::Timestamp(unit, tz) => {
                crate::arrow1::datatype::Timestamp::new(unit.clone(), tz.clone()).into()
            }
            DataType::Date32 => crate::arrow1::datatype::Date32.into(),
            DataType::Date64 => crate::arrow1::datatype::Date64.into(),
            DataType::Time32(unit) => crate::arrow1::datatype::Time32::new(unit.clone()).into(),
            DataType::Time64(unit) => crate::arrow1::datatype::Time64::new(unit.clone()).into(),
            DataType::Duration(unit) => crate::arrow1::datatype::Duration::new(unit.clone()).into(),
            DataType::Interval(unit) => crate::arrow1::datatype::Interval::new(unit.clone()).into(),
            DataType::Binary => crate::arrow1::datatype::Binary.into(),
            DataType::FixedSizeBinary(size) => {
                crate::arrow1::datatype::FixedSizeBinary::new(*size).into()
            }
            DataType::LargeBinary => crate::arrow1::datatype::LargeBinary.into(),
            DataType::Utf8 => crate::arrow1::datatype::Utf8.into(),
            DataType::LargeUtf8 => crate::arrow1::datatype::LargeUtf8.into(),
            DataType::List(field) => crate::arrow1::datatype::List::new(field.clone()).into(),
            DataType::FixedSizeList(field, size) => {
                crate::arrow1::datatype::FixedSizeList::new(field.clone(), *size).into()
            }
            DataType::LargeList(field) => {
                crate::arrow1::datatype::LargeList::new(field.clone()).into()
            }
            DataType::Struct(fields) => crate::arrow1::datatype::Struct::new(fields.clone()).into(),
            DataType::Union(fields, mode) => {
                crate::arrow1::datatype::Union::new(fields.clone(), *mode).into()
            }
            DataType::Dictionary(key_type, value_type) => {
                crate::arrow1::datatype::Dictionary::new(key_type.clone(), value_type.clone())
                    .into()
            }
            DataType::Decimal128(precision, scale) => {
                crate::arrow1::datatype::Decimal128::new(*precision, *scale).into()
            }
            DataType::Decimal256(precision, scale) => {
                crate::arrow1::datatype::Decimal256::new(*precision, *scale).into()
            }
            DataType::Map(field, sorted) => {
                crate::arrow1::datatype::Map_::new(field.clone(), *sorted).into()
            }
            DataType::RunEndEncoded(run_ends, values) => {
                crate::arrow1::datatype::RunEndEncoded::new(run_ends.clone(), values.clone()).into()
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
