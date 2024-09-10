use std::sync::Arc;

use arrow::array::AsArray;
use arrow::array::{make_array, Array, ArrayRef};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_schema::DataType;
use arrow_schema::{Field, FieldRef};
use wasm_bindgen::prelude::*;

use crate::error::{ArrowWasmError, WasmResult};
use crate::ffi::FFIData;

/// A representation of an Arrow `Data` instance in WebAssembly memory.
///
/// This has the same underlying representation as an Arrow JS `Data` object.
#[wasm_bindgen]
pub struct Data {
    array: ArrayRef,
    field: FieldRef,
}

#[wasm_bindgen]
impl Data {
    /// Export this to FFI.
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFIData> {
        let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(&self.field)?;
        let ffi_array = arrow::ffi::FFI_ArrowArray::new(&self.array.to_data());
        Ok(FFIData::new(Box::new(ffi_array), Box::new(ffi_schema)))
    }

    /// Copy the values of this `Data` instance to a TypedArray in the JavaScript heap.
    ///
    /// This will silently ignore any null values. This will error on non-primitive data types for
    /// which a TypedArray does not exist in JavaScript.
    #[wasm_bindgen(js_name = toTypedArray)]
    pub fn to_typed_array(&self) -> WasmResult<JsValue> {
        macro_rules! impl_to_typed_array {
            ($arrow_type:ty, $js_array:ty) => {{
                let values = self.array.as_primitive::<$arrow_type>().values().as_ref();
                Ok(<$js_array>::from(values).into())
            }};
        }

        match self.array.data_type() {
            DataType::UInt8 => impl_to_typed_array!(UInt8Type, js_sys::Uint8Array),
            DataType::UInt16 => impl_to_typed_array!(UInt16Type, js_sys::Uint16Array),
            DataType::UInt32 => impl_to_typed_array!(UInt32Type, js_sys::Uint32Array),
            DataType::UInt64 => impl_to_typed_array!(UInt64Type, js_sys::BigUint64Array),
            DataType::Int8 => impl_to_typed_array!(Int8Type, js_sys::Int8Array),
            DataType::Int16 => impl_to_typed_array!(Int16Type, js_sys::Int16Array),
            DataType::Int32 => impl_to_typed_array!(Int32Type, js_sys::Int32Array),
            DataType::Int64 => impl_to_typed_array!(Int64Type, js_sys::BigInt64Array),
            DataType::Float32 => impl_to_typed_array!(Float32Type, js_sys::Float32Array),
            DataType::Float64 => impl_to_typed_array!(Float64Type, js_sys::Float64Array),
            dt => {
                Err(ArrowWasmError::InternalError(format!("Unexpected data type: {}", dt)).into())
            }
        }
    }
}

impl Data {
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        assert_eq!(array.data_type(), field.data_type());
        Self { array, field }
    }

    pub fn from_array<A: Array>(array: A) -> Self {
        let array = make_array(array.into_data());
        Self::from_array_ref(array)
    }

    /// Create a new Data from an [ArrayRef], inferring its data type automatically.
    pub fn from_array_ref(array: ArrayRef) -> Self {
        let field = Field::new("", array.data_type().clone(), true);
        Self::new(array, Arc::new(field))
    }
}

impl From<ArrayRef> for Data {
    fn from(array: ArrayRef) -> Self {
        Self::from_array_ref(array)
    }
}

impl AsRef<ArrayRef> for Data {
    fn as_ref(&self) -> &ArrayRef {
        &self.array
    }
}
