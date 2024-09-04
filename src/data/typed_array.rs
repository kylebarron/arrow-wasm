use crate::data::*;
use crate::ArrowWasmError;

use arrow::array::AsArray;
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_schema::DataType;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
impl Data {
    /// Copy the values of this `Data` instance to a TypedArray in the JavaScript heap.
    ///
    /// This will silently ignore any null values. This will error on non-primitive data types for
    /// which a TypedArray does not exist in JavaScript.
    #[wasm_bindgen]
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
