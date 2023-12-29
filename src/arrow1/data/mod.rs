mod datatype;
mod typed_array;

use crate::arrow1::error::WasmResult;
use crate::arrow1::ffi::FFIArrowArray;
use arrow::array::Array;
use wasm_bindgen::prelude::*;

macro_rules! impl_data {
    ($struct_name:ident, $arrow_type:ty) => {
        #[wasm_bindgen]
        pub struct $struct_name($arrow_type);

        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn to_ffi(&self) -> WasmResult<FFIArrowArray> {
                let array_data = self.0.to_data();
                let (ffi_arr, _ffi_schema) = arrow::ffi::to_ffi(&array_data)?;
                Ok(ffi_arr.into())
            }
        }

        impl $struct_name {
            pub fn new(arr: $arrow_type) -> Self {
                Self(arr)
            }
        }

        impl From<$struct_name> for $arrow_type {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }

        impl From<$arrow_type> for $struct_name {
            fn from(value: $arrow_type) -> Self {
                Self(value)
            }
        }

        impl AsRef<$arrow_type> for $struct_name {
            fn as_ref(&self) -> &$arrow_type {
                &self.0
            }
        }
    };
}

impl_data!(BooleanData, arrow::array::BooleanArray);
impl_data!(Uint8Data, arrow::array::UInt8Array);
impl_data!(Uint16Data, arrow::array::UInt16Array);
impl_data!(Uint32Data, arrow::array::UInt32Array);
impl_data!(Uint64Data, arrow::array::UInt64Array);
impl_data!(Int8Data, arrow::array::Int8Array);
impl_data!(Int16Data, arrow::array::Int16Array);
impl_data!(Int32Data, arrow::array::Int32Array);
impl_data!(Int64Data, arrow::array::Int64Array);
impl_data!(Float32Data, arrow::array::Float32Array);
impl_data!(Float64Data, arrow::array::Float64Array);

impl_data!(Utf8Data, arrow::array::StringArray);
impl_data!(LargeUtf8Data, arrow::array::LargeStringArray);
impl_data!(ListData, arrow::array::ListArray);
impl_data!(LargeListData, arrow::array::LargeListArray);
impl_data!(BinaryData, arrow::array::BinaryArray);
impl_data!(LargeBinaryData, arrow::array::LargeBinaryArray);
