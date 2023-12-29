use crate::arrow1::data::*;

use arrow::datatypes::ArrowPrimitiveType;
use wasm_bindgen::prelude::*;

macro_rules! impl_to_typed_array {
    ($data_name:ty, $arrow_type:ty) => {
        #[wasm_bindgen]
        impl $data_name {
            /// Convert the values of this `Data` instance to a typed array in the JavaScript heap.
            ///
            /// This will silently ignore any null values.
            ///
            /// Note this doesn't take the offset of this array into account.
            #[wasm_bindgen]
            pub fn to_typed_array(&self) -> Vec<<$arrow_type as ArrowPrimitiveType>::Native> {
                self.0.values().to_vec()
            }
        }
    };
}

impl_to_typed_array!(Uint8Data, arrow::datatypes::UInt8Type);
impl_to_typed_array!(Uint16Data, arrow::datatypes::UInt16Type);
impl_to_typed_array!(Uint32Data, arrow::datatypes::UInt32Type);
impl_to_typed_array!(Uint64Data, arrow::datatypes::UInt64Type);
impl_to_typed_array!(Int8Data, arrow::datatypes::Int8Type);
impl_to_typed_array!(Int16Data, arrow::datatypes::Int16Type);
impl_to_typed_array!(Int32Data, arrow::datatypes::Int32Type);
impl_to_typed_array!(Int64Data, arrow::datatypes::Int64Type);
impl_to_typed_array!(Float32Data, arrow::datatypes::Float32Type);
impl_to_typed_array!(Float64Data, arrow::datatypes::Float64Type);
