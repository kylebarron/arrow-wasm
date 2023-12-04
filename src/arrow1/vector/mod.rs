mod ffi;
use wasm_bindgen::prelude::*;

macro_rules! impl_vector {
    ($struct_name:ident, $arrow_type:ty) => {
        #[wasm_bindgen]
        pub struct $struct_name(Vec<$arrow_type>);
    };
}

impl_vector!(Uint8Vector, arrow::array::UInt8Array);
impl_vector!(Uint16Vector, arrow::array::UInt16Array);
impl_vector!(Uint32Vector, arrow::array::UInt32Array);
impl_vector!(Uint64Vector, arrow::array::UInt64Array);
impl_vector!(Int8Vector, arrow::array::Int8Array);
impl_vector!(Int16Vector, arrow::array::Int16Array);
impl_vector!(Int32Vector, arrow::array::Int32Array);
impl_vector!(Int64Vector, arrow::array::Int64Array);
impl_vector!(Float32Vector, arrow::array::Float32Array);
impl_vector!(Float64Vector, arrow::array::Float64Array);

impl_vector!(Utf8Vector, arrow::array::StringArray);
impl_vector!(LargeUtf8Vector, arrow::array::LargeStringArray);
impl_vector!(ListVector, arrow::array::ListArray);
impl_vector!(LargeListVector, arrow::array::LargeListArray);
impl_vector!(BinaryVector, arrow::array::BinaryArray);
impl_vector!(LargeBinaryVector, arrow::array::LargeBinaryArray);
