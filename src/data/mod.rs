mod typed_array;

use std::sync::Arc;

use crate::error::WasmResult;
use crate::ffi::FFIData;
use arrow::array::{make_array, Array, ArrayRef};
use arrow_schema::{Field, FieldRef};
use wasm_bindgen::prelude::*;

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
    #[wasm_bindgen]
    pub fn to_ffi(&self) -> WasmResult<FFIData> {
        let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(&self.field)?;
        let ffi_array = arrow::ffi::FFI_ArrowArray::new(&self.array.to_data());
        Ok(FFIData::new(Box::new(ffi_array), Box::new(ffi_schema)))
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
