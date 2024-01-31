use arrow::array::Array;
use arrow::ffi;
use wasm_bindgen::prelude::*;

use crate::error::Result;

#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::FFI_ArrowArray>);

impl FFIArrowArray {
    pub fn new(array: Box<ffi::FFI_ArrowArray>) -> Self {
        Self(array)
    }
}

impl From<Box<ffi::FFI_ArrowArray>> for FFIArrowArray {
    fn from(value: Box<ffi::FFI_ArrowArray>) -> Self {
        Self(value)
    }
}

impl From<ffi::FFI_ArrowArray> for FFIArrowArray {
    fn from(value: ffi::FFI_ArrowArray) -> Self {
        Self(Box::new(value))
    }
}

impl TryFrom<&dyn Array> for FFIArrowArray {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        let ffi_array = ffi::FFI_ArrowArray::new(&value.to_data());
        Ok(Self(Box::new(ffi_array)))
    }
}

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowArray {
        self.0.as_ref() as *const _
    }
}
