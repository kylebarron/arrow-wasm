use arrow2::array::Array;
use arrow2::ffi;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct FFIVector {
    field: Box<ffi::ArrowSchema>,
    array: Box<ffi::ArrowArray>,
}

impl FFIVector {
    pub fn new(field: Box<ffi::ArrowSchema>, array: Box<ffi::ArrowArray>) -> Self {
        Self { field, array }
    }

    pub fn from_array(array: Box<dyn Array>, field: &arrow2::datatypes::Field) -> Self {
        Self {
            field: Box::new(ffi::export_field_to_c(field)),
            array: Box::new(ffi::export_array_to_c(array)),
        }
    }
}

#[wasm_bindgen]
impl FFIVector {
    #[wasm_bindgen]
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
        self.array.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn field_addr(&self) -> *const ffi::ArrowSchema {
        self.field.as_ref() as *const _
    }
}
