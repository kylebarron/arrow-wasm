use arrow2::ffi;
use wasm_bindgen::prelude::*;

/// Wrapper an Arrow RecordBatch stored as FFI in Wasm memory.
#[wasm_bindgen]
pub struct FFIRecordBatch {
    pub(crate) field: Box<ffi::ArrowSchema>,
    pub(crate) array: Box<ffi::ArrowArray>,
}

impl FFIRecordBatch {
    pub fn new(field: Box<ffi::ArrowSchema>, array: Box<ffi::ArrowArray>) -> Self {
        Self { field, array }
    }
}

#[wasm_bindgen]
impl FFIRecordBatch {
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
        self.array.as_ref() as *const _
    }

    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::ArrowSchema {
        self.field.as_ref() as *const _
    }
}
