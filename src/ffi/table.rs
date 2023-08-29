use crate::ffi::FFIRecordBatch;
use wasm_bindgen::prelude::*;
// TODO: should we copy our own ffi structs in so that we aren't relying on either arrow2 or
// arrow1's ffi structs? Not sure that will work though because the structs will be considered
// different?
use arrow2::ffi;

/// Wrapper around an Arrow Table in Wasm memory (a list of FFIArrowRecordBatch objects.)
///
/// Refer to {@linkcode readParquetFFI} for instructions on how to use this.
#[wasm_bindgen]
pub struct FFITable(Vec<FFIRecordBatch>);

impl FFITable {
    pub fn new(batches: Vec<FFIRecordBatch>) -> Self {
        Self(batches)
    }
}

#[wasm_bindgen]
impl FFITable {
    /// Get the total number of record batches in the table
    #[wasm_bindgen(js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.0.len()
    }

    /// Get the pointer to one ArrowSchema FFI struct
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::ArrowSchema {
        // Note: this assumes that every record batch has the same schema
        self.0[0].schema_addr()
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param chunk number The chunk index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, chunk: usize) -> *const ffi::ArrowArray {
        self.0[chunk].array_addr()
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0);
    }
}
