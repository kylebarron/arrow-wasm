use std::sync::Arc;

use arrow::array::Array;
use arrow::ffi;
use wasm_bindgen::prelude::*;

use crate::error::{Result, WasmResult};
use crate::ffi::FFIArrowSchema;

/// A chunked Arrow array including associated field metadata
#[wasm_bindgen]
pub struct FFIVector {
    field: FFIArrowSchema,
    chunks: Vec<ffi::FFI_ArrowArray>,
}

impl FFIVector {
    pub fn from_raw(field: FFIArrowSchema, chunks: Vec<ffi::FFI_ArrowArray>) -> Self {
        Self { field, chunks }
    }

    /// Construct an FFIVector from array chunks and optionally from the provided field.
    ///
    /// For now, the field is inferred from the first chunk if not provided.
    ///
    // TODO: validate that each chunk has the same data types?
    pub fn from_arrow(
        chunks: Vec<Arc<dyn Array>>,
        field: Option<impl Into<FFIArrowSchema>>,
    ) -> Result<Self> {
        let mut ffi_field: Option<FFIArrowSchema> = field.map(|f| f.into());
        let mut ffi_chunks = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let (ffi_array, ffi_schema) = ffi::to_ffi(&chunk.to_data())?;
            if ffi_field.is_none() {
                ffi_field = Some(FFIArrowSchema::new(Box::new(ffi_schema)));
            }
            ffi_chunks.push(ffi_array);
        }

        Ok(Self {
            field: ffi_field.unwrap(),
            chunks: ffi_chunks,
        })
    }
}

#[wasm_bindgen]
impl FFIVector {
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.field.addr()
    }

    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, i: usize) -> WasmResult<*const ffi::FFI_ArrowArray> {
        Ok(self.chunks.get(i).unwrap() as *const _)
    }
}
