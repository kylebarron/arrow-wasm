use crate::ffi::array::FFIArrowArray;
use crate::ffi::schema::FFIArrowSchema;
use arrow::array::{Array, StructArray};
use arrow::ffi::{self, to_ffi};
use wasm_bindgen::prelude::*;

/// A representation of an Arrow RecordBatch in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
#[wasm_bindgen]
pub struct FFIRecordBatch {
    array: FFIArrowArray,
    schema: FFIArrowSchema,
}

impl FFIRecordBatch {
    pub fn new(array: FFIArrowArray, schema: FFIArrowSchema) -> Self {
        Self { schema, array }
    }
}

#[wasm_bindgen]
impl FFIRecordBatch {
    /// Access the pointer to the
    /// [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions)
    /// struct. This can be viewed or copied (without serialization) to an Arrow JS `RecordBatch` by
    /// using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi). You can access the
    /// [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory)
    /// instance by using {@linkcode wasmMemory}.
    ///
    /// **Example**:
    ///
    /// ```ts
    /// import { parseRecordBatch } from "arrow-js-ffi";
    ///
    /// const wasmRecordBatch: FFIRecordBatch = ...
    /// const wasmMemory: WebAssembly.Memory = wasmMemory();
    ///
    /// // Pass `true` to copy arrays across the boundary instead of creating views.
    /// const jsRecordBatch = parseRecordBatch(
    ///   wasmMemory.buffer,
    ///   wasmRecordBatch.arrayAddr(),
    ///   wasmRecordBatch.schemaAddr(),
    ///   true
    /// );
    /// ```
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self) -> *const ffi::FFI_ArrowArray {
        self.array.addr()
    }

    /// Access the pointer to the
    /// [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions)
    /// struct. This can be viewed or copied (without serialization) to an Arrow JS `Field` by
    /// using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi). You can access the
    /// [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory)
    /// instance by using {@linkcode wasmMemory}.
    ///
    /// **Example**:
    ///
    /// ```ts
    /// import { parseRecordBatch } from "arrow-js-ffi";
    ///
    /// const wasmRecordBatch: FFIRecordBatch = ...
    /// const wasmMemory: WebAssembly.Memory = wasmMemory();
    ///
    /// // Pass `true` to copy arrays across the boundary instead of creating views.
    /// const jsRecordBatch = parseRecordBatch(
    ///   wasmMemory.buffer,
    ///   wasmRecordBatch.arrayAddr(),
    ///   wasmRecordBatch.schemaAddr(),
    ///   true
    /// );
    /// ```
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.schema.addr()
    }
}

impl From<arrow::record_batch::RecordBatch> for FFIRecordBatch {
    fn from(value: arrow::record_batch::RecordBatch) -> Self {
        let struct_array: StructArray = value.into();
        let data = struct_array.into_data();
        let (out_array, out_schema) = to_ffi(&data).unwrap();
        Self::new(
            FFIArrowArray::new(Box::new(out_array)),
            FFIArrowSchema::new(Box::new(out_schema)),
        )
    }
}

impl From<&arrow::record_batch::RecordBatch> for FFIRecordBatch {
    fn from(value: &arrow::record_batch::RecordBatch) -> Self {
        value.clone().into()
    }
}
