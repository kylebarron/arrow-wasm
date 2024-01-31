use crate::ffi::array::FFIArrowArray;
use crate::ffi::schema::FFIArrowSchema;
use arrow::ffi;
use wasm_bindgen::prelude::*;

/// A representation of an Arrow Table in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
#[wasm_bindgen]
pub struct FFITable {
    schema: FFIArrowSchema,
    batches: Vec<FFIArrowArray>,
}

impl FFITable {
    pub fn new(schema: FFIArrowSchema, batches: Vec<FFIArrowArray>) -> Self {
        Self { schema, batches }
    }
}

#[wasm_bindgen]
impl FFITable {
    /// Get the total number of record batches in the table
    #[wasm_bindgen(js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Get the pointer to one ArrowSchema FFI struct
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.schema.addr()
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    ///
    /// Access the pointer to one
    /// [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions)
    /// struct representing one of the internal `RecordBatch`es. This can be viewed or copied (without serialization) to an Arrow JS `RecordBatch` by
    /// using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi). You can access the
    /// [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory)
    /// instance by using {@linkcode wasmMemory}.
    ///
    /// **Example**:
    ///
    /// ```ts
    /// import * as arrow from "apache-arrow";
    /// import { parseRecordBatch } from "arrow-js-ffi";
    ///
    /// const wasmTable: FFITable = ...
    /// const wasmMemory: WebAssembly.Memory = wasmMemory();
    ///
    /// const jsBatches: arrow.RecordBatch[] = []
    /// for (let i = 0; i < wasmTable.numBatches(); i++) {
    ///   // Pass `true` to copy arrays across the boundary instead of creating views.
    ///   const jsRecordBatch = parseRecordBatch(
    ///     wasmMemory.buffer,
    ///     wasmTable.arrayAddr(i),
    ///     wasmTable.schemaAddr(),
    ///     true
    ///   );
    ///   jsBatches.push(jsRecordBatch);
    /// }
    /// const jsTable = new arrow.Table(jsBatches);
    /// ```
    ///
    /// @param chunk number The chunk index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, chunk: usize) -> *const ffi::FFI_ArrowArray {
        self.batches[chunk].addr()
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.schema);
        drop(self.batches);
    }
}
