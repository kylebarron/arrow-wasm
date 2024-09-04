use arrow::ffi;
use wasm_bindgen::convert::IntoWasmAbi;
use wasm_bindgen::prelude::*;

use crate::ffi::FFISchema;

/// A representation of an Arrow C Stream in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
///
/// Unlike other Arrow implementations outside of JS, this always stores the "stream" fully
/// materialized as a sequence of Arrow chunks.
#[wasm_bindgen]
pub struct FFIStream {
    pub(crate) field: FFISchema,
    pub(crate) arrays: Vec<ffi::FFI_ArrowArray>,
}

impl FFIStream {
    pub fn new(field: Box<ffi::FFI_ArrowSchema>, arrays: Vec<ffi::FFI_ArrowArray>) -> Self {
        Self {
            field: FFISchema::new(field),
            arrays,
        }
    }
}

#[wasm_bindgen]
impl FFIStream {
    /// Get the total number of elements in this stream
    #[wasm_bindgen(js_name = numArrays)]
    pub fn num_arrays(&self) -> usize {
        self.arrays.len()
    }

    /// Get the pointer to the ArrowSchema FFI struct
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.field.addr()
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
        &self.arrays[chunk] as *const _
    }

    #[wasm_bindgen(js_name = arrayAddrs)]
    pub fn array_addrs(&self) -> Vec<u32> {
        // wasm-bindgen doesn't allow a Vec<*const ffi::FFI_ArrowArray> so we cast to u32
        self.arrays
            .iter()
            .map(|array| (array as *const ffi::FFI_ArrowArray).into_abi())
            .collect()
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.field);
        drop(self.arrays);
    }
}
