use crate::arrow1::error::WasmResult;
use crate::arrow1::{Schema, Table};
use arrow::array::{Array, StructArray};
use arrow::ffi::{self, to_ffi};
use wasm_bindgen::prelude::*;

/// A group of columns of equal length in WebAssembly memory with an associated {@linkcode Schema}.
#[wasm_bindgen]
pub struct RecordBatch(arrow::record_batch::RecordBatch);

impl RecordBatch {
    pub fn new(batch: arrow::record_batch::RecordBatch) -> Self {
        Self(batch)
    }

    pub fn into_inner(self) -> arrow::record_batch::RecordBatch {
        self.0
    }
}

#[wasm_bindgen]
impl RecordBatch {
    /// The number of rows in this RecordBatch.
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    /// The number of columns in this RecordBatch.
    #[wasm_bindgen(getter, js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// The {@linkcode Schema} of this RecordBatch.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.0.schema())
    }

    /// Export this RecordBatch to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does not consume** the RecordBatch, so you must remember to call {@linkcode
    /// RecordBatch.free} to release the resources. The underlying arrays are reference counted, so
    /// this method does not copy data, it only prevents the data from being released.
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFIRecordBatch> {
        Ok(self.into())
    }

    /// Export this RecordBatch to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does consume** the RecordBatch, so the original RecordBatch will be
    /// inaccessible after this call. You must still call {@linkcode FFIRecordBatch.free} after
    /// you've finished using the FFIRecordBatch.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFIRecordBatch> {
        Ok(self.into())
    }

    /// Consume this RecordBatch and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let table = Table::new(vec![self.0]);
        table.into_ipc_stream()
    }
}

/// A representation of an Arrow RecordBatch in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
#[wasm_bindgen]
pub struct FFIRecordBatch {
    array: Box<ffi::FFI_ArrowArray>,
    field: Box<ffi::FFI_ArrowSchema>,
}

impl FFIRecordBatch {
    pub fn new(array: Box<ffi::FFI_ArrowArray>, field: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self { field, array }
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
        self.array.as_ref() as *const _
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
        self.field.as_ref() as *const _
    }
}

impl From<arrow::record_batch::RecordBatch> for RecordBatch {
    fn from(value: arrow::record_batch::RecordBatch) -> Self {
        Self(value)
    }
}

impl From<RecordBatch> for arrow::record_batch::RecordBatch {
    fn from(value: RecordBatch) -> Self {
        value.0
    }
}

impl From<arrow::record_batch::RecordBatch> for FFIRecordBatch {
    fn from(value: arrow::record_batch::RecordBatch) -> Self {
        let struct_array: StructArray = value.into();
        let data = struct_array.into_data();
        let (out_array, out_schema) = to_ffi(&data).unwrap();
        Self::new(Box::new(out_array), Box::new(out_schema))
    }
}

impl From<&arrow::record_batch::RecordBatch> for FFIRecordBatch {
    fn from(value: &arrow::record_batch::RecordBatch) -> Self {
        value.clone().into()
    }
}

impl From<RecordBatch> for FFIRecordBatch {
    fn from(value: RecordBatch) -> Self {
        value.0.into()
    }
}

impl From<&RecordBatch> for FFIRecordBatch {
    fn from(value: &RecordBatch) -> Self {
        value.into()
    }
}
