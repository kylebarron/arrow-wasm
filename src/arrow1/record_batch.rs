use crate::arrow1::error::WasmResult;
use crate::arrow1::{Schema, Table};
use arrow::array::{Array, StructArray};
use arrow::ffi::{self, to_ffi};
use wasm_bindgen::prelude::*;

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
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    #[wasm_bindgen(getter, js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// Returns the schema of the record batches.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.0.schema())
    }

    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFIRecordBatch> {
        Ok(self.into())
    }

    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFIRecordBatch> {
        Ok(self.into())
    }

    /// Consume this record batch and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPC)]
    pub fn into_ipc(self) -> WasmResult<Vec<u8>> {
        let table = Table::new(vec![self.0]);
        table.into_ipc()
    }
}

/// Wrapper an Arrow RecordBatch stored as FFI in Wasm memory.
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
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self) -> *const ffi::FFI_ArrowArray {
        self.array.as_ref() as *const _
    }

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
        value.clone().into()
    }
}
