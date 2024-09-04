use crate::error::WasmResult;
use crate::ffi::FFIData;
use crate::ArrowWasmError;
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
    #[cfg(feature = "schema")]
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> crate::Schema {
        crate::Schema::new(self.0.schema())
    }

    /// Export this RecordBatch to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does not consume** the RecordBatch, so you must remember to call {@linkcode
    /// RecordBatch.free} to release the resources. The underlying arrays are reference counted, so
    /// this method does not copy data, it only prevents the data from being released.
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFIData> {
        Ok((&self.0).try_into()?)
    }

    /// Export this RecordBatch to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does consume** the RecordBatch, so the original RecordBatch will be
    /// inaccessible after this call. You must still call {@linkcode FFIRecordBatch.free} after
    /// you've finished using the FFIRecordBatch.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFIData> {
        Ok((&self.0).try_into()?)
    }

    /// Consume this RecordBatch and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let table = crate::Table::new(self.0.schema(), vec![self.0]);
        table.into_ipc_stream()
    }

    /// Override the schema of this [`RecordBatch`]
    ///
    /// Returns an error if `schema` is not a superset of the current schema
    /// as determined by [`Schema::contains`]
    #[cfg(feature = "schema")]
    #[wasm_bindgen(js_name = withSchema)]
    pub fn with_schema(&self, schema: crate::Schema) -> WasmResult<RecordBatch> {
        Ok(self.0.clone().with_schema(schema.0)?.into())
    }

    /// Return a new RecordBatch where each column is sliced
    /// according to `offset` and `length`
    #[wasm_bindgen]
    pub fn slice(&self, offset: usize, length: usize) -> RecordBatch {
        self.0.slice(offset, length).into()
    }

    /// Returns the total number of bytes of memory occupied physically by this batch.
    #[wasm_bindgen(js_name = getArrayMemorySize)]
    pub fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
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

impl TryFrom<RecordBatch> for FFIData {
    type Error = ArrowWasmError;

    fn try_from(value: RecordBatch) -> Result<Self, ArrowWasmError> {
        (&value.0).try_into()
    }
}

impl TryFrom<&RecordBatch> for FFIData {
    type Error = ArrowWasmError;

    fn try_from(value: &RecordBatch) -> Result<Self, ArrowWasmError> {
        (&value.0).try_into()
    }
}

impl AsRef<arrow::record_batch::RecordBatch> for RecordBatch {
    fn as_ref(&self) -> &arrow::record_batch::RecordBatch {
        &self.0
    }
}
