use crate::arrow1::error::WasmResult;
use crate::arrow1::Schema;
use crate::arrow1::{FFIRecordBatch, RecordBatch};
use arrow::ffi;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Table(Vec<arrow::record_batch::RecordBatch>);

impl Table {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self(batches)
    }

    pub fn into_inner(self) -> Vec<arrow::record_batch::RecordBatch> {
        self.0
    }
}

#[wasm_bindgen]
impl Table {
    /// Returns the schema of the record batches.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.0[0].schema())
    }

    #[wasm_bindgen(js_name = recordBatch)]
    pub fn record_batch(&self, index: usize) -> Option<RecordBatch> {
        let batch = self.0.get(index)?;
        Some(RecordBatch::new(batch.clone()))
    }

    /// Return the number of batches in the file
    #[wasm_bindgen(getter, js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> FFITable {
        self.into()
    }

    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> FFITable {
        self.into()
    }

    /// Consume this table and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let mut output_file = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut output_file, &self.schema().into_inner())?;

            // Iterate over record batches, writing them to IPC stream
            for chunk in self.0 {
                writer.write(&chunk)?;
            }
            writer.finish()?;
        }

        // Note that this returns output_file directly instead of using
        // writer.into_inner().to_vec() as the latter seems likely to incur an extra copy of the
        // vec
        Ok(output_file)
    }

    /// Create a table from an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = fromIPCStream)]
    pub fn from_ipc_stream(buf: &[u8]) -> WasmResult<Table> {
        let input_file = Cursor::new(buf);
        let arrow_ipc_reader = StreamReader::try_new(input_file, None)?;

        let mut batches = vec![];
        for maybe_chunk in arrow_ipc_reader {
            let chunk = maybe_chunk?;
            batches.push(chunk);
        }

        Ok(Self(batches))
    }
}

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
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        // Note: this assumes that every record batch has the same schema
        self.0[0].schema_addr()
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param chunk number The chunk index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, chunk: usize) -> *const ffi::FFI_ArrowArray {
        self.0[chunk].array_addr()
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0);
    }
}

impl From<Table> for FFITable {
    fn from(value: Table) -> Self {
        let num_batches = value.num_batches();
        let mut ffi_batches = Vec::with_capacity(num_batches);

        for batch in value.0.into_iter() {
            ffi_batches.push(batch.into());
        }

        Self(ffi_batches)
    }
}

impl From<&Table> for FFITable {
    fn from(value: &Table) -> Self {
        let num_batches = value.num_batches();
        let mut ffi_batches = Vec::with_capacity(num_batches);

        for batch in value.0.iter() {
            ffi_batches.push(batch.into());
        }

        Self(ffi_batches)
    }
}
