use crate::error::WasmResult;
use crate::ffi::table::FFITable;
use crate::ffi::FFIArrowSchema;
use crate::RecordBatch;
use crate::{ArrowWasmError, Schema};
use arrow::array::{Array, StructArray};
use arrow::ffi;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;
use wasm_bindgen::prelude::*;

/// A Table in WebAssembly memory conforming to the Apache Arrow spec.
///
/// A Table consists of one or more {@linkcode RecordBatch} objects plus a {@linkcode Schema} that
/// each RecordBatch conforms to.
#[derive(Debug)]
#[wasm_bindgen]
pub struct Table {
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow::record_batch::RecordBatch>,
}

impl Table {
    pub fn new(
        schema: arrow_schema::SchemaRef,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Self {
        Self { schema, batches }
    }

    /// Consume this table and return its components
    pub fn into_inner(
        self,
    ) -> (
        arrow_schema::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
    ) {
        (self.schema, self.batches)
    }
}

#[wasm_bindgen]
impl Table {
    /// Access the Table's {@linkcode Schema}.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.schema.clone())
    }

    /// Access a RecordBatch from the Table by index.
    ///
    /// @param index The positional index of the RecordBatch to retrieve.
    /// @returns a RecordBatch or `null` if out of range.
    #[wasm_bindgen(js_name = recordBatch)]
    pub fn record_batch(&self, index: usize) -> Option<RecordBatch> {
        let batch = self.batches.get(index)?;
        Some(RecordBatch::new(batch.clone()))
    }

    /// The number of batches in the Table
    #[wasm_bindgen(getter, js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Export this Table to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does not consume** the Table, so you must remember to call {@linkcode
    /// Table.free} to release the resources. The underlying arrays are reference counted, so
    /// this method does not copy data, it only prevents the data from being released.
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFITable> {
        Ok(self.try_into()?)
    }

    /// Export this Table to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does consume** the Table, so the original Table will be
    /// inaccessible after this call. You must still call {@linkcode FFITable.free} after
    /// you've finished using the FFITable.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFITable> {
        Ok(self.try_into()?)
    }

    /// Consume this table and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let mut output_file = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut output_file, &self.schema().into_inner())?;

            // Iterate over record batches, writing them to IPC stream
            for chunk in self.batches {
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
        let schema = arrow_ipc_reader.schema();

        let mut batches = vec![];
        for maybe_chunk in arrow_ipc_reader {
            let chunk = maybe_chunk?;
            batches.push(chunk);
        }

        Ok(Self::new(schema, batches))
    }

    /// Returns the total number of bytes of memory occupied physically by all batches in this
    /// table.
    #[wasm_bindgen]
    pub fn get_array_memory_size(&self) -> usize {
        self.batches
            .iter()
            .fold(0, |sum, batch| sum + batch.get_array_memory_size())
    }
}

impl TryFrom<Table> for FFITable {
    type Error = ArrowWasmError;

    fn try_from(value: Table) -> Result<Self, Self::Error> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.schema.as_ref())?;
        let ffi_schema = FFIArrowSchema::new(Box::new(ffi_schema));

        let mut ffi_batches = Vec::with_capacity(value.num_batches());
        for batch in value.batches.into_iter() {
            ffi_batches
                .push(ffi::FFI_ArrowArray::new(&StructArray::from(batch).into_data()).into());
        }

        Ok(Self::new(ffi_schema, ffi_batches))
    }
}

impl TryFrom<&Table> for FFITable {
    type Error = ArrowWasmError;

    fn try_from(value: &Table) -> Result<Self, Self::Error> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.schema.as_ref())?;
        let ffi_schema = FFIArrowSchema::new(Box::new(ffi_schema));

        let mut ffi_batches = Vec::with_capacity(value.num_batches());
        for batch in value.batches.iter() {
            ffi_batches.push(
                ffi::FFI_ArrowArray::new(&StructArray::from(batch.clone()).into_data()).into(),
            );
        }

        Ok(Self::new(ffi_schema, ffi_batches))
    }
}
