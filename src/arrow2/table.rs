use crate::arrow2::error::WasmResult;
use crate::arrow2::{FFIRecordBatch, RecordBatch, Schema};
use arrow2::array::Array;
use arrow2::ffi;
use arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader};
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Table {
    schema: arrow2::datatypes::Schema,
    batches: Vec<arrow2::chunk::Chunk<Box<dyn Array>>>,
}

impl Table {
    pub fn new(
        schema: arrow2::datatypes::Schema,
        batches: Vec<arrow2::chunk::Chunk<Box<dyn Array>>>,
    ) -> Self {
        Self { schema, batches }
    }
}

#[wasm_bindgen]
impl Table {
    /// Returns the schema of the record batches.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.schema.clone())
    }

    #[wasm_bindgen(js_name = recordBatch)]
    pub fn record_batch(&self, index: usize) -> Option<RecordBatch> {
        let batch = self.batches.get(index)?;

        Some(RecordBatch::new(self.schema.clone(), batch.clone()))
    }

    /// Return the number of batches in the file
    #[wasm_bindgen(getter, js_name = numBatches)]
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFITable> {
        Ok(self.into())
    }

    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFITable> {
        Ok(self.into())
    }

    /// Consume this table and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPC)]
    pub fn into_ipc(self) -> WasmResult<Vec<u8>> {
        // Create IPC writer
        let mut output_file = Vec::new();
        let options = IPCWriteOptions { compression: None };
        let mut writer = IPCStreamWriter::new(&mut output_file, options);
        writer.start(&self.schema, None)?;

        // Iterate over chunks, writing each into the IPC writer
        for chunk in self.batches {
            writer.write(&chunk, None)?;
        }

        writer.finish()?;
        Ok(output_file)
    }

    /// Create a table from an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = fromIPC)]
    pub fn from_ipc(buf: Vec<u8>) -> WasmResult<Table> {
        let mut input_file = Cursor::new(buf);
        let stream_metadata = read_file_metadata(&mut input_file)?;
        let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None, None);

        let schema = stream_metadata.schema.clone();
        let mut batches = vec![];

        for maybe_chunk in arrow_ipc_reader {
            let chunk = maybe_chunk?;
            batches.push(chunk);
        }

        Ok(Self { schema, batches })
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
    pub fn schema_addr(&self) -> *const ffi::ArrowSchema {
        // Note: this assumes that every record batch has the same schema
        self.0[0].field_addr()
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

impl From<Table> for FFITable {
    fn from(value: Table) -> Self {
        let num_batches = value.num_batches();
        let mut ffi_batches = Vec::with_capacity(num_batches);

        for i in 0..num_batches {
            let batch = value.record_batch(i).unwrap();
            ffi_batches.push(batch.into());
        }

        Self(ffi_batches)
    }
}

impl From<&Table> for FFITable {
    fn from(value: &Table) -> Self {
        let num_batches = value.num_batches();
        let mut ffi_batches = Vec::with_capacity(num_batches);

        for i in 0..num_batches {
            let batch = value.record_batch(i).unwrap();
            ffi_batches.push(batch.into());
        }

        Self(ffi_batches)
    }
}

// impl FFITable {
//     pub fn import(self) -> Result<(Schema, ArrowTable)> {
//         todo!()
//         // let schema: Schema = self.schema.as_ref().try_into()?;
//         // let data_types: Vec<&DataType> = schema
//         //     .fields
//         //     .iter()
//         //     .map(|field| field.data_type())
//         //     .collect();

//         // let mut chunks: Vec<Chunk<Box<dyn Array>>> = vec![];
//         // for chunk in self.chunks.into_iter() {
//         //     let imported = chunk.import(&data_types)?;
//         //     chunks.push(imported);
//         // }

//         // Ok((schema, chunks))
//     }
// }
