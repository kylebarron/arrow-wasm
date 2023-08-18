use std::io::Cursor;

use crate::arrow2::error::WasmResult;
use crate::arrow2::{RecordBatch, Schema};
use arrow2::array::Array;
use arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader};
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Table {
    schema: arrow2::datatypes::Schema,
    batches: Vec<arrow2::chunk::Chunk<Box<dyn Array>>>,
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
