use crate::arrow2::error::WasmResult;
use crate::arrow2::{FFIRecordBatch, RecordBatch, Schema};
use arrow2::array::Array;
use arrow2::ffi;
use arrow2::io::ipc::read::{
    read_file_metadata, read_stream_metadata, FileReader as IPCFileReader, StreamReader,
    StreamState,
};
use arrow2::io::ipc::write::{StreamWriter as IPCStreamWriter, WriteOptions as IPCWriteOptions};
use std::io::Cursor;
use wasm_bindgen::prelude::*;

/// A Table in WebAssembly memory conforming to the Apache Arrow spec.
///
/// A Table consists of one or more {@linkcode RecordBatch} objects plus a {@linkcode Schema} that
/// each RecordBatch conforms to.
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

    /// Consume this table and return its components
    pub fn into_inner(
        self,
    ) -> (
        arrow2::datatypes::Schema,
        Vec<arrow2::chunk::Chunk<Box<dyn Array>>>,
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

        Some(RecordBatch::new(self.schema.clone(), batch.clone()))
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
        Ok(self.into())
    }

    /// Export this Table to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does consume** the Table, so the original Table will be
    /// inaccessible after this call. You must still call {@linkcode FFITable.free} after
    /// you've finished using the FFITable.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFITable> {
        Ok(self.into())
    }

    /// Consume this table and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
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

    /// Create a table from an Arrow IPC File buffer
    #[wasm_bindgen(js_name = fromIPCFile)]
    pub fn from_ipc_file(buf: Vec<u8>) -> WasmResult<Table> {
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

    /// Create a table from an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = fromIPCStream)]
    pub fn from_ipc_stream(buf: Vec<u8>) -> WasmResult<Table> {
        let mut input_file = Cursor::new(buf);
        let stream_metadata = read_stream_metadata(&mut input_file)?;
        let stream = StreamReader::new(&mut input_file, stream_metadata.clone(), None);

        let mut batches = vec![];

        for maybe_stream_state in stream {
            match maybe_stream_state {
                Ok(StreamState::Some(chunk)) => {
                    batches.push(chunk);
                }
                Ok(StreamState::Waiting) => {
                    panic!("Expected the entire stream to be contained in input buffer")
                }
                Err(l) => return Err(l.into()),
            }
        }

        Ok(Self {
            schema: stream_metadata.schema,
            batches,
        })
    }
}

/// A representation of an Arrow Table in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
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
        self.0[0].schema_addr()
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
