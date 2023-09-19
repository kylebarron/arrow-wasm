use arrow2::array::{Array, StructArray};
use arrow2::datatypes::{DataType, Field};
use arrow2::ffi;
use wasm_bindgen::prelude::*;

use crate::arrow2::error::WasmResult;
use crate::arrow2::{Schema, Table, Vector};

/// A group of columns of equal length in WebAssembly memory with an associated {@linkcode Schema}.
#[wasm_bindgen]
pub struct RecordBatch {
    schema: arrow2::datatypes::Schema,
    chunk: arrow2::chunk::Chunk<Box<dyn Array>>,
}

impl RecordBatch {
    pub fn new(
        schema: arrow2::datatypes::Schema,
        chunk: arrow2::chunk::Chunk<Box<dyn Array>>,
    ) -> Self {
        Self { schema, chunk }
    }

    pub fn into_inner(
        self,
    ) -> (
        arrow2::datatypes::Schema,
        arrow2::chunk::Chunk<Box<dyn Array>>,
    ) {
        (self.schema, self.chunk)
    }
}

#[wasm_bindgen]
impl RecordBatch {
    /// The number of rows in this RecordBatch
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.chunk.len()
    }

    /// The number of columns in this RecordBatch
    #[wasm_bindgen(getter, js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.chunk.columns().len()
    }

    /// The {@linkcode Schema} of this RecordBatch.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.schema.clone())
    }

    /// Get a column's vector by index.
    #[wasm_bindgen]
    pub fn column(&self, index: usize) -> Option<Vector> {
        let arr = self.chunk.columns().get(index)?;
        Some(Vector::new(arr.clone()))
    }

    /// Get a column's vector by name
    #[wasm_bindgen]
    pub fn column_by_name(&self, name: &str) -> Option<Vector> {
        let column_idx = self
            .schema
            .fields
            .iter()
            .position(|field| field.name == name)?;
        self.column(column_idx)
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
    /// you've finished using the FFI table.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFIRecordBatch> {
        Ok(self.into())
    }

    /// Consume this RecordBatch and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let table = Table::new(self.schema, vec![self.chunk]);
        table.into_ipc_stream()
    }
}

/// A representation of an Arrow RecordBatch in WebAssembly memory exposed as FFI-compatible
/// structs through the Arrow C Data Interface.
#[wasm_bindgen]
pub struct FFIRecordBatch {
    field: Box<ffi::ArrowSchema>,
    array: Box<ffi::ArrowArray>,
}

impl FFIRecordBatch {
    pub fn new(field: Box<ffi::ArrowSchema>, array: Box<ffi::ArrowArray>) -> Self {
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
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
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
    pub fn schema_addr(&self) -> *const ffi::ArrowSchema {
        self.field.as_ref() as *const _
    }
}

impl From<RecordBatch> for FFIRecordBatch {
    fn from(value: RecordBatch) -> Self {
        let data_type = DataType::Struct(value.schema.fields);
        let struct_array =
            StructArray::try_new(data_type.clone(), value.chunk.to_vec(), None).unwrap();
        let field = Field::new("", data_type, false).with_metadata(value.schema.metadata);

        Self {
            field: Box::new(ffi::export_field_to_c(&field)),
            array: Box::new(ffi::export_array_to_c(struct_array.boxed())),
        }
    }
}

impl From<&RecordBatch> for FFIRecordBatch {
    fn from(value: &RecordBatch) -> Self {
        let data_type = DataType::Struct(value.schema.fields.clone());
        let struct_array =
            StructArray::try_new(data_type.clone(), value.chunk.to_vec(), None).unwrap();
        let field = Field::new("", data_type, false).with_metadata(value.schema.metadata.clone());

        Self {
            field: Box::new(ffi::export_field_to_c(&field)),
            array: Box::new(ffi::export_array_to_c(struct_array.boxed())),
        }
    }
}

//     /// Get all columns in the RecordBatch.
//     // TODO: specify that the output type is Array<Vector>, not Array<any>
//     #[wasm_bindgen(getter)]
//     pub fn columns(&self) -> Array {
//         let vectors: Vec<vector::Vector> = self
//             .0
//             .columns()
//             .iter()
//             .map(|column| vector::Vector::new(column.clone()))
//             .collect();

//         vectors.into_iter().map(JsValue::from).collect()
//     }
