use arrow::ffi;
use arrow::ffi_stream;
use wasm_bindgen::prelude::*;

use crate::arrow1::error::WasmResult;
use crate::arrow1::{Field, Schema};

#[wasm_bindgen]
pub struct FFIArrowSchema(Box<ffi::FFI_ArrowSchema>);

impl FFIArrowSchema {
    pub fn new(schema: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self(schema)
    }
}

#[wasm_bindgen]
impl FFIArrowSchema {
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
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.0.as_ref() as *const _
    }
}

impl TryFrom<&Schema> for FFIArrowSchema {
    type Error = crate::arrow1::error::ArrowWasmError;

    fn try_from(value: &Schema) -> Result<Self, Self::Error> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.0.as_ref())?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

impl TryFrom<&Field> for FFIArrowSchema {
    type Error = crate::arrow1::error::ArrowWasmError;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.0.as_ref())?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

impl From<Box<ffi::FFI_ArrowSchema>> for FFIArrowSchema {
    fn from(value: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self(value)
    }
}

impl From<ffi::FFI_ArrowSchema> for FFIArrowSchema {
    fn from(value: ffi::FFI_ArrowSchema) -> Self {
        Self(Box::new(value))
    }
}

#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::FFI_ArrowArray>);

impl From<Box<ffi::FFI_ArrowArray>> for FFIArrowArray {
    fn from(value: Box<ffi::FFI_ArrowArray>) -> Self {
        Self(value)
    }
}

impl From<ffi::FFI_ArrowArray> for FFIArrowArray {
    fn from(value: ffi::FFI_ArrowArray) -> Self {
        Self(Box::new(value))
    }
}

#[wasm_bindgen]
pub struct FFIArrowArrayStream(Box<ffi_stream::FFI_ArrowArrayStream>);

impl FFIArrowArrayStream {
    pub fn new(stream: Box<ffi_stream::FFI_ArrowArrayStream>) -> Self {
        Self(stream)
    }
}

#[wasm_bindgen]
impl FFIArrowArrayStream {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi_stream::FFI_ArrowArrayStream {
        self.0.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn get_schema(&mut self) -> WasmResult<FFIArrowSchema> {
        let mut schema = ffi::FFI_ArrowSchema::empty();

        let stream_ptr = self.0.as_mut() as *mut ffi_stream::FFI_ArrowArrayStream;
        let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, &mut schema) };

        if ret_code == 0 {
            Ok(schema.into())
        } else {
            Err(JsError::new(
                "Cannot get schema from input stream. Error code: {ret_code:?}",
            ))
        }
    }

    #[wasm_bindgen]
    pub fn get_next(&mut self) -> WasmResult<Option<FFIArrowArray>> {
        let mut array = ffi::FFI_ArrowArray::empty();

        let stream_ptr = self.0.as_mut() as *mut ffi_stream::FFI_ArrowArrayStream;
        let ret_code = unsafe { (*stream_ptr).get_next.unwrap()(stream_ptr, &mut array) };

        if ret_code == 0 {
            // The end of stream has been reached
            if array.is_released() {
                return Ok(None);
            }

            Ok(Some(array.into()))
        } else {
            Err(JsError::new(
                "Cannot get array from input stream. Error code: {ret_code:?}",
            ))
        }
    }
}

#[wasm_bindgen]
pub struct FFIArrowArrayStreamReader(ffi_stream::ArrowArrayStreamReader);

#[wasm_bindgen]
impl FFIArrowArrayStreamReader {
    pub fn tmp(&self) {
        // self.0.schema()
    }
}

// #[wasm_bindgen]
// pub fn test_stream() -> FFIArrowArrayStream {
//     use arrow::array::UInt8Array;
//     use arrow::record_batch::{RecordBatch, RecordBatchIterator};
//     use std::sync::Arc;

//     let field_a = arrow_schema::Field::new("uint8_col", arrow_schema::DataType::UInt8, false);
//     let schema = arrow_schema::Schema::new(vec![field_a]);
//     let schema_ref = Arc::new(schema);

//     let arr1 = UInt8Array::from(vec![1, 2, 3]);
//     let arr2 = UInt8Array::from(vec![4, 5, 6]);
//     let batch1 = RecordBatch::try_new(schema_ref.clone(), vec![Arc::new(arr1)]).unwrap();
//     let batch2 = RecordBatch::try_new(schema_ref.clone(), vec![Arc::new(arr2)]).unwrap();

//     let mut stream = ffi_stream::FFI_ArrowArrayStream::empty();
//     let batches: Vec<RecordBatch> = vec![batch1, batch2];

//     let reader = Box::new(RecordBatchIterator::new(
//         batches.into_iter().map(Ok),
//         schema_ref.clone(),
//     ));
//     unsafe { ffi_stream::export_reader_into_raw(reader, &mut stream) };

//     FFIArrowArrayStream::new(Box::new(stream))
// }