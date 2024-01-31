use std::sync::Arc;

use arrow::array::Array;
use arrow::ffi;
use arrow::ffi_stream;
use wasm_bindgen::prelude::*;

use crate::error::{Result, WasmResult};
use crate::{Field, Schema};


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
