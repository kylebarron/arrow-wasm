use arrow::array::Array;
use arrow::ffi;
use wasm_bindgen::prelude::*;

use crate::error::Result;
use crate::ffi::{FFIArrowArray, FFIArrowSchema};

/// An Arrow array including associated field metadata.
///
/// Using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi), you can view or copy Arrow
/// these objects to JavaScript.
///

// TODO: fix example
// ```ts
// import { parseField, parseVector } from "arrow-js-ffi";
//
// // You need to access the geoarrow webassembly memory space.
// // The way to do this is different per geoarrow bundle method.
// const WASM_MEMORY: WebAssembly.Memory = geoarrow.__wasm.memory;
//
// // Say we have a point array from somewhere
// const pointArray: geoarrow.PointArray = ...;
//
// // Export this existing point array to wasm.
// const ffiArray = pointArray.toFfi();
//
// // Parse an arrow-js field object from the pointer
// const jsArrowField = parseField(WASM_MEMORY.buffer, ffiArray.field_addr());
//
// // Parse an arrow-js vector from the pointer and parsed field
// const jsPointVector = parseVector(
//   WASM_MEMORY.buffer,
//   ffiArray.array_addr(),
//   field.type
// );
// ```
//
// ## Memory management
//
// Note that this array will not be released automatically. You need to manually call `.free()` to
// release memory.
#[wasm_bindgen]
pub struct FFIData {
    array: FFIArrowArray,
    field: FFIArrowSchema,
}

impl FFIData {
    pub fn from_raw(array: FFIArrowArray, field: FFIArrowSchema) -> Self {
        Self { field, array }
    }

    /// Construct an [FFIData] from an Arrow array and optionally a field.
    ///
    /// In the Rust Arrow implementation, arrays do not store associated fields, so exporting an
    /// `Arc<dyn Array>` to this [`FFIData`] will infer a "default field" for the given data type.
    /// This is not sufficient for some Arrow data, such as with extension types, where custom
    /// field metadata is required.
    pub fn from_arrow(array: &dyn Array, field: Option<impl Into<FFIArrowSchema>>) -> Result<Self> {
        let (ffi_array, ffi_schema) = ffi::to_ffi(&array.to_data())?;
        let ffi_schema = field
            .map(|f| f.into())
            .unwrap_or_else(|| Box::new(ffi_schema).into());
        Ok(Self {
            field: ffi_schema,
            array: Box::new(ffi_array).into(),
        })
    }
}

impl TryFrom<&dyn Array> for FFIData {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        let (ffi_array, ffi_schema) = ffi::to_ffi(&value.to_data())?;
        Ok(Self {
            field: Box::new(ffi_schema).into(),
            array: Box::new(ffi_array).into(),
        })
    }
}

#[wasm_bindgen]
impl FFIData {
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self) -> *const ffi::FFI_ArrowArray {
        self.array.addr()
    }

    #[wasm_bindgen]
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.field.addr()
    }
}
