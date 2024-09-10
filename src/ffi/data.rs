use arrow::array::{Array, StructArray};
use arrow::ffi;
use arrow_schema::{ArrowError, Field};
use wasm_bindgen::prelude::*;

use crate::error::Result;
use crate::ArrowWasmError;

/// An Arrow array exported to FFI.
///
/// Using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi), you can view or copy Arrow
/// these objects to JavaScript.
///
/// Note that this also includes an ArrowSchema C struct as well, so that extension type
/// information can be maintained.

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
/// ## Memory management
///
/// Note that this array will not be released automatically. You need to manually call `.free()` to
/// release memory.
#[wasm_bindgen]
pub struct FFIData {
    array: Box<ffi::FFI_ArrowArray>,
    field: Box<ffi::FFI_ArrowSchema>,
}

impl FFIData {
    pub fn new(array: Box<ffi::FFI_ArrowArray>, field: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self { array, field }
    }

    /// Construct an [FFIData] from an Arrow array and optionally a field.
    ///
    /// In the Rust Arrow implementation, arrays do not store associated fields, so exporting an
    /// `Arc<dyn Array>` to this [`FFIData`] will infer a "default field" for the given data type.
    /// This is not sufficient for some Arrow data, such as with extension types, where custom
    /// field metadata is required.
    pub fn from_arrow(
        array: &dyn Array,
        field: impl TryInto<arrow::ffi::FFI_ArrowSchema, Error = ArrowError>,
    ) -> Result<Self> {
        let ffi_field: arrow::ffi::FFI_ArrowSchema = field.try_into()?;
        let ffi_array = arrow::ffi::FFI_ArrowArray::new(&array.to_data());

        Ok(Self {
            array: Box::new(ffi_array),
            field: Box::new(ffi_field),
        })
    }
}

impl TryFrom<&dyn Array> for FFIData {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        let ffi_field = ffi::FFI_ArrowSchema::try_from(value.data_type())?;
        let ffi_array = ffi::FFI_ArrowArray::new(&value.to_data());
        Ok(Self {
            field: Box::new(ffi_field),
            array: Box::new(ffi_array),
        })
    }
}

impl TryFrom<&arrow::record_batch::RecordBatch> for FFIData {
    type Error = ArrowWasmError;

    fn try_from(
        value: &arrow::record_batch::RecordBatch,
    ) -> std::result::Result<Self, Self::Error> {
        let field = Field::new_struct("", value.schema_ref().fields().clone(), false);
        let data = StructArray::from(value.clone());
        Self::from_arrow(&data, field)
    }
}

#[wasm_bindgen]
impl FFIData {
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
    pub fn array_addr(&self) -> *const ffi::FFI_ArrowArray {
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
    pub fn schema_addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.field.as_ref() as *const _
    }
}
