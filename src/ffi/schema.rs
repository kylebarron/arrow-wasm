use arrow::ffi;
use arrow_schema::ArrowError;
use wasm_bindgen::prelude::*;

use crate::error::Result;

#[wasm_bindgen]
pub struct FFISchema(Box<ffi::FFI_ArrowSchema>);

impl FFISchema {
    pub fn new(schema: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self(schema)
    }

    /// Construct an [FFIData] from an Arrow array and optionally a field.
    ///
    /// In the Rust Arrow implementation, arrays do not store associated fields, so exporting an
    /// `Arc<dyn Array>` to this [`FFIData`] will infer a "default field" for the given data type.
    /// This is not sufficient for some Arrow data, such as with extension types, where custom
    /// field metadata is required.
    pub fn from_arrow(
        field: impl TryInto<arrow::ffi::FFI_ArrowSchema, Error = ArrowError>,
    ) -> Result<Self> {
        let ffi_field: arrow::ffi::FFI_ArrowSchema = field.try_into()?;
        Ok(Self::new(Box::new(ffi_field)))
    }
}

#[wasm_bindgen]
impl FFISchema {
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

impl TryFrom<&arrow_schema::Schema> for FFISchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &arrow_schema::Schema) -> Result<Self> {
        Self::from_arrow(value)
    }
}

impl TryFrom<&arrow_schema::Field> for FFISchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &arrow_schema::Field) -> Result<Self> {
        Self::from_arrow(value)
    }
}

#[cfg(feature = "schema")]
impl TryFrom<&crate::Schema> for FFISchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &crate::Schema) -> Result<Self> {
        Self::from_arrow(value.as_ref())
    }
}

#[cfg(feature = "field")]
impl TryFrom<&crate::Field> for FFISchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &crate::Field) -> Result<Self> {
        Self::from_arrow(value.as_ref())
    }
}

impl From<Box<ffi::FFI_ArrowSchema>> for FFISchema {
    fn from(value: Box<ffi::FFI_ArrowSchema>) -> Self {
        Self(value)
    }
}

impl From<ffi::FFI_ArrowSchema> for FFISchema {
    fn from(value: ffi::FFI_ArrowSchema) -> Self {
        Self(Box::new(value))
    }
}
