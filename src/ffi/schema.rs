use arrow::ffi;
use wasm_bindgen::prelude::*;

use crate::error::Result;

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

impl TryFrom<&arrow_schema::Schema> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &arrow_schema::Schema) -> Result<Self> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value)?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

impl TryFrom<&arrow_schema::Field> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &arrow_schema::Field) -> Result<Self> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value)?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

#[cfg(feature = "schema")]
impl TryFrom<&crate::Schema> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &crate::Schema) -> Result<Self> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.0.as_ref())?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

#[cfg(feature = "field")]
impl TryFrom<&crate::Field> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: &crate::Field) -> Result<Self> {
        let ffi_schema = ffi::FFI_ArrowSchema::try_from(value.0.as_ref())?;
        Ok(Self(Box::new(ffi_schema)))
    }
}

#[cfg(feature = "schema")]
impl TryFrom<crate::Schema> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: crate::Schema) -> Result<Self> {
        (&value).try_into()
    }
}

#[cfg(feature = "field")]
impl TryFrom<crate::Field> for FFIArrowSchema {
    type Error = crate::error::ArrowWasmError;

    fn try_from(value: crate::Field) -> Result<Self> {
        (&value).try_into()
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
