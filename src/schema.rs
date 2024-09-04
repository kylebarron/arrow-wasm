use arrow::ipc::writer::StreamWriter;
use std::collections::HashMap;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use crate::ffi::FFISchema;

#[wasm_bindgen(typescript_custom_section)]
const TS_SchemaMetadata: &'static str = r#"
export type SchemaMetadata = Map<string, string>;
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "SchemaMetadata")]
    pub type SchemaMetadata;
}

/// A named collection of types that defines the column names and types in a RecordBatch or Table
/// data structure.
///
/// A Schema can also contain extra user-defined metadata either at the Table or Column level.
/// Column-level metadata is often used to define [extension
/// types](https://arrow.apache.org/docs/format/Columnar.html#extension-types).
#[wasm_bindgen]
pub struct Schema(pub(crate) arrow_schema::SchemaRef);

#[wasm_bindgen]
impl Schema {
    /// Export this schema to an FFISchema object, which can be read with arrow-js-ffi.
    ///
    /// This method **does not consume** the Schema, so you must remember to call {@linkcode
    /// Schema.free} to release the resources. The underlying arrays are reference counted, so
    /// this method does not copy data, it only prevents the data from being released.
    #[wasm_bindgen(js_name = toFFI)]
    pub fn to_ffi(&self) -> WasmResult<FFISchema> {
        Ok(FFISchema::from_arrow(self.0.as_ref())?)
    }

    /// Export this Table to FFI structs according to the Arrow C Data Interface.
    ///
    /// This method **does consume** the Table, so the original Table will be
    /// inaccessible after this call. You must still call {@linkcode FFITable.free} after
    /// you've finished using the FFITable.
    #[wasm_bindgen(js_name = intoFFI)]
    pub fn into_ffi(self) -> WasmResult<FFISchema> {
        Ok(FFISchema::from_arrow(self.0.as_ref())?)
    }

    /// Consume this schema and convert to an Arrow IPC Stream buffer
    #[wasm_bindgen(js_name = intoIPCStream)]
    pub fn into_ipc_stream(self) -> WasmResult<Vec<u8>> {
        let mut output_file = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut output_file, &self.0)?;

            writer.finish()?;
        }

        // Note that this returns output_file directly instead of using
        // writer.into_inner().to_vec() as the latter seems likely to incur an extra copy of the
        // vec
        Ok(output_file)
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected using an
    /// offset within the internal `fields` vector.
    #[cfg(feature = "field")]
    #[wasm_bindgen]
    pub fn field(&self, i: usize) -> crate::Field {
        (self.0.fields()[i].clone()).into()
    }

    /// Returns an immutable reference of a specific [`Field`] instance selected by name.
    #[cfg(feature = "field")]
    #[wasm_bindgen(js_name = fieldWithName)]
    pub fn field_with_name(&self, name: &str) -> WasmResult<crate::Field> {
        let field = self.0.field_with_name(name)?;
        Ok(field.clone().into())
    }

    /// Sets the metadata of this `Schema` to be `metadata` and returns a new object
    #[wasm_bindgen(js_name = withMetadata)]
    pub fn with_metadata(&mut self, metadata: SchemaMetadata) -> WasmResult<Schema> {
        let metadata: HashMap<String, String> = serde_wasm_bindgen::from_value(metadata.into())?;
        let field = self.0.as_ref().clone();
        Ok(field.with_metadata(metadata).into())
    }

    /// Find the index of the column with the given name.
    #[wasm_bindgen(js_name = indexOf)]
    pub fn index_of(&mut self, name: &str) -> WasmResult<usize> {
        Ok(self.0.index_of(name)?)
    }

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    #[wasm_bindgen]
    pub fn metadata(&self) -> WasmResult<SchemaMetadata> {
        Ok(serde_wasm_bindgen::to_value(self.0.metadata())?.into())
    }
}

impl Schema {
    pub fn new(schema: arrow_schema::SchemaRef) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> arrow_schema::SchemaRef {
        self.0
    }
}

impl From<arrow_schema::Schema> for Schema {
    fn from(value: arrow_schema::Schema) -> Self {
        Self(Arc::new(value))
    }
}

impl From<arrow_schema::SchemaRef> for Schema {
    fn from(value: arrow_schema::SchemaRef) -> Self {
        Self(value)
    }
}

impl From<Schema> for arrow_schema::SchemaRef {
    fn from(value: Schema) -> Self {
        value.0
    }
}

impl AsRef<arrow_schema::Schema> for Schema {
    fn as_ref(&self) -> &arrow_schema::Schema {
        &self.0
    }
}
