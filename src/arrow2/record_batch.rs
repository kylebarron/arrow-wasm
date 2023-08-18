use arrow2::array::{Array, StructArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field};
use arrow2::ffi;
use wasm_bindgen::prelude::*;

use crate::arrow2::{Schema, Vector};

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
}

#[wasm_bindgen]
impl RecordBatch {
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.chunk.len()
    }

    #[wasm_bindgen(getter, js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.chunk.columns().len()
    }

    /// Returns the schema of the record batches.
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
}

/// Wrapper an Arrow RecordBatch stored as FFI in Wasm memory.
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
    #[wasm_bindgen]
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
        self.array.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn field_addr(&self) -> *const ffi::ArrowSchema {
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

//     /// Get all columns in the record batch.
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
