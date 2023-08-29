use wasm_bindgen::prelude::*;

use crate::arrow1::schema::Schema;

#[wasm_bindgen]
pub struct RecordBatch(arrow::record_batch::RecordBatch);

impl RecordBatch {
    pub fn new(batch: arrow::record_batch::RecordBatch) -> Self {
        Self(batch)
    }

    pub fn into_inner(self) -> arrow::record_batch::RecordBatch {
        self.0
    }
}

#[wasm_bindgen]
impl RecordBatch {
    #[wasm_bindgen(getter, js_name = numRows)]
    pub fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    #[wasm_bindgen(getter, js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// Returns the schema of the record batches.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.0.schema().as_ref().clone())
    }

}

impl From<arrow::record_batch::RecordBatch> for RecordBatch {
    fn from(value: arrow::record_batch::RecordBatch) -> Self {
        Self(value)
    }
}

impl From<RecordBatch> for arrow::record_batch::RecordBatch {
    fn from(value: RecordBatch) -> Self {
        value.0
    }
}
