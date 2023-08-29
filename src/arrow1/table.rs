use wasm_bindgen::prelude::*;

use crate::arrow1::schema::Schema;

#[wasm_bindgen]
pub struct Table {
    // TODO: should this refrain from having its own schema?
    schema: arrow_schema::Schema,
    batches: Vec<arrow::record_batch::RecordBatch>,
}

impl Table {
    pub fn new(
        schema: arrow_schema::Schema,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Self {
        Self { schema, batches }
    }

    pub fn into_inner(self) -> (arrow_schema::Schema, Vec<arrow::record_batch::RecordBatch>) {
        (self.schema, self.batches)
    }
}

#[wasm_bindgen]
impl Table {
    /// Returns the schema of the record batches.
    #[wasm_bindgen(getter)]
    pub fn schema(&self) -> Schema {
        Schema::new(self.schema.clone())
    }
}
