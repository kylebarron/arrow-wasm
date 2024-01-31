use std::sync::Arc;

use crate::error::WasmResult;
use arrow::array::{make_array, AsArray};
use arrow_schema::SchemaRef;
// use arrow::record_batch::RecordBatch;
use crate::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

use crate::arrow_js::data::{import_data, JSData};
use crate::arrow_js::schema::{import_schema, JSSchema};

#[wasm_bindgen]
extern "C" {
    pub type JSRecordBatch;

    #[wasm_bindgen(method, getter)]
    pub fn schema(this: &JSRecordBatch) -> JSSchema;

    #[wasm_bindgen(method, getter)]
    pub fn data(this: &JSRecordBatch) -> JSData;

}

impl RecordBatch {
    pub fn from_js(js_record_batch: &JSRecordBatch) -> WasmResult<Self> {
        let schema = import_schema(&js_record_batch.schema());
        let data = import_data(&js_record_batch.data());
        let dyn_arr = make_array(data);
        let struct_arr = dyn_arr.as_struct();

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            struct_arr.columns().to_vec(),
        )?;
        Ok(RecordBatch::new(batch))
    }

    pub fn from_js_with_schema(
        js_record_batch: &JSRecordBatch,
        schema: SchemaRef,
    ) -> WasmResult<Self> {
        let data = import_data(&js_record_batch.data());
        let dyn_arr = make_array(data);
        let struct_arr = dyn_arr.as_struct();

        let batch =
            arrow::record_batch::RecordBatch::try_new(schema, struct_arr.columns().to_vec())?;
        Ok(RecordBatch::new(batch))
    }
}
