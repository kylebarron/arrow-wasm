use std::sync::Arc;

use arrow::array::{make_array, AsArray};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::data::{import_data, JSData};
use crate::arrow1::arrow_js::schema::{import_schema, JSSchema};

#[wasm_bindgen]
extern "C" {
    pub type JSRecordBatch;

    #[wasm_bindgen(method, getter)]
    pub fn schema(this: &JSRecordBatch) -> JSSchema;

    #[wasm_bindgen(method, getter)]
    pub fn data(this: &JSRecordBatch) -> JSData;

}

pub fn import_record_batch(js_record_batch: &JSRecordBatch) -> RecordBatch {
    let schema = import_schema(&js_record_batch.schema());
    let data = import_data(&js_record_batch.data());
    let dyn_arr = make_array(data);
    let struct_arr = dyn_arr.as_struct();

    RecordBatch::try_new(Arc::new(schema), struct_arr.columns().to_vec()).unwrap()
}
