use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::record_batch::{import_record_batch, JSRecordBatch};
use crate::arrow1::arrow_js::schema::JSSchema;
use crate::arrow1::Table;

#[wasm_bindgen]
extern "C" {
    pub type JSTable;

    #[wasm_bindgen(method, getter)]
    pub fn schema(this: &JSTable) -> JSSchema;

    #[wasm_bindgen(method, getter)]
    pub fn batches(this: &JSTable) -> Vec<JSRecordBatch>;

}

pub fn import_table(js_table: &JSTable) -> Table {
    let batches = js_table
        .batches()
        .into_iter()
        .map(|batch| import_record_batch(&batch))
        .collect();
    Table::new(batches)
}
