use std::sync::Arc;

use wasm_bindgen::prelude::*;

use crate::arrow_js::record_batch::JSRecordBatch;
use crate::arrow_js::schema::{import_schema, JSSchema};
use crate::error::WasmResult;
use crate::{RecordBatch, Table};

#[wasm_bindgen]
extern "C" {
    pub type JSTable;

    #[wasm_bindgen(method, getter)]
    pub fn schema(this: &JSTable) -> JSSchema;

    #[wasm_bindgen(method, getter)]
    pub fn batches(this: &JSTable) -> Vec<JSRecordBatch>;

}

impl Table {
    pub fn from_js(js_table: &JSTable) -> WasmResult<Table> {
        let schema = Arc::new(import_schema(&js_table.schema()));
        let batches = js_table
            .batches()
            .into_iter()
            .map(|batch| Ok(RecordBatch::from_js_with_schema(&batch, schema.clone())?.into_inner()))
            .collect::<WasmResult<Vec<_>>>()?;
        Ok(Table::new(schema, batches))
    }
}
