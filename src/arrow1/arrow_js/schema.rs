use arrow_schema::Schema;
use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::field::{import_field, JSField};

#[wasm_bindgen]
extern "C" {
    pub type JSSchema;

    #[wasm_bindgen(method, getter)]
    pub fn fields(this: &JSSchema) -> Vec<JSField>;

    #[wasm_bindgen(method, getter)]
    pub fn metadata(this: &JSSchema) -> js_sys::Map;

}

pub fn import_schema(js_schema: &JSSchema) -> Schema {
    let fields: Vec<_> = js_schema
        .fields()
        .into_iter()
        .map(|js_field| import_field(&js_field))
        .collect();
    Schema::new_with_metadata(
        fields,
        serde_wasm_bindgen::from_value(js_schema.metadata().into()).unwrap(),
    )
}
