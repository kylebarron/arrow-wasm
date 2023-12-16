use arrow_schema::Field;
use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::r#type::{import_data_type, JSDataType};

#[wasm_bindgen]
extern "C" {
    pub type JSField;

    #[wasm_bindgen(method, getter, js_name = "type")]
    pub fn data_type(this: &JSField) -> JSDataType;

    #[wasm_bindgen(method, getter)]
    pub fn name(this: &JSField) -> String;

    #[wasm_bindgen(method, getter)]
    pub fn nullable(this: &JSField) -> bool;

    #[wasm_bindgen(method, getter)]
    pub fn metadata(this: &JSField) -> js_sys::Map;

}

pub fn import_field(js_field: &JSField) -> Field {
    let data_type = import_data_type(&js_field.data_type());
    Field::new(js_field.name(), data_type, js_field.nullable())
        .with_metadata(serde_wasm_bindgen::from_value(js_field.metadata().into()).unwrap())
}
