use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Schema(arrow2::datatypes::Schema);

impl Schema {
    pub fn new(schema: arrow2::datatypes::Schema) -> Self {
        Self(schema)
    }
}

// #[wasm_bindgen]
// impl Schema {
//     pub fn field(&self, i: usize) -> field::Field {
//         field::Field::new(self.0.field(i).clone())
//     }

//     #[wasm_bindgen(getter)]
//     pub fn fields(&self) -> JsValue {
//         JsValue::from_serde(&self.0.fields()).unwrap()
//     }

//     #[wasm_bindgen(getter, js_name = numFields)]
//     pub fn num_fields(&self) -> usize {
//         self.0.fields().len()
//     }

//     /// Look up a column by name and return a immutable reference to the column along with its index.
//     #[wasm_bindgen(js_name = columnWithName)]
//     pub fn column_with_name(&self, name: &str) -> JsValue {
//         match self.0.column_with_name(name) {
//             Some(column) => JsValue::from_serde(&column).unwrap(),
//             None => wasm_bindgen::JsValue::undefined(),
//         }
//     }

//     /// Find the index of the column with the given name.
//     #[wasm_bindgen(js_name = indexOf)]
//     pub fn index_of(&self, name: &str) -> Result<usize, JsValue> {
//         self.0
//             .index_of(name)
//             .map_err(|error| format!("{}", error).into())
//     }

//     #[wasm_bindgen(js_name = fieldWithName)]
//     pub fn field_with_name(&self, name: &str) -> Result<field::Field, JsValue> {
//         match self.0.field_with_name(name) {
//             Ok(field) => Ok(field::Field::new(field.clone())),
//             Err(error) => Err(format!("{}", error).into()),
//         }
//     }

//     /// Returns custom metadata key-value pairs.
//     #[wasm_bindgen(getter)]
//     pub fn metadata(&self) -> JsValue {
//         JsValue::from_serde(&self.0.metadata()).unwrap()
//     }

//     /// Parse a `Schema` definition from a JSON representation.
//     pub fn from(json: &JsValue) -> Result<Schema, JsValue> {
//         let value = json.into_serde().unwrap();
//         let schema = match arrow::datatypes::Schema::from(&value) {
//             Ok(schema) => schema,
//             Err(error) => return Err(format!("{}", error).into()),
//         };
//         Ok(Schema {
//             0: std::sync::Arc::new(schema),
//         })
//     }
// }
