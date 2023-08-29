use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Schema(arrow_schema::Schema);

impl Schema {
    pub fn new(schema: arrow_schema::Schema) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> arrow_schema::Schema {
        self.0
    }
}

impl From<arrow_schema::Schema> for Schema {
    fn from(value: arrow_schema::Schema) -> Self {
        Self(value)
    }
}

impl From<Schema> for arrow_schema::Schema {
    fn from(value: Schema) -> Self {
        value.0
    }
}
