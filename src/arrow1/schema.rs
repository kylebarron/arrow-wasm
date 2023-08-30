use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Schema(Arc<arrow_schema::Schema>);

impl Schema {
    pub fn new(schema: Arc<arrow_schema::Schema>) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> Arc<arrow_schema::Schema> {
        self.0
    }
}

impl From<arrow_schema::Schema> for Schema {
    fn from(value: arrow_schema::Schema) -> Self {
        Self(Arc::new(value))
    }
}

impl From<arrow_schema::SchemaRef> for Schema {
    fn from(value: arrow_schema::SchemaRef) -> Self {
        Self(value)
    }
}

impl From<Schema> for Arc<arrow_schema::Schema> {
    fn from(value: Schema) -> Self {
        value.0
    }
}

impl From<Schema> for arrow_schema::Schema {
    fn from(value: Schema) -> Self {
        value.0.as_ref().clone()
    }
}
