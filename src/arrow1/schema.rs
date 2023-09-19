use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// A named collection of types that defines the column names and types in a RecordBatch or Table
/// data structure.
///
/// A Schema can also contain extra user-defined metadata either at the Table or Column level.
/// Column-level metadata is often used to define [extension
/// types](https://arrow.apache.org/docs/format/Columnar.html#extension-types).
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
