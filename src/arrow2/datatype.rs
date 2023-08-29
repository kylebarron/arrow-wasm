use wasm_bindgen::prelude::*;

/// The set of supported logical types in this crate.
#[wasm_bindgen]
pub struct DataType(arrow2::datatypes::DataType);

impl DataType {
    pub fn new(datatype: arrow2::datatypes::DataType) -> Self {
        Self(datatype)
    }

    pub fn into_inner(self) -> arrow2::datatypes::DataType {
        self.0
    }
}
