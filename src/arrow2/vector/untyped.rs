use arrow2::array::Array;
use wasm_bindgen::prelude::*;

use crate::arrow2::DataType;

/// An Arrow vector of unknown type that is guaranteed to have contiguous memory (i.e. cannot be
/// chunked).
#[wasm_bindgen]
pub struct Vector(Box<dyn arrow2::array::Array>);

impl Vector {
    pub fn new(arr: Box<dyn Array>) -> Self {
        Self(arr)
    }

    pub fn into_inner(self) -> Box<dyn arrow2::array::Array> {
        self.0
    }
}

#[wasm_bindgen]
impl Vector {
    /// The data type of this vector
    #[wasm_bindgen]
    pub fn data_type(&self) -> DataType {
        DataType::new(self.0.data_type().clone())
    }
}
