use arrow2::array::Array;
use wasm_bindgen::prelude::*;

use crate::arrow2::DataType;

#[wasm_bindgen]
pub struct Vector(Box<dyn arrow2::array::Array>);

impl Vector {
    pub fn new(arr: Box<dyn Array>) -> Self {
        Self(arr)
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
