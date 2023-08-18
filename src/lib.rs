#[cfg(feature = "arrow2")]
pub mod arrow2;

mod utils;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct HelloWorld;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, test!");
}
