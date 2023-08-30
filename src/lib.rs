use wasm_bindgen::prelude::*;

#[cfg(feature = "arrow1")]
pub mod arrow1;

#[cfg(feature = "arrow2")]
pub mod arrow2;

mod utils;

/// Returns a handle to this wasm instance's `WebAssembly.Memory`
#[wasm_bindgen(js_name = _memory)]
pub fn memory() -> JsValue {
    wasm_bindgen::memory()
}

/// Returns a handle to this wasm instance's `WebAssembly.Table` which is the indirect function
/// table used by Rust
#[wasm_bindgen(js_name = _functionTable)]
pub fn function_table() -> JsValue {
    wasm_bindgen::function_table()
}
