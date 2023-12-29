use wasm_bindgen::prelude::*;

#[cfg(feature = "arrow1")]
pub mod arrow1;

#[cfg(feature = "arrow2")]
pub mod arrow2;

mod utils;

#[wasm_bindgen(typescript_custom_section)]
const TS_FunctionTable: &'static str = r#"
export type FunctionTable = WebAssembly.Table;
"#;

#[wasm_bindgen(typescript_custom_section)]
const TS_WasmMemory: &'static str = r#"
export type Memory = WebAssembly.Memory;
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "FunctionTable")]
    pub type FunctionTable;

    #[wasm_bindgen(typescript_type = "Memory")]
    pub type Memory;
}

/// Returns a handle to this wasm instance's `WebAssembly.Memory`
#[wasm_bindgen(js_name = wasmMemory)]
pub fn memory() -> Memory {
    wasm_bindgen::memory().into()
}

/// Returns a handle to this wasm instance's `WebAssembly.Table` which is the indirect function
/// table used by Rust
#[wasm_bindgen(js_name = _functionTable)]
pub fn function_table() -> FunctionTable {
    wasm_bindgen::function_table().into()
}
