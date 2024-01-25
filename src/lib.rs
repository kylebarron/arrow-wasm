use wasm_bindgen::prelude::*;

#[cfg(feature = "read_arrow_js")]
pub mod arrow_js;
#[cfg(feature = "data")]
pub mod data;
#[cfg(feature = "data_type")]
pub mod datatype;
pub mod error;
pub mod ffi;
pub mod field;
pub mod record_batch;
pub mod schema;
#[cfg(feature = "table")]
pub mod table;
#[cfg(feature = "vector")]
pub mod vector;

pub use error::ArrowWasmError;
pub use field::Field;
pub use record_batch::{FFIRecordBatch, RecordBatch};
pub use schema::Schema;
pub use table::{FFITable, Table};

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
