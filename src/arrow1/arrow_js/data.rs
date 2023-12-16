use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::Buffer;
use arrow_schema::DataType;
use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::r#type::{import_data_type, JSDataType};

#[wasm_bindgen]
extern "C" {
    pub type JSData;

    #[wasm_bindgen(method, getter)]
    pub fn r#type(this: &JSData) -> JSDataType;

    #[wasm_bindgen(method, getter)]
    pub fn length(this: &JSData) -> usize;

    #[wasm_bindgen(method, getter)]
    pub fn offset(this: &JSData) -> usize;

    #[wasm_bindgen(method, getter)]
    pub fn stride(this: &JSData) -> usize;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSData) -> Vec<JSData>;

    #[wasm_bindgen(method, getter)]
    pub fn values(this: &JSData) -> TypedArrayLike;

    #[wasm_bindgen(method, getter, js_name = "typeIds")]
    pub fn type_ids(this: &JSData) -> TypedArrayLike;

    #[wasm_bindgen(method, getter, js_name = "nullBitmap")]
    pub fn null_bitmap(this: &JSData) -> Option<js_sys::Uint8Array>;

    #[wasm_bindgen(method, getter, js_name = "valueOffsets")]
    pub fn value_offsets(this: &JSData) -> js_sys::Int32Array;

    pub type TypedArrayLike;

    /// The buffer accessor property represents the `ArrayBuffer` referenced
    /// by a `TypedArray` at construction time.
    #[wasm_bindgen(getter, method)]
    pub fn buffer(this: &TypedArrayLike) -> js_sys::ArrayBuffer;

    /// The byteLength accessor property represents the length (in bytes) of a
    /// typed array.
    #[wasm_bindgen(method, getter, js_name = byteLength)]
    pub fn byte_length(this: &TypedArrayLike) -> u32;

    /// The byteOffset accessor property represents the offset (in bytes) of a
    /// typed array from the start of its `ArrayBuffer`.
    #[wasm_bindgen(method, getter, js_name = byteOffset)]
    pub fn byte_offset(this: &TypedArrayLike) -> u32;
}

fn copy_null_bitmap(js_data: &JSData) -> Option<Buffer> {
    js_data.null_bitmap().map(|arr| {
        let buf: Vec<u8> = serde_wasm_bindgen::from_value(arr.into()).unwrap();
        buf.into()
    })
}

fn copy_typed_array_like(arr: &TypedArrayLike) -> Buffer {
    let uint8_view = js_sys::Uint8Array::new_with_byte_offset_and_length(
        &arr.buffer().into(),
        arr.byte_offset(),
        arr.byte_length(),
    );
    let buf: Vec<u8> = serde_wasm_bindgen::from_value(uint8_view.into()).unwrap();
    buf.into()
}

fn import_uint8(js_data: &JSData) -> UInt8Array {
    js
    UInt8Array::new(values, nulls)
}

pub fn import_data(js_data: &JSData) -> Arc<dyn Array> {
    let mut child_data = vec![];
    for child in js_data.children() {
        child_data.push(import_data(&child));
    }

    let data_type = import_data_type(&js_data.r#type());

    let buffers = match data_type {
        DataType::Null => vec![],

        _ => todo!()
    };


    ArrayData::try_new(
        data_type,
        js_data.length(),
        copy_null_bitmap(js_data),
        js_data.offset(),
        buffers,
        child_data,
    ).unwrap()
}
