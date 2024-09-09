use arrow::array::*;
use arrow::buffer::Buffer;
use arrow_schema::DataType;
use wasm_bindgen::prelude::*;

use crate::arrow_js::r#type::{import_data_type, JSDataType};

#[wasm_bindgen]
extern "C" {
    pub type JSData;

    #[wasm_bindgen(method, getter, js_name = "type")]
    pub fn data_type(this: &JSData) -> JSDataType;

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
    pub fn value_offsets(this: &JSData) -> TypedArrayLike;

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
    js_data.null_bitmap().and_then(|arr| {
        let buf = arr.to_vec();
        // Arrow JS often stores an empty Uint8Array for a non-null array. So here, if the buffer
        // is of length 0, we return None, to signify fully valid.
        if buf.is_empty() {
            None
        } else {
            Some(Buffer::from_vec(buf))
        }
    })
}

fn copy_typed_array_like(arr: &TypedArrayLike) -> Buffer {
    let uint8_view = js_sys::Uint8Array::new_with_byte_offset_and_length(
        &arr.buffer().into(),
        arr.byte_offset(),
        arr.byte_length(),
    );
    Buffer::from_vec(uint8_view.to_vec())
}

fn copy_values(js_data: &JSData) -> Buffer {
    copy_typed_array_like(&js_data.values())
}

fn copy_value_offsets(js_data: &JSData) -> Buffer {
    copy_typed_array_like(js_data.value_offsets().unchecked_ref())
}

pub fn import_data(js_data: &JSData) -> ArrayData {
    let mut child_data = vec![];
    for child in js_data.children() {
        child_data.push(import_data(&child));
    }

    let data_type = import_data_type(&js_data.data_type());

    // TODO: support dictionary
    let buffers = match data_type.is_primitive() {
        true => vec![copy_values(js_data)],
        false => match data_type {
            DataType::Null => vec![],
            DataType::Boolean => vec![copy_values(js_data)],
            DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8 => {
                vec![copy_value_offsets(js_data), copy_values(js_data)]
            }
            DataType::List(_) | DataType::LargeList(_) => vec![copy_value_offsets(js_data)],
            DataType::FixedSizeBinary(_) | DataType::FixedSizeList(_, _) | DataType::Struct(_) => {
                vec![]
            }
            DataType::Union(_fields, _mode) => {
                todo!()
            }
            _ => unreachable!(),
        },
    };

    ArrayData::try_new(
        data_type,
        js_data.length(),
        copy_null_bitmap(js_data),
        js_data.offset(),
        buffers,
        child_data,
    )
    .unwrap()
}
