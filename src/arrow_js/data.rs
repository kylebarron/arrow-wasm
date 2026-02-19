use arrow_buffer::{Buffer, IntervalMonthDayNano, i256};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
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

fn invalid_argument(message: impl Into<String>) -> JsError {
    ArrowError::InvalidArgumentError(message.into()).into()
}

fn bytes_for_typed_array(arr: &TypedArrayLike) -> Vec<u8> {
    let uint8_view = js_sys::Uint8Array::new_with_byte_offset_and_length(
        &arr.buffer().into(),
        arr.byte_offset(),
        arr.byte_length(),
    );
    uint8_view.to_vec()
}

fn decode_primitive_vec<T, const WIDTH: usize>(
    arr: &TypedArrayLike,
    parse: impl Fn([u8; WIDTH]) -> T,
) -> WasmResult<Vec<T>> {
    let bytes = bytes_for_typed_array(arr);
    if bytes.len() % WIDTH != 0 {
        return Err(invalid_argument(format!(
            "typed buffer length {} is not divisible by element width {}",
            bytes.len(),
            WIDTH
        )));
    }
    Ok(bytes
        .chunks_exact(WIDTH)
        .map(|chunk| {
            let mut raw = [0u8; WIDTH];
            raw.copy_from_slice(chunk);
            parse(raw)
        })
        .collect())
}

fn copy_values_for_type(js_data: &JSData, data_type: &DataType) -> WasmResult<Buffer> {
    let values = js_data.values();
    let buffer = match data_type {
        DataType::Boolean | DataType::Binary | DataType::LargeBinary | DataType::Utf8
        | DataType::LargeUtf8 | DataType::FixedSizeBinary(_) => copy_typed_array_like(&values),
        DataType::Int8 => Buffer::from_vec(decode_primitive_vec::<i8, 1>(&values, |b| b[0] as i8)?),
        DataType::UInt8 => Buffer::from_vec(decode_primitive_vec::<u8, 1>(&values, |b| b[0])?),
        DataType::Int16 => {
            Buffer::from_vec(decode_primitive_vec::<i16, 2>(&values, i16::from_le_bytes)?)
        }
        DataType::UInt16 | DataType::Float16 => {
            Buffer::from_vec(decode_primitive_vec::<u16, 2>(&values, u16::from_le_bytes)?)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Buffer::from_vec(decode_primitive_vec::<i32, 4>(&values, i32::from_le_bytes)?)
        }
        DataType::UInt32 => {
            Buffer::from_vec(decode_primitive_vec::<u32, 4>(&values, u32::from_le_bytes)?)
        }
        DataType::Float32 => {
            Buffer::from_vec(decode_primitive_vec::<f32, 4>(&values, f32::from_le_bytes)?)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => {
            Buffer::from_vec(decode_primitive_vec::<i64, 8>(&values, i64::from_le_bytes)?)
        }
        DataType::UInt64 => {
            Buffer::from_vec(decode_primitive_vec::<u64, 8>(&values, u64::from_le_bytes)?)
        }
        DataType::Float64 => {
            Buffer::from_vec(decode_primitive_vec::<f64, 8>(&values, f64::from_le_bytes)?)
        }
        DataType::Decimal128(_, _) => {
            Buffer::from_vec(decode_primitive_vec::<i128, 16>(&values, i128::from_le_bytes)?)
        }
        DataType::Decimal256(_, _) => {
            Buffer::from_vec(decode_primitive_vec::<i256, 32>(&values, i256::from_le_bytes)?)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => Buffer::from_vec(
            decode_primitive_vec::<IntervalMonthDayNano, 16>(&values, |b| {
                let months = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                let days = i32::from_le_bytes([b[4], b[5], b[6], b[7]]);
                let nanoseconds =
                    i64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
                IntervalMonthDayNano::new(months, days, nanoseconds)
            })?,
        ),
        _ => {
            return Err(invalid_argument(format!(
                "unsupported values buffer data type in Arrow JS import: {data_type:?}"
            )))
        }
    };
    Ok(buffer)
}

fn copy_value_offsets_for_type(js_data: &JSData, data_type: &DataType) -> WasmResult<Buffer> {
    let offsets = js_data.value_offsets();
    match data_type {
        DataType::Binary | DataType::Utf8 | DataType::List(_) | DataType::Map(_, _) => {
            Ok(Buffer::from_vec(decode_primitive_vec::<i32, 4>(
                offsets.unchecked_ref(),
                i32::from_le_bytes,
            )?))
        }
        DataType::LargeBinary | DataType::LargeUtf8 | DataType::LargeList(_) => Ok(
            Buffer::from_vec(decode_primitive_vec::<i64, 8>(
                offsets.unchecked_ref(),
                i64::from_le_bytes,
            )?),
        ),
        _ => Err(invalid_argument(format!(
            "offset buffer requested for unsupported data type: {data_type:?}"
        ))),
    }
}

pub fn import_data(js_data: &JSData) -> WasmResult<ArrayData> {
    let mut child_data = vec![];
    for child in js_data.children() {
        child_data.push(import_data(&child)?);
    }

    let data_type = import_data_type(&js_data.data_type());

    // TODO: support dictionary
    let buffers = match &data_type {
        DataType::Null => vec![],
        DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8 => {
            vec![
                copy_value_offsets_for_type(js_data, &data_type)?,
                copy_values_for_type(js_data, &data_type)?,
            ]
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
            vec![copy_value_offsets_for_type(js_data, &data_type)?]
        }
        DataType::FixedSizeList(_, _) | DataType::Struct(_) => vec![],
        DataType::Union(_fields, _mode) => {
            todo!()
        }
        _ => {
            vec![copy_values_for_type(js_data, &data_type)?]
        }
    };

    Ok(ArrayData::try_new(
        data_type,
        js_data.length(),
        copy_null_bitmap(js_data),
        js_data.offset(),
        buffers,
        child_data,
    )
    ?)
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::make_array;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_test::*;

    fn set_property(obj: &js_sys::Object, key: &str, value: &JsValue) {
        js_sys::Reflect::set(obj, &JsValue::from_str(key), value).unwrap();
    }

    fn make_int_type(bit_width: i32, is_signed: bool) -> JsValue {
        let obj = js_sys::Object::new();
        set_property(&obj, "typeId", &JsValue::from(2));
        set_property(&obj, "bitWidth", &JsValue::from(bit_width));
        set_property(&obj, "isSigned", &JsValue::from(is_signed));
        obj.into()
    }

    fn make_float_type(precision: i32) -> JsValue {
        let obj = js_sys::Object::new();
        set_property(&obj, "typeId", &JsValue::from(3));
        set_property(&obj, "precision", &JsValue::from(precision));
        obj.into()
    }

    fn make_utf8_type() -> JsValue {
        let obj = js_sys::Object::new();
        set_property(&obj, "typeId", &JsValue::from(5));
        obj.into()
    }

    fn make_data(
        type_value: JsValue,
        length: usize,
        offset: usize,
        values: JsValue,
        value_offsets: JsValue,
    ) -> JsValue {
        let obj = js_sys::Object::new();
        set_property(&obj, "type", &type_value);
        set_property(&obj, "length", &JsValue::from(length as u32));
        set_property(&obj, "offset", &JsValue::from(offset as u32));
        set_property(&obj, "stride", &JsValue::from(1u32));
        set_property(&obj, "children", &js_sys::Array::new().into());
        set_property(&obj, "values", &values);
        set_property(&obj, "nullBitmap", &JsValue::NULL);
        set_property(&obj, "valueOffsets", &value_offsets);
        obj.into()
    }

    fn uint8_view_with_offset(bytes: &[u8], start: u32) -> js_sys::Uint8Array {
        let arr = js_sys::Uint8Array::from(bytes);
        arr.subarray(start, bytes.len() as u32)
    }

    #[wasm_bindgen_test]
    fn import_int32_values_from_misaligned_bytes() {
        let mut bytes = vec![0u8];
        for value in [1i32, 2, 3] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let values = uint8_view_with_offset(&bytes, 1);
        let data = make_data(
            make_int_type(32, true),
            3,
            0,
            values.into(),
            js_sys::Int32Array::new_with_length(0).into(),
        );
        let imported = import_data(data.unchecked_ref()).unwrap();
        let array = make_array(imported);
        let actual = array.as_primitive::<arrow_array::types::Int32Type>().values();
        assert_eq!(actual.as_ref(), &[1, 2, 3]);
    }

    #[wasm_bindgen_test]
    fn import_float64_values_from_misaligned_bytes() {
        let mut bytes = vec![0u8, 0u8];
        for value in [1.5f64, -2.0] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let values = uint8_view_with_offset(&bytes, 2);
        let data = make_data(
            make_float_type(2),
            2,
            0,
            values.into(),
            js_sys::Int32Array::new_with_length(0).into(),
        );
        let imported = import_data(data.unchecked_ref()).unwrap();
        let array = make_array(imported);
        let actual = array
            .as_primitive::<arrow_array::types::Float64Type>()
            .values();
        assert_eq!(actual.as_ref(), &[1.5, -2.0]);
    }

    #[wasm_bindgen_test]
    fn import_utf8_values_and_offsets() {
        let values = js_sys::Uint8Array::from([b'a', b'b', b'c', b'd', b'e'].as_slice());
        let mut offset_bytes = vec![];
        for value in [0i32, 2, 5] {
            offset_bytes.extend_from_slice(&value.to_le_bytes());
        }
        let offsets = js_sys::Uint8Array::from(offset_bytes.as_slice());
        let data = make_data(make_utf8_type(), 2, 0, values.into(), offsets.into());
        let imported = import_data(data.unchecked_ref()).unwrap();
        let array = make_array(imported);
        let actual = array.as_string::<i32>();
        assert_eq!(actual.value(0), "ab");
        assert_eq!(actual.value(1), "cde");
    }

    #[wasm_bindgen_test]
    fn invalid_typed_buffer_returns_error() {
        let values = js_sys::Uint8Array::from([1u8, 0, 0, 0, 2].as_slice());
        let data = make_data(
            make_int_type(32, true),
            1,
            0,
            values.into(),
            js_sys::Int32Array::new_with_length(0).into(),
        );
        assert!(import_data(data.unchecked_ref()).is_err());
    }
}
