use std::sync::Arc;

use arrow_schema::{DataType, UnionFields};
use wasm_bindgen::prelude::*;

use crate::arrow1::arrow_js::field::{import_field, JSField};

#[wasm_bindgen]
extern "C" {
    pub type JSDataType;

    #[wasm_bindgen(method, getter, js_name = "typeId")]
    pub fn type_id(this: &JSDataType) -> super::r#enum::Type;

    // Any data types containing data need to be defined separately:

    /// Arrow.Int
    pub type JSInt;

    #[wasm_bindgen(method, getter, js_name = "bitWidth")]
    pub fn bit_width(this: &JSInt) -> u8;

    #[wasm_bindgen(method, getter, js_name = "isSigned")]
    pub fn is_signed(this: &JSInt) -> bool;

    /// Arrow.Float
    pub type JSFloat;

    #[wasm_bindgen(method, getter)]
    pub fn precision(this: &JSFloat) -> super::r#enum::Precision;

    pub type JSDecimal;

    #[wasm_bindgen(method, getter)]
    pub fn scale(this: &JSDecimal) -> i8;

    #[wasm_bindgen(method, getter)]
    pub fn precision(this: &JSDecimal) -> u8;

    #[wasm_bindgen(method, getter, js_name = "bitWidth")]
    pub fn bit_width(this: &JSDecimal) -> usize;

    pub type JSDate_;

    #[wasm_bindgen(method, getter)]
    pub fn unit(this: &JSDate_) -> super::r#enum::DateUnit;

    pub type JSTime;

    #[wasm_bindgen(method, getter)]
    pub fn unit(this: &JSTime) -> super::r#enum::TimeUnit;

    pub type JSTimestamp;

    #[wasm_bindgen(method, getter)]
    pub fn unit(this: &JSTimestamp) -> super::r#enum::TimeUnit;

    #[wasm_bindgen(method, getter)]
    pub fn timezone(this: &JSTimestamp) -> Option<String>;

    pub type JSInterval;

    #[wasm_bindgen(method, getter)]
    pub fn unit(this: &JSInterval) -> super::r#enum::IntervalUnit;

    pub type JSDuration;

    #[wasm_bindgen(method, getter)]
    pub fn unit(this: &JSDuration) -> super::r#enum::TimeUnit;

    pub type JSList;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSList) -> Vec<JSField>;

    pub type JSStruct;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSStruct) -> Vec<JSField>;

    pub type JSUnion;

    #[wasm_bindgen(method, getter)]
    pub fn mode(this: &JSUnion) -> super::r#enum::UnionMode;

    #[wasm_bindgen(method, getter, js_name = "typeIds")]
    pub fn type_ids(this: &JSUnion) -> js_sys::Int32Array;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSUnion) -> Vec<JSField>;

    pub type JSFixedSizeBinary;

    #[wasm_bindgen(method, getter, js_name = "byteWidth")]
    pub fn byte_width(this: &JSFixedSizeBinary) -> i32;

    pub type JSFixedSizeList;

    #[wasm_bindgen(method, getter, js_name = "listSize")]
    pub fn list_size(this: &JSFixedSizeList) -> i32;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSFixedSizeList) -> Vec<JSField>;

    pub type JSMap_;

    #[wasm_bindgen(method, getter, js_name = "keysSorted")]
    pub fn keys_sorted(this: &JSMap_) -> bool;

    #[wasm_bindgen(method, getter)]
    pub fn children(this: &JSMap_) -> Vec<JSField>;

}

fn import_int(js_type: &JSInt) -> DataType {
    match (js_type.is_signed(), js_type.bit_width()) {
        (true, 8) => DataType::Int8,
        (true, 16) => DataType::Int16,
        (true, 32) => DataType::Int32,
        (true, 64) => DataType::Int64,
        (false, 8) => DataType::UInt8,
        (false, 16) => DataType::UInt16,
        (false, 32) => DataType::UInt32,
        (false, 64) => DataType::UInt64,
        _ => unreachable!(),
    }
}

fn import_float(js_type: &JSFloat) -> DataType {
    use super::r#enum::Precision;

    match js_type.precision() {
        Precision::Half => DataType::Float16,
        Precision::Single => DataType::Float32,
        Precision::Double => DataType::Float64,
    }
}

fn import_decimal(js_type: &JSDecimal) -> DataType {
    match js_type.bit_width() {
        128 => DataType::Decimal128(js_type.precision(), js_type.scale()),
        256 => DataType::Decimal256(js_type.precision(), js_type.scale()),
        _ => unreachable!(),
    }
}

fn import_date(js_type: &JSDate_) -> DataType {
    use super::r#enum::DateUnit;

    match js_type.unit() {
        DateUnit::Day => DataType::Date32,
        DateUnit::Millisecond => DataType::Date64,
    }
}

fn import_time(js_type: &JSTime) -> DataType {
    use super::r#enum::TimeUnit;

    match js_type.unit() {
        TimeUnit::Second => DataType::Time32(arrow_schema::TimeUnit::Second),
        TimeUnit::Millisecond => DataType::Time32(arrow_schema::TimeUnit::Millisecond),
        TimeUnit::Microsecond => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        TimeUnit::Nanosecond => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),
    }
}

fn import_timestamp(js_type: &JSTimestamp) -> DataType {
    use super::r#enum::TimeUnit;

    match js_type.unit() {
        TimeUnit::Second => DataType::Timestamp(
            arrow_schema::TimeUnit::Second,
            js_type.timezone().map(|s| s.into()),
        ),
        TimeUnit::Millisecond => DataType::Timestamp(
            arrow_schema::TimeUnit::Millisecond,
            js_type.timezone().map(|s| s.into()),
        ),
        TimeUnit::Microsecond => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            js_type.timezone().map(|s| s.into()),
        ),
        TimeUnit::Nanosecond => DataType::Timestamp(
            arrow_schema::TimeUnit::Nanosecond,
            js_type.timezone().map(|s| s.into()),
        ),
    }
}

fn import_interval(js_type: &JSInterval) -> DataType {
    use super::r#enum::IntervalUnit;

    match js_type.unit() {
        IntervalUnit::DayTime => DataType::Interval(arrow_schema::IntervalUnit::DayTime),
        IntervalUnit::YearMonth => DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
        IntervalUnit::MonthDayNano => DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
    }
}

fn import_duration(js_type: &JSDuration) -> DataType {
    use super::r#enum::TimeUnit;

    match js_type.unit() {
        TimeUnit::Second => DataType::Duration(arrow_schema::TimeUnit::Second),
        TimeUnit::Millisecond => DataType::Duration(arrow_schema::TimeUnit::Millisecond),
        TimeUnit::Microsecond => DataType::Duration(arrow_schema::TimeUnit::Microsecond),
        TimeUnit::Nanosecond => DataType::Duration(arrow_schema::TimeUnit::Nanosecond),
    }
}

fn import_list(js_type: &JSList) -> DataType {
    let mut children = js_type.children();
    assert_eq!(children.len(), 1);
    let child = children.pop().unwrap();
    let field = import_field(&child);
    DataType::List(Arc::new(field))
}

fn import_struct(js_type: &JSStruct) -> DataType {
    let fields = js_type
        .children()
        .into_iter()
        .map(|child| import_field(&child))
        .collect();
    DataType::Struct(fields)
}

fn import_union(js_type: &JSUnion) -> DataType {
    use super::r#enum::UnionMode;

    let fields: Vec<arrow_schema::Field> = js_type
        .children()
        .into_iter()
        .map(|child| import_field(&child))
        .collect();
    let type_ids: Vec<i8> = js_type
        .type_ids()
        .to_vec()
        .into_iter()
        .map(|val| i8::try_from(val).unwrap())
        .collect();

    let union_fields = UnionFields::new(type_ids, fields);
    match js_type.mode() {
        UnionMode::Dense => DataType::Union(union_fields, arrow_schema::UnionMode::Dense),
        UnionMode::Sparse => DataType::Union(union_fields, arrow_schema::UnionMode::Sparse),
    }
}

fn import_fixed_size_binary(js_type: &JSFixedSizeBinary) -> DataType {
    DataType::FixedSizeBinary(js_type.byte_width())
}

fn import_fixed_size_list(js_type: &JSFixedSizeList) -> DataType {
    let mut children = js_type.children();
    assert_eq!(children.len(), 1);
    let child = children.pop().unwrap();
    let field = import_field(&child);
    DataType::FixedSizeList(Arc::new(field), js_type.list_size())
}

fn import_map(js_type: &JSMap_) -> DataType {
    let mut children = js_type.children();
    assert_eq!(children.len(), 1);
    let child = children.pop().unwrap();
    let field = import_field(&child);
    DataType::Map(Arc::new(field), js_type.keys_sorted())
}

pub fn import_data_type(js_type: &JSDataType) -> DataType {
    use super::r#enum::Type;

    match js_type.type_id() {
        // Type None should never be initialized
        Type::NONE => panic!("Type None"),
        Type::Null => DataType::Null,
        Type::Int => import_int(js_type.unchecked_ref()),
        Type::Float => import_float(js_type.unchecked_ref()),
        Type::Binary => DataType::Binary,
        Type::Utf8 => DataType::Utf8,
        Type::Bool => DataType::Boolean,
        Type::Decimal => import_decimal(js_type.unchecked_ref()),
        Type::Date => import_date(js_type.unchecked_ref()),
        Type::Time => import_time(js_type.unchecked_ref()),
        Type::Timestamp => import_timestamp(js_type.unchecked_ref()),
        Type::Interval => import_interval(js_type.unchecked_ref()),
        Type::Duration => import_duration(js_type.unchecked_ref()),
        Type::List => import_list(js_type.unchecked_ref()),
        Type::Struct => import_struct(js_type.unchecked_ref()),
        Type::Union => import_union(js_type.unchecked_ref()),
        Type::FixedSizeBinary => import_fixed_size_binary(js_type.unchecked_ref()),
        Type::FixedSizeList => import_fixed_size_list(js_type.unchecked_ref()),
        Type::Map => import_map(js_type.unchecked_ref()),
    }
}
