use arrow_schema::DataType;
use wasm_bindgen::prelude::*;

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

pub fn import_data_type(js_type: &JSDataType) -> DataType {
    use super::r#enum::Type;

    match js_type.type_id() {
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

        _ => todo!(),
    }
}
