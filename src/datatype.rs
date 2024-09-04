// Allow struct fields to match Arrow representation, even when not currently used
#![allow(dead_code)]

use arrow_schema::{FieldRef, Fields, IntervalUnit, TimeUnit, UnionFields, UnionMode};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

pub struct DataType(arrow_schema::DataType);

/// Null type
#[wasm_bindgen]
pub struct Null;

/// A boolean datatype representing the values `true` and `false`.
#[wasm_bindgen]
pub struct Boolean;

/// A signed 8-bit integer.
#[wasm_bindgen]
pub struct Int8;

/// A signed 16-bit integer.
#[wasm_bindgen]
pub struct Int16;

/// A signed 32-bit integer.
#[wasm_bindgen]
pub struct Int32;

/// A signed 64-bit integer.
#[wasm_bindgen]
pub struct Int64;

/// An unsigned 8-bit integer.
#[wasm_bindgen]
pub struct UInt8;

/// An unsigned 16-bit integer.
#[wasm_bindgen]
pub struct UInt16;

/// An unsigned 32-bit integer.
#[wasm_bindgen]
pub struct UInt32;

/// An unsigned 64-bit integer.
#[wasm_bindgen]
pub struct UInt64;

/// A 16-bit floating point number.
#[wasm_bindgen]
pub struct Float16;

/// A 32-bit floating point number.
#[wasm_bindgen]
pub struct Float32;

/// A 64-bit floating point number.
#[wasm_bindgen]
pub struct Float64;

/// A timestamp with an optional timezone.
///
/// Time is measured as a Unix epoch, counting the seconds from
/// 00:00:00.000 on 1 January 1970, excluding leap seconds,
/// as a 64-bit integer.
///
/// The time zone is a string indicating the name of a time zone, one of:
///
/// * As used in the Olson time zone database (the "tz database" or
///   "tzdata"), such as "America/New_York"
/// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
///
/// Timestamps with a non-empty timezone
/// ------------------------------------
///
/// If a Timestamp column has a non-empty timezone value, its epoch is
/// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
/// (the Unix epoch), regardless of the Timestamp's own timezone.
///
/// Therefore, timestamp values with a non-empty timezone correspond to
/// physical points in time together with some additional information about
/// how the data was obtained and/or how to display it (the timezone).
///
///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
///   application may prefer to display it as "January 1st 1970, 01h00" in
///   the Europe/Paris timezone (which is the same physical point in time).
///
/// One consequence is that timestamp values with a non-empty timezone
/// can be compared and ordered directly, since they all share the same
/// well-known point of reference (the Unix epoch).
///
/// Timestamps with an unset / empty timezone
/// -----------------------------------------
///
/// If a Timestamp column has no timezone value, its epoch is
/// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
///
/// Therefore, timestamp values without a timezone cannot be meaningfully
/// interpreted as physical points in time, but only as calendar / clock
/// indications ("wall clock time") in an unspecified timezone.
///
///   For example, the timestamp value 0 with an empty timezone string
///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
///   is not enough information to interpret it as a well-defined physical
///   point in time.
///
/// One consequence is that timestamp values without a timezone cannot
/// be reliably compared or ordered, since they may have different points of
/// reference.  In particular, it is *not* possible to interpret an unset
/// or empty timezone as the same as "UTC".
///
/// Conversion between timezones
/// ----------------------------
///
/// If a Timestamp column has a non-empty timezone, changing the timezone
/// to a different non-empty value is a metadata-only operation:
/// the timestamp values need not change as their point of reference remains
/// the same (the Unix epoch).
///
/// However, if a Timestamp column has no timezone value, changing it to a
/// non-empty value requires to think about the desired semantics.
/// One possibility is to assume that the original timestamp values are
/// relative to the epoch of the timezone being set; timestamp values should
/// then adjusted to the Unix epoch (for example, changing the timezone from
/// empty to "Europe/Paris" would require converting the timestamp values
/// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
/// nevertheless correct).
#[wasm_bindgen]
pub struct Timestamp(TimeUnit, Option<Arc<str>>);

impl Timestamp {
    pub fn new(unit: TimeUnit, tz: Option<Arc<str>>) -> Self {
        Self(unit, tz)
    }
}

/// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
/// in days (32 bits).
#[wasm_bindgen]
pub struct Date32;

/// A 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
/// in milliseconds (64 bits). Values are evenly divisible by 86400000.
#[wasm_bindgen]
pub struct Date64;

/// A 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
#[wasm_bindgen]
pub struct Time32(TimeUnit);

impl Time32 {
    pub fn new(unit: TimeUnit) -> Self {
        Self(unit)
    }
}

/// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
#[wasm_bindgen]
pub struct Time64(TimeUnit);

impl Time64 {
    pub fn new(unit: TimeUnit) -> Self {
        Self(unit)
    }
}

/// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
#[wasm_bindgen]
pub struct Duration(TimeUnit);

impl Duration {
    pub fn new(unit: TimeUnit) -> Self {
        Self(unit)
    }
}

/// A "calendar" interval which models types that don't necessarily
/// have a precise duration without the context of a base timestamp (e.g.
/// days can differ in length during day light savings time transitions).
#[wasm_bindgen]
pub struct Interval(IntervalUnit);

impl Interval {
    pub fn new(unit: IntervalUnit) -> Self {
        Self(unit)
    }
}

/// Opaque binary data of variable length.
///
/// A single Binary array can store up to [`i32::MAX`] bytes
/// of binary data in total
#[wasm_bindgen]
pub struct Binary;

/// Opaque binary data of fixed size.
/// Enum parameter specifies the number of bytes per value.
#[wasm_bindgen]
pub struct FixedSizeBinary(i32);

impl FixedSizeBinary {
    pub fn new(size: i32) -> Self {
        Self(size)
    }
}

/// Opaque binary data of variable length and 64-bit offsets.
///
/// A single LargeBinary array can store up to [`i64::MAX`] bytes
/// of binary data in total
#[wasm_bindgen]
pub struct LargeBinary;

/// A variable-length string in Unicode with UTF-8 encoding
///
/// A single Utf8 array can store up to [`i32::MAX`] bytes
/// of string data in total
#[wasm_bindgen]
pub struct Utf8;

/// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
///
/// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
/// of string data in total
#[wasm_bindgen]
pub struct LargeUtf8;

/// A list of some logical data type with variable length.
///
/// A single List array can store up to [`i32::MAX`] elements in total
#[wasm_bindgen]
pub struct List(FieldRef);

impl List {
    pub fn new(field: FieldRef) -> Self {
        Self(field)
    }
}

/// A list of some logical data type with fixed length.
#[wasm_bindgen]
pub struct FixedSizeList(FieldRef, i32);

impl FixedSizeList {
    pub fn new(field: FieldRef, size: i32) -> Self {
        Self(field, size)
    }
}

/// A list of some logical data type with variable length and 64-bit offsets.
///
/// A single LargeList array can store up to [`i64::MAX`] elements in total
#[wasm_bindgen]
pub struct LargeList(FieldRef);

impl LargeList {
    pub fn new(field: FieldRef) -> Self {
        Self(field)
    }
}

/// A nested datatype that contains a number of sub-fields.
#[wasm_bindgen]
pub struct Struct(Fields);

impl Struct {
    pub fn new(fields: Fields) -> Self {
        Self(fields)
    }
}

/// A nested datatype that can represent slots of differing types. Components:
///
/// 1. [`UnionFields`]
/// 2. The type of union (Sparse or Dense)
#[wasm_bindgen]
pub struct Union(UnionFields, UnionMode);

impl Union {
    pub fn new(fields: UnionFields, mode: UnionMode) -> Self {
        Self(fields, mode)
    }
}

/// A dictionary encoded array (`key_type`, `value_type`), where
/// each array element is an index of `key_type` into an
/// associated dictionary of `value_type`.
///
/// Dictionary arrays are used to store columns of `value_type`
/// that contain many repeated values using less memory, but with
/// a higher CPU overhead for some operations.
///
/// This type mostly used to represent low cardinality string
/// arrays or a limited set of primitive types as integers.
#[wasm_bindgen]
pub struct Dictionary(Box<arrow_schema::DataType>, Box<arrow_schema::DataType>);

impl Dictionary {
    pub fn new(
        key_type: Box<arrow_schema::DataType>,
        value_type: Box<arrow_schema::DataType>,
    ) -> Self {
        Self(key_type, value_type)
    }
}

/// Exact 128-bit width decimal value with precision and scale
///
/// * precision is the total number of digits
/// * scale is the number of digits past the decimal
///
/// For example the number 123.45 has precision 5 and scale 2.
///
/// In certain situations, scale could be negative number. For
/// negative scale, it is the number of padding 0 to the right
/// of the digits.
///
/// For example the number 12300 could be treated as a decimal
/// has precision 3 and scale -2.
#[wasm_bindgen]
pub struct Decimal128(u8, i8);

impl Decimal128 {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self(precision, scale)
    }
}

/// Exact 256-bit width decimal value with precision and scale
///
/// * precision is the total number of digits
/// * scale is the number of digits past the decimal
///
/// For example the number 123.45 has precision 5 and scale 2.
///
/// In certain situations, scale could be negative number. For
/// negative scale, it is the number of padding 0 to the right
/// of the digits.
///
/// For example the number 12300 could be treated as a decimal
/// has precision 3 and scale -2.
#[wasm_bindgen]
pub struct Decimal256(u8, i8);

impl Decimal256 {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self(precision, scale)
    }
}

/// A Map is a logical nested type that is represented as
///
/// `List<entries: Struct<key: K, value: V>>`
///
/// The keys and values are each respectively contiguous.
/// The key and value types are not constrained, but keys should be
/// hashable and unique.
/// Whether the keys are sorted can be set in the `bool` after the `Field`.
///
/// In a field with Map type, the field has a child Struct field, which then
/// has two children: key type and the second the value type. The names of the
/// child fields may be respectively "entries", "key", and "value", but this is
/// not enforced.
#[wasm_bindgen]
pub struct Map_(FieldRef, bool);

impl Map_ {
    pub fn new(field: FieldRef, sorted: bool) -> Self {
        Self(field, sorted)
    }
}

/// A run-end encoding (REE) is a variation of run-length encoding (RLE). These
/// encodings are well-suited for representing data containing sequences of the
/// same value, called runs. Each run is represented as a value and an integer giving
/// the index in the array where the run ends.
///
/// A run-end encoded array has no buffers by itself, but has two child arrays. The
/// first child array, called the run ends array, holds either 16, 32, or 64-bit
/// signed integers. The actual values of each run are held in the second child array.
///
/// These child arrays are prescribed the standard names of "run_ends" and "values"
/// respectively.
#[wasm_bindgen]
pub struct RunEndEncoded(FieldRef, FieldRef);

impl RunEndEncoded {
    pub fn new(run_ends: FieldRef, values: FieldRef) -> Self {
        Self(run_ends, values)
    }
}
