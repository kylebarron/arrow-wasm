use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub enum UnionMode {
    Sparse = 0,
    Dense = 1,
}

#[wasm_bindgen]
pub enum Precision {
    Half = 0,
    Single = 1,
    Double = 2,
}

#[wasm_bindgen]
pub enum DateUnit {
    Day = 0,
    Millisecond = 1,
}

#[wasm_bindgen]
pub enum TimeUnit {
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}

#[wasm_bindgen]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
    MonthDayNano = 2,
}

#[wasm_bindgen(js_name = Type)]
pub enum r#Type {
    /// The default placeholder type
    NONE = 0,
    /// A NULL type having no physical storage
    Null = 1,
    /// Signed or unsigned 8, 16, 32, or 64-bit little-endian integer
    Int = 2,
    /// 2, 4, or 8-byte floating point value
    Float = 3,
    /// Variable-length bytes (no guarantee of UTF8-ness)
    Binary = 4,
    /// UTF8 variable-length string as List<Char>
    Utf8 = 5,
    /// Boolean as 1 bit, LSB bit-packed ordering
    Bool = 6,
    /// Precision-and-scale-based decimal type. Storage type depends on the parameters.
    Decimal = 7,
    /// int32_t days or int64_t milliseconds since the UNIX epoch
    Date = 8,
    /// Time as signed 32 or 64-bit integer, representing either seconds, milliseconds,
    /// microseconds, or nanoseconds since midnight since midnight
    Time = 9,
    /// Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond)
    Timestamp = 10,
    /// YEAR_MONTH or DAY_TIME interval in SQL style
    Interval = 11,
    /// A list of some logical data type
    List = 12,
    /// Struct of logical types
    Struct = 13,
    /// Union of logical types
    Union = 14,
    /// Fixed-size binary. Each value occupies the same number of bytes
    FixedSizeBinary = 15,
    /// Fixed-size list. Each value occupies the same number of bytes
    FixedSizeList = 16,
    /// Map of named logical types
    Map = 17,
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration = 18,
}

#[wasm_bindgen]
pub enum BufferType {
    /**
     * used in List type, Dense Union and variable length primitive types (String, Binary)
     */
    Offset = 0,

    /**
     * actual data, either fixed width primitive types in slots or variable width delimited by an OFFSET vector
     */
    Data = 1,

    /**
     * Bit vector indicating if each value is null
     */
    Validity = 2,

    /**
     * Type vector used in Union type
     */
    Type = 3,
}
