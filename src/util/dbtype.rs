use prost_types::Timestamp;

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    ValString(String),
    ValI32(i32),
    ValFloat(f32),
    ValTimestamp(Timestamp),
    ValI64(i64),
    ValDouble(f64),
    ValBool(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    // Strings have a given length value (in bytes).
    String(u16),
    I32,
    Timestamp,
    I64,
    Float,
    Double,
    Bool,
}