use prost_types::Timestamp;

pub enum ValueType {
    ValString(String),
    ValI32(i32),
    ValFloat(f32),
    ValTimestamp(Timestamp),
    ValI64(i64),
    ValDouble(f64),
    ValBool(bool),
}