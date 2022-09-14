use prost_types::Timestamp;

enum ValueType {
    ColString(String),
    ColI32(i32),
    ColFloat(f32),
    ColTimestamp(Timestamp),
    ColI64(i64),
    ColDouble(f64),
    ColBool(bool),
}