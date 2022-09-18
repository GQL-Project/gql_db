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

impl ColumnType {
    pub fn from(val: u16) -> ColumnType {
        match val {
            0 => ColumnType::I32,
            1 => ColumnType::I64,
            2 => ColumnType::Float,
            3 => ColumnType::Double,
            4 => ColumnType::Bool,
            5 => ColumnType::Timestamp,
            x if x & (1 << 15) != 0 => ColumnType::String((x & !(1 << 15)) as u16),
            _ => panic!("Invalid column type"),
        }
    }

    pub fn to_u16(&self) -> u16 {
        match self {
            ColumnType::I32 => 0,
            ColumnType::I64 => 1,
            ColumnType::Float => 2,
            ColumnType::Double => 3,
            ColumnType::Bool => 4,
            ColumnType::Timestamp => 5,
            ColumnType::String(x) => (1 << 15) | (*x as u16),
        }
    }
}
