use core::mem::size_of;
use prost_types::Timestamp;

use crate::fileio::pageio::{read_string, read_type, write_string, write_type, Page};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    I32(i32),
    Float(f32),
    Timestamp(Timestamp),
    I64(i64),
    Double(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    // Strings have a given length value (in bytes).
    String(u16),
    I32,
    Timestamp,
    I64,
    Float,
    Double,
    Bool,
}

impl Column {
    pub fn decode_type(val: u16) -> Column {
        match val {
            0 => Column::I32,
            1 => Column::I64,
            2 => Column::Float,
            3 => Column::Double,
            4 => Column::Bool,
            5 => Column::Timestamp,
            x if x & (1 << 15) != 0 => Column::String((x & !(1 << 15)) as u16),
            _ => panic!("Invalid column type"),
        }
    }

    pub fn encode_type(&self) -> u16 {
        match self {
            Column::I32 => 0,
            Column::I64 => 1,
            Column::Float => 2,
            Column::Double => 3,
            Column::Bool => 4,
            Column::Timestamp => 5,
            Column::String(x) => (1 << 15) | (*x as u16),
        }
    }

    pub fn read(&self, page: &Page, offset: usize) -> Result<Value, String> {
        match self {
            Column::I32 => {
                let val: i32 = read_type(page, offset)?;
                Ok(Value::I32(val))
            }
            Column::I64 => {
                let val: i64 = read_type(page, offset)?;
                Ok(Value::I64(val))
            }
            Column::Float => {
                let val: f32 = read_type(page, offset)?;
                Ok(Value::Float(val))
            }
            Column::Double => {
                let val: f64 = read_type(page, offset)?;
                Ok(Value::Double(val))
            }
            Column::Bool => {
                let val: bool = read_type(page, offset)?;
                Ok(Value::Bool(val))
            }
            Column::Timestamp => {
                let val: Timestamp = read_type(page, offset)?;
                Ok(Value::Timestamp(val))
            }
            Column::String(len) => {
                let val = read_string(page, offset, *len as usize)?;
                Ok(Value::String(val))
            }
        }
    }

    pub fn write(&self, row: &Value, page: &mut Page, offset: usize) -> Result<(), String> {
        match (self, row) {
            (Column::I32, Value::I32(x)) => write_type(page, offset, *x),
            (Column::I64, Value::I64(x)) => write_type(page, offset, *x),
            (Column::Float, Value::Float(x)) => write_type(page, offset, *x),
            (Column::Double, Value::Double(x)) => write_type(page, offset, *x),
            (Column::Bool, Value::Bool(x)) => write_type(page, offset, *x),
            (Column::Timestamp, Value::Timestamp(x)) => write_type(page, offset, x.clone()),
            // Type conversions:
            (Column::I32, Value::I64(x)) => write_type(page, offset, *x as i32),
            (Column::I64, Value::I32(x)) => write_type(page, offset, *x as i64),
            (Column::Float, Value::Double(x)) => write_type(page, offset, *x as f32),
            (Column::Double, Value::Float(x)) => write_type(page, offset, *x as f64),
            (Column::String(size), Value::String(x)) => {
                write_string(page, offset, &x, *size as usize)
            }
            _ => panic!("Unexpected Type, types must always map to their corresponding rows"),
        }?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        match self {
            Column::I32 => size_of::<i32>(),
            Column::I64 => size_of::<i64>(),
            Column::Float => size_of::<f32>(),
            Column::Double => size_of::<f64>(),
            Column::Bool => size_of::<bool>(),
            Column::Timestamp => size_of::<i32>(),
            Column::String(x) => (*x as usize) * size_of::<u8>(),
        }
    }

    pub fn match_value(&self, val: &Value) -> bool {
        match (self, val) {
            (Column::I32, Value::I32(_)) => true,
            (Column::I64, Value::I64(_)) => true,
            (Column::Float, Value::Float(_)) => true,
            (Column::Double, Value::Double(_)) => true,
            (Column::Bool, Value::Bool(_)) => true,
            (Column::Timestamp, Value::Timestamp(_)) => true,
            (Column::String(_), Value::String(_)) => true,
            // Type coercions
            (Column::I64, Value::I32(_)) => true,
            (Column::Double, Value::Float(_)) => true,
            (Column::Float, Value::Double(_)) => true,
            (Column::I32, Value::I64(x)) => i32::try_from(*x).is_ok(),
            _ => false,
        }
    }
}

impl ToString for Column {
    fn to_string(&self) -> String {
        match self {
            Column::I32 => "I32".to_string(),
            Column::I64 => "I64".to_string(),
            Column::Float => "Float".to_string(),
            Column::Double => "Double".to_string(),
            Column::Bool => "Bool".to_string(),
            Column::Timestamp => "Timestamp".to_string(),
            Column::String(x) => format!("String({})", x),
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::I32(x) => format!("I32({})", x),
            Value::I64(x) => format!("I64({})", x),
            Value::Float(x) => format!("Float({})", x),
            Value::Double(x) => format!("Double({})", x),
            Value::Bool(x) => format!("Bool({})", x),
            Value::Timestamp(x) => format!("Timestamp({})", x),
            Value::String(x) => format!("String({})", x),
            Value::Null => "Null()".to_string(),
        }
    }
}
