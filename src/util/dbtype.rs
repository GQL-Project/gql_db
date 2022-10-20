use chrono::NaiveDateTime;
use core::mem::size_of;
use prost_types::Timestamp;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType};

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
    Nullable(Box<Column>), // Heap allocations are necessary for recursive types.
}

impl Column {
    pub fn from_col_def(data_type: &ColumnDef) -> Result<Column, String> {
        // If the Null option is present, then the column is nullable.
        let is_nullable: bool = data_type.options.iter().any(|option| match option.option {
            ColumnOption::Null => true,
            _ => false,
        });
        let data_col = match data_type.data_type {
            DataType::SmallInt(_) => Column::I32,
            DataType::Int(_) => Column::I64,
            DataType::Float(_) => Column::Float,
            DataType::Double => Column::Double,
            DataType::Boolean => Column::Bool,
            DataType::Timestamp => Column::Timestamp,
            DataType::Char(Some(size)) => Column::String(size as u16),
            DataType::Varchar(Some(size)) => Column::String(size as u16),
            DataType::Char(None) => Column::String(1),
            DataType::Varchar(None) => Column::String(1),
            _ => Err("Unsupported data type")?,
        };
        if is_nullable {
            Ok(Column::Nullable(Box::new(data_col)))
        } else {
            Ok(data_col)
        }
    }

    pub fn decode_type(val: u16) -> Column {
        if val & (1 << 15) != 0 {
            // This is a nullable type, find it's base type.
            Column::Nullable(Box::new(Column::decode_type(val & !(1 << 15))))
        } else {
            match val {
                0 => Column::I32,
                1 => Column::I64,
                2 => Column::Float,
                3 => Column::Double,
                4 => Column::Bool,
                5 => Column::Timestamp,
                // If the second most significant bit is set, then the column is a string.
                x if x & (1 << 14) != 0 => Column::String((x & !(1 << 14)) as u16),
                _ => panic!("Invalid column type"),
            }
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
            Column::String(x) => (1 << 14) | (*x as u16),
            Column::Nullable(x) => (1 << 15) | x.encode_type(),
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
            Column::Nullable(x) => {
                // Check if the value is null.
                let val: u8 = read_type(page, offset)?;
                if val == 0 {
                    Ok(Value::Null)
                } else {
                    x.read(page, offset + size_of::<u8>())
                }
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
            (Column::String(size), Value::String(x)) => {
                write_string(page, offset, &x, *size as usize)
            }
            // Type conversions
            (Column::I32, Value::I64(x)) => write_type(page, offset, *x as i32),
            (Column::I64, Value::I32(x)) => write_type(page, offset, *x as i64),
            (Column::Float, Value::Double(x)) => write_type(page, offset, *x as f32),
            (Column::Double, Value::Float(x)) => write_type(page, offset, *x as f64),
            // Null cases
            (Column::Nullable(_), Value::Null) => write_type(page, offset, 0u8),
            (Column::Nullable(x), y) => {
                // Attempt to write the value, and only if it succeeds, write the null byte.
                x.write(y, page, offset + size_of::<u8>())?;
                write_type(page, offset, 1u8)
            }
            _ => Err(
                "Unexpected Type, types must always map to their corresponding rows".to_string(),
            ),
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
            // Add a single byte overhead for the null flag.
            Column::Nullable(x) => size_of::<u8>() + x.size(),
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
            // Null cases
            (Column::Nullable(_), Value::Null) => true,
            (Column::Nullable(x), y) => x.match_value(y),
            _ => false,
        }
    }

    pub fn parse(&self, str: &String) -> Result<Value, String> {
        let res = match self {
            Column::I32 => Value::I32(
                str.parse()
                    .map_err(|_x| format!("Could not parse value {str} into type Int32"))?,
            ),
            Column::Float => Value::Float(
                str.parse()
                    .map_err(|_x| format!("Could not parse value {str} into type Float"))?,
            ),
            Column::String(_) => Value::String(str.clone()),
            Column::Bool => Value::Bool(
                str.parse()
                    .map_err(|_x| format!("Could not parse value {str} into type Bool"))?,
            ),
            Column::Timestamp => Value::Timestamp(parse_time(str)?),
            Column::I64 => Value::I64(
                str.parse()
                    .map_err(|_x| format!("Could not parse value {str} into type Int64"))?,
            ),
            Column::Double => Value::Double(
                str.parse()
                    .map_err(|_x| format!("Could not parse value {str} into type Double"))?,
            ),
            Column::Nullable(x) => {
                if str == "" {
                    Value::Null
                } else {
                    x.parse(str)?
                }
            }
        };
        Ok(res)
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
            Column::Nullable(x) => format!("Nullable({})", x.to_string()),
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

fn parse_time(str: &String) -> Result<Timestamp, String> {
    let time = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S");
    if let Ok(x) = time {
        Ok(Timestamp {
            seconds: x.timestamp(),
            nanos: x.timestamp_subsec_nanos() as i32,
        })
    } else {
        Err(format!("Could not parse value {str} into type time"))
    }
}
