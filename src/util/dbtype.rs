use core::mem::size_of;
use prost_types::Timestamp;

use crate::fileio::pageio::{read_string, read_type, write_string, write_type, Page};

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
    pub fn decode_type(val: u16) -> ColumnType {
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

    pub fn encode_type(&self) -> u16 {
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

    pub fn read(&self, page: &Page, offset: usize) -> Result<ValueType, String> {
        match self {
            ColumnType::I32 => {
                let val: i32 = read_type(page, offset)?;
                Ok(ValueType::ValI32(val))
            }
            ColumnType::I64 => {
                let val: i64 = read_type(page, offset)?;
                Ok(ValueType::ValI64(val))
            }
            ColumnType::Float => {
                let val: f32 = read_type(page, offset)?;
                Ok(ValueType::ValFloat(val))
            }
            ColumnType::Double => {
                let val: f64 = read_type(page, offset)?;
                Ok(ValueType::ValDouble(val))
            }
            ColumnType::Bool => {
                let val: bool = read_type(page, offset)?;
                Ok(ValueType::ValBool(val))
            }
            ColumnType::Timestamp => {
                let val: Timestamp = read_type(page, offset)?;
                Ok(ValueType::ValTimestamp(val))
            }
            ColumnType::String(len) => {
                let val = read_string(page, offset, *len as usize)?;
                Ok(ValueType::ValString(val))
            }
        }
    }

    pub fn write(&self, row: &ValueType, page: &mut Page, offset: usize) -> Result<(), String> {
        match (self, row) {
            (ColumnType::I32, ValueType::ValI32(x)) => write_type(page, offset, x),
            (ColumnType::I64, ValueType::ValI64(x)) => write_type(page, offset, x),
            (ColumnType::Float, ValueType::ValFloat(x)) => write_type(page, offset, x),
            (ColumnType::Double, ValueType::ValDouble(x)) => write_type(page, offset, x),
            (ColumnType::Bool, ValueType::ValBool(x)) => write_type(page, offset, x),
            (ColumnType::Timestamp, ValueType::ValTimestamp(x)) => write_type(page, offset, x),
            (ColumnType::String(size), ValueType::ValString(x)) => {
                write_string(page, offset, &x, *size as usize)
            }
            _ => panic!("Unexpected Type, types must always map to their corresponding rows"),
        }?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        match self {
            ColumnType::I32 => size_of::<i32>(),
            ColumnType::I64 => size_of::<i64>(),
            ColumnType::Float => size_of::<f32>(),
            ColumnType::Double => size_of::<f64>(),
            ColumnType::Bool => size_of::<bool>(),
            ColumnType::Timestamp => size_of::<i32>(),
            ColumnType::String(x) => (*x as usize) * size_of::<char>(),
        }
    }
}
