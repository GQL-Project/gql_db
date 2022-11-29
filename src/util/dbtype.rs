use chrono::NaiveDateTime;
use core::mem::size_of;
use prost_types::Timestamp;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::fileio::pageio::{read_string, read_type, write_string, write_type, Page};

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    I32(i32),
    Float(f32),
    Timestamp(Timestamp),
    I64(i64),
    Double(f64),
    Bool(bool),
    Null(Column),
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
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
            _ => Err(format!("Unsupported data type: {}", data_type.data_type))?,
        };
        if is_nullable {
            Ok(Column::Nullable(Box::new(data_col)))
        } else {
            Ok(data_col)
        }
    }

    pub fn from_datatype_def(data_type: &DataType) -> Result<Column, String> {
        // No Nullable
        let data_col = match data_type {
            DataType::SmallInt(_) => Column::I32,
            DataType::Int(_) => Column::I64,
            DataType::Float(_) => Column::Float,
            DataType::Double => Column::Double,
            DataType::Boolean => Column::Bool,
            DataType::Timestamp => Column::Timestamp,
            DataType::Char(Some(size)) => Column::String(*size as u16),
            DataType::Varchar(Some(size)) => Column::String(*size as u16),
            DataType::Char(None) => Column::String(1),
            DataType::Varchar(None) => Column::String(1),
            _ => Err(format!("Unsupported data type: {}", data_type))?,
        };
        Ok(data_col)
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
                    Ok(Value::Null(*x.clone()))
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
            // Null cases
            (Column::Nullable(_), Value::Null(_)) => write_type(page, offset, 0u8),
            (Column::Nullable(x), y) => {
                // Attempt to write the value, and only if it succeeds, write the null byte.
                x.write(y, page, offset + size_of::<u8>())?;
                write_type(page, offset, 1u8)
            }
            _ => {
                // This should never happen, as types should already be coerced to the correct type at this point
                // If it does happen, this can mess up diffs.
                println!("Warning: type mismatch: {:?} {:?}", self, row);
                self.write(&self.coerce_type(row.clone())?, page, offset)
            }
        }?;
        Ok(())
    }

    // Convert a value to the correct type expected by this column.
    pub fn coerce_type(&self, value: Value) -> Result<Value, String> {
        match (self, &value) {
            (Column::I32, Value::I32(_)) => Ok(value),
            (Column::I64, Value::I64(_)) => Ok(value),
            (Column::Float, Value::Float(_)) => Ok(value),
            (Column::Double, Value::Double(_)) => Ok(value),
            (Column::Bool, Value::Bool(_)) => Ok(value),
            (Column::Timestamp, Value::Timestamp(_)) => Ok(value),
            (Column::String(_), Value::String(_)) => Ok(value),
            // Type conversions
            (Column::I32, Value::I64(x)) => Ok(Value::I32(*x as i32)),
            (Column::I64, Value::I32(x)) => Ok(Value::I64(*x as i64)),
            (Column::Float, Value::Double(x)) => Ok(Value::Float(*x as f32)),
            (Column::Double, Value::Float(x)) => Ok(Value::Double(*x as f64)),
            // Floats to Ints
            (Column::I32, Value::Float(x)) => Ok(Value::I32(*x as i32)),
            (Column::I32, Value::Double(x)) => Ok(Value::I32(*x as i32)),
            (Column::I64, Value::Float(x)) => Ok(Value::I64(*x as i64)),
            (Column::I64, Value::Double(x)) => Ok(Value::I64(*x as i64)),
            // Ints to Floats
            (Column::Float, Value::I32(x)) => Ok(Value::Float(*x as f32)),
            (Column::Float, Value::I64(x)) => Ok(Value::Float(*x as f32)),
            (Column::Double, Value::I32(x)) => Ok(Value::Double(*x as f64)),
            (Column::Double, Value::I64(x)) => Ok(Value::Double(*x as f64)),
            // Time stamps
            (Column::Timestamp, Value::String(x)) => Ok(Value::Timestamp(parse_time(x)?)),
            // Null cases
            (Column::Nullable(x), Value::Null(_)) => Ok(Value::Null(*x.clone())),
            (Column::Nullable(x), _) => x.coerce_type(value),
            _ => Err(format!(
                "Unexpected Type, could not promote value {:?} to type {:?}",
                value, self,
            )),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Column::I32 => size_of::<i32>(),
            Column::I64 => size_of::<i64>(),
            Column::Float => size_of::<f32>(),
            Column::Double => size_of::<f64>(),
            Column::Bool => size_of::<bool>(),
            Column::Timestamp => size_of::<Timestamp>(),
            Column::String(x) => (*x as usize) * size_of::<u8>(),
            // Add a single byte overhead for the null flag.
            Column::Nullable(x) => size_of::<u8>() + x.size(),
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
                    Value::Null(*x.clone())
                } else {
                    x.parse(str)?
                }
            }
        };
        Ok(res)
    }

    pub fn from_sql_value(&self, parse: &SqlValue) -> Result<Value, String> {
        match parse {
            SqlValue::Number(x, _) => self.parse(&x.to_string()),
            SqlValue::SingleQuotedString(x) => self.parse(&x.to_string()),
            SqlValue::DoubleQuotedString(x) => self.parse(&x.to_string()),
            SqlValue::Boolean(x) => Ok(Value::Bool(*x)),
            SqlValue::Null => Ok(Value::Null(Column::I32)),
            _ => Err(format!("Unsupported value type: {:?}", parse)),
        }
    }

    /// Gets a default value for this column type.
    pub fn get_default_value(&self) -> Value {
        match self {
            Column::I32 => Value::I32(0),
            Column::I64 => Value::I64(0),
            Column::Float => Value::Float(0.0),
            Column::Double => Value::Double(0.0),
            Column::Bool => Value::Bool(false),
            Column::Timestamp => {
                Value::Timestamp(parse_time(&"1970-01-01 00:00:00".to_string()).unwrap())
            }
            Column::String(_) => Value::String(String::new()),
            Column::Nullable(x) => Value::Null(*x.clone()),
        }
    }

    pub fn match_type(&self, other: &Column) -> bool {
        self.coerce_type(other.get_default_value()).is_ok()
    }

    pub fn as_nullable(self) -> Column {
        match self {
            Column::Nullable(_) => self,
            _ => Column::Nullable(Box::new(self)),
        }
    }
}

impl Value {
    pub fn from_sql_value(parse: &SqlValue) -> Result<Value, String> {
        match parse {
            SqlValue::Number(x, _) => Column::I64.parse(x).or_else(|_| Column::Double.parse(x)),
            SqlValue::SingleQuotedString(x) => Ok(Value::String(x.clone())),
            SqlValue::DoubleQuotedString(x) => Ok(Value::String(x.clone())),
            SqlValue::Boolean(x) => Ok(Value::Bool(*x)),
            SqlValue::Null => Ok(Value::Null(Column::I32)),
            _ => Err(format!("Unsupported value type: {:?}", parse)),
        }
    }

    pub fn get_coltype(&self) -> Column {
        match self {
            Value::I32(_) => Column::I32,
            Value::I64(_) => Column::I64,
            Value::Float(_) => Column::Float,
            Value::Double(_) => Column::Double,
            Value::Bool(_) => Column::Bool,
            Value::Timestamp(_) => Column::Timestamp,
            Value::String(_) => Column::String(0),
            Value::Null(x) => Column::Nullable(Box::new(x.clone())),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
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
            Value::Null(_) => "Null()".to_string(),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::I32(x), Value::I32(y)) => x.partial_cmp(y),
            (Value::I64(x), Value::I64(y)) => x.partial_cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
            (Value::Double(x), Value::Double(y)) => x.partial_cmp(y),
            (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
            (Value::Timestamp(x), Value::Timestamp(y)) => {
                x.seconds.partial_cmp(&y.seconds).and_then(|opt| {
                    if opt == Ordering::Equal {
                        x.nanos.partial_cmp(&y.nanos)
                    } else {
                        Some(opt)
                    }
                })
            }
            (Value::String(x), Value::String(y)) => x.partial_cmp(y),
            // Type coercions
            (Value::I64(x), Value::I32(y)) => x.partial_cmp(&(*y as i64)),
            (Value::I64(x), Value::Double(y)) => (*x as f64).partial_cmp(y),
            (Value::I64(x), Value::Float(y)) => (*x as f32).partial_cmp(y),

            (Value::I32(x), Value::I64(y)) => (*x as i64).partial_cmp(y),
            (Value::I32(x), Value::Float(y)) => (*x as f32).partial_cmp(y),
            (Value::I32(x), Value::Double(y)) => (*x as f64).partial_cmp(y),

            (Value::Float(x), Value::I32(y)) => x.partial_cmp(&(*y as f32)),
            (Value::Float(x), Value::I64(y)) => x.partial_cmp(&(*y as f32)),
            (Value::Float(x), Value::Double(y)) => x.partial_cmp(&(*y as f32)),

            (Value::Double(x), Value::I32(y)) => x.partial_cmp(&(*y as f64)),
            (Value::Double(x), Value::I64(y)) => x.partial_cmp(&(*y as f64)),
            (Value::Double(x), Value::Float(y)) => x.partial_cmp(&(*y as f64)),
            // Null cases
            (Value::Null(_), Value::Null(_)) => Some(Ordering::Equal),
            (Value::Null(_), _) => Some(Ordering::Less),
            (_, Value::Null(_)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::I32(x) => {
                state.write_u8(0);
                x.hash(state);
            }
            Value::I64(x) => {
                state.write_u8(1);
                x.hash(state);
            }
            Value::Float(x) => {
                state.write_u8(2);
                x.to_bits().hash(state);
            }
            Value::Double(x) => {
                state.write_u8(3);
                x.to_bits().hash(state);
            }
            Value::Bool(x) => {
                state.write_u8(4);
                x.hash(state);
            }
            Value::Timestamp(x) => {
                state.write_u8(5);
                x.seconds.hash(state);
                x.nanos.hash(state);
            }
            Value::String(x) => {
                state.write_u8(6);
                x.hash(state);
            }
            Value::Null(_) => {
                state.write_u8(7);
            }
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(other) {
            Some(x) => x,
            None => Ordering::Less,
        }
    }
}

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

pub fn parse_time(str: &String) -> Result<Timestamp, String> {
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
