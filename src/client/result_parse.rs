use tabled::{builder::Builder, Style};

use crate::{
    server::server::db_connection::QueryResult,
    util::{convert::from_row_value, dbtype::Value},
};

pub fn result_parse(result_inner: QueryResult) -> Result<(), String> {
    // setting the table column
    let mut table_column: Vec<String> = Vec::new();
    for col in result_inner.column_names {
        table_column.push(col);
    }

    // setting the table rows
    let mut table_rows: Vec<Vec<String>> = Vec::new();
    for row in result_inner.row_values {
        let mut row_value: Vec<String> = Vec::new();
        for value in from_row_value(row) {
            match value {
                Value::String(s) => row_value.push(s),
                Value::I32(i) => row_value.push(i.to_string()),
                Value::Float(f) => row_value.push(f.to_string()),
                Value::Timestamp(t) => row_value.push(t.to_string()),
                Value::I64(i) => row_value.push(i.to_string()),
                Value::Double(d) => row_value.push(d.to_string()),
                Value::Bool(b) => row_value.push(b.to_string()),
            }
        }
        table_rows.push(row_value);
    }

    let mut builder = Builder::default();
    builder.set_columns(table_column);

    for row in table_rows {
        builder.add_record(row);
    }

    // pretty table
    let mut table = builder.build();
    table.with(Style::rounded());
    // println!("Print something here!");
    // will print the table on the terminal
    println!("{}", table);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::util::{convert::to_row_value, dbtype::Value};

    use super::*;

    #[test]
    fn test_parse() {
        let result = QueryResult {
            column_names: vec![
                "Name".to_string(),
                "Age".to_string(),
                "Height".to_string(),
                "Weight".to_string(),
                "Location".to_string(),
            ],
            row_values: vec![
                to_row_value(vec![
                    Value::String("John Adams".to_string()),
                    Value::I32(20),
                    Value::Float(5.5),
                    Value::Float(150.0),
                    Value::String("New York".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Jane Washington".to_string()),
                    Value::I32(21),
                    Value::Float(5.3),
                    Value::Float(130.0),
                    Value::String("Boston".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("George Jefferson".to_string()),
                    Value::I32(22),
                    Value::Float(5.7),
                    Value::Float(160.0),
                    Value::String("San Francisco".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Thomas Jefferson".to_string()),
                    Value::I32(23),
                    Value::Float(5.7),
                    Value::Float(160.0),
                    Value::String("New York".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Abraham Lincoln".to_string()),
                    Value::I32(24),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Chicago".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Andrew Jackson".to_string()),
                    Value::I32(25),
                    Value::Float(5.8),
                    Value::Float(170.0),
                    Value::String("Charleston".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Ulysses S. Grant".to_string()),
                    Value::I32(26),
                    Value::Float(6.0),
                    Value::Float(190.0),
                    Value::String("Washington DC".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Rutherford B. Hayes".to_string()),
                    Value::I32(27),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Indianapolis".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("James Garfield".to_string()),
                    Value::I32(28),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Cleveland".to_string()),
                ]),
            ],
        };

        result_parse(result).unwrap();
    }

    #[test]
    fn test_parse_empty() {
        let result = QueryResult {
            column_names: vec![],
            row_values: vec![],
        };

        assert_eq!(result_parse(result).unwrap(), ());
    }

    #[test]
    fn test_parse_one() {
        let result = QueryResult {
            column_names: vec!["Name".to_string()],
            row_values: vec![to_row_value(vec![Value::String("John Adams".to_string())])],
        };

        result_parse(result).unwrap();
    }

    #[test]
    fn test_parse_none() {
        let result = QueryResult {
            column_names: vec!["Name".to_string()],
            row_values: vec![],
        };

        result_parse(result).unwrap();
    }
}
