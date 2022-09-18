use crate::server::server::db_connection::*;
use crate::server::server::db_connection::cell_value::CellType::*;
use super::dbtype::ValueType;

/// The types generated by Prost aren't ideal. These help fix that.
pub fn to_connect_result(id: String) -> ConnectResult {
    ConnectResult { id }
}

pub fn to_query_result(column_names: Vec<String>, row_values: Vec<Vec<ValueType>>) -> QueryResult {
    QueryResult {
        column_names,
        row_values: row_values.into_iter().map(to_row_value).collect(),
    }
}

pub fn to_update_result(message: String) -> UpdateResult {
    UpdateResult { message }
}

pub fn to_row_value(row_values: Vec<ValueType>) -> RowValue {
    RowValue {
        cell_values: row_values.into_iter().map(to_value).collect(),
    }
}

pub fn to_value(value: ValueType) -> CellValue {
    match value {
        ValueType::ValString(s) => CellValue {
            cell_type: Some(ColString { 0: s }),
        },
        ValueType::ValI32(i) => CellValue {
            cell_type: Some(ColI32 { 0: i }),
        },
        ValueType::ValFloat(f) => CellValue {
            cell_type: Some(ColFloat { 0: f }),
        },
        ValueType::ValTimestamp(t) => CellValue {
            cell_type: Some(ColTime { 0: t }),
        },
        ValueType::ValI64(i) => CellValue {
            cell_type: Some(ColI64 { 0: i }),
        },
        ValueType::ValDouble(d) => CellValue {
            cell_type: Some(ColDouble { 0: d }),
        },
        ValueType::ValBool(b) => CellValue {
            cell_type: Some(ColBool { 0: b }),
        },
    }
}

pub fn from_row_value(row_value: RowValue) -> Vec<ValueType> {
    row_value
        .cell_values
        .into_iter()
        .map(from_value)
        .collect::<Vec<ValueType>>()
}

pub fn from_value(value: CellValue) -> ValueType {
    match value.cell_type.unwrap() {
        ColString { 0: s } => ValueType::ValString(s),
        ColI32 { 0: i } => ValueType::ValI32(i),
        ColFloat { 0: f } => ValueType::ValFloat(f),
        ColTime { 0: t } => ValueType::ValTimestamp(t),
        ColI64 { 0: i } => ValueType::ValI64(i),
        ColDouble { 0: d } => ValueType::ValDouble(d),
        ColBool { 0: b } => ValueType::ValBool(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_connect_result() {
        let id = "12345".to_string();
        let result = to_connect_result(id.clone());
        assert_eq!(result.id, id);
    }

    #[test]
    fn test_to_query_result() {
        let column_names = vec!["a".to_string(), "b".to_string()];
        let row_values = vec![
            vec![ValueType::ValString("a".to_string()), ValueType::ValString("b".to_string())],
            vec![ValueType::ValString("c".to_string()), ValueType::ValString("d".to_string())],
        ];
        let result = to_query_result(column_names.clone(), row_values.clone());
        assert_eq!(result.column_names, column_names);
        assert_eq!(
            result.row_values,
            vec![
                to_row_value(row_values[0].clone()),
                to_row_value(row_values[1].clone())
            ]
        );
    }

    #[test]
    fn test_to_update_result() {
        let message = "12345".to_string();
        let result = to_update_result(message.clone());
        assert_eq!(result.message, message);
    }

    #[test]
    fn test_from_row_value() {
        let row_value = RowValue {
            cell_values: vec![
                CellValue {
                    cell_type: Some(ColString { 0: "a".to_string() }),
                },
                CellValue {
                    cell_type: Some(ColString { 0: "b".to_string() }),
                },
            ],
        };
        let result = from_row_value(row_value);
        assert_eq!(
            result,
            vec![
                ValueType::ValString("a".to_string()),
                ValueType::ValString("b".to_string())
            ]
        );
    }

    #[test]
    fn test_from_value() {
        let value = CellValue {
            cell_type: Some(ColString { 0: "a".to_string() }),
        };
        let result = from_value(value);
        assert_eq!(result, ValueType::ValString("a".to_string()));
    }
}