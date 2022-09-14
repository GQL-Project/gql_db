/* Extensions to make conversions
to/from protobuf types easier */

pub fn to_connect_result(id: String) -> ConnectResult {
    ConnectResult { id }
}

pub fn to_query_result(column_names: Vec<String>, row_values: Vec<Vec<ValueType>>) -> QueryResult {
    QueryResult {
        column_names,
        row_values,
    }
}
