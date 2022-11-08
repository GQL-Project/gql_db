use crate::fileio::pageio::*;
use crate::fileio::rowio::*;
use crate::util::dbtype::*;
use crate::fileio::header::*;
use crate::util::row::Row;

/// The vector of column indices that make up the index
pub type ColsInIndex = Vec<u8>;
/// The vector of column types that make up the index key
pub type IndexKeyType = Vec<Column>;
/// The vector of column values that make up the index key
pub type IndexKey = Vec<Value>;

/// The value of an index in an internal index page
#[derive(Debug, Clone)]
pub struct InternalIndexValue {
    /// The page number that the key points to
    pub pagenum: u32,
}

/// The (pagenum, rownum) that the key points to
#[derive(Debug, Clone)]
pub struct LeafIndexValue {
    /// The page number that the key points to
    pub pagenum: u32,
    /// The row number that the key points to
    pub rownum: u16,
}

/// The result of a key comparison operation
#[derive(Debug, PartialEq, Clone)]
pub enum KeyComparison {
    /// The first key is less than the second key
    Less,
    /// The keys are equal
    Equal,
    /// The first key is greater than the second key
    Greater,
    /// The two keys are incomparable
    Incomparable,
}


/// Maps the column names to the column indices in the schema to create an IndexKey.
/// Note: the col_names must be in order of the index. For example, if the index is
/// on (col1, col2), then col_names must be \["col1", "col2"\], NOT \["col2", "col1"\].
pub fn col_names_to_cols_in_index(
    col_names: &Vec<String>,
    schema: &Schema
) -> Result<ColsInIndex, String> {
    let mut index_key: ColsInIndex = Vec::new();
    'outer: for col_name in col_names {
        for (i, col) in schema.iter().enumerate() {
            if col.0 == *col_name {
                index_key.push(i as u8);
                continue 'outer;
            }
        }
        return Err(format!("Column {} not found in schema", col_name));
    }
    Ok(index_key)
}

/// Checks if two index key types are comparable
pub fn are_comparable_index_types(
    index1: &IndexKeyType,
    index2: &IndexKeyType
) -> bool {
    if index1.len() != index2.len() {
        return false;
    }
    for (i, col) in index1.iter().enumerate() {
        if col != &index2[i] {
            return false;
        }
    }
    true
}

/// Checks if two index keys are comparable
pub fn are_comparable_indexes(
    index1: &IndexKey,
    index2: &IndexKey
) -> bool {
    are_comparable_index_types(
        &get_index_key_type(&index1), 
        &get_index_key_type(&index2)
    )
}

/// Gets the index key type from the index key
pub fn get_index_key_type(
    index_key: &IndexKey
) -> IndexKeyType {
    index_key.iter().map(|val| val.get_coltype()).collect()
}

/// This compares two index keys and returns the result of the comparison.
pub fn compare_indexes(
    index1: &IndexKey,
    index2: &IndexKey
) -> KeyComparison {
    // Check that the keys are comparable
    if !are_comparable_indexes(&index1, &index2) {
        return KeyComparison::Incomparable;
    }

    // Compare the keys
    for (i, col) in index1.iter().enumerate() {
        match col.clone().cmp(&index2[i]) {
            std::cmp::Ordering::Less => return KeyComparison::Less,
            std::cmp::Ordering::Equal => continue,
            std::cmp::Ordering::Greater => return KeyComparison::Greater,
        }
    }
    KeyComparison::Equal
}

/// Writes an index key to a page at a specific offset
pub fn write_index_key_at_offset(index_key: &IndexKey, index_key_type: &IndexKeyType, page: &mut Page, offset: usize) -> Result<(), String> {
    // Convert the IndexKey to a Schema with empty column names
    let schema: Schema = index_key_type.iter().map(|col| ("".to_string(), col.clone())).collect();
    write_row_at_offset(&schema, page, index_key, offset)
}

/// Writes an internal index value to a page at a specific offset
pub fn write_internal_index_value_at_offset(index_value: &InternalIndexValue, page: &mut Page, offset: usize) -> Result<(), String> {
    // Convert the InternalIndexValue to a Schema with empty column names
    let schema: Schema = vec![
        ("pagenum".to_string(), Column::I32),
    ];
    let row: Row = vec![
        Value::I32(index_value.pagenum as i32),
    ];
    write_row_at_offset(&schema, page, &row, offset)
}

/// Writes a leaf index value to a page at a specific offset
pub fn write_leaf_index_value_at_offset(index_value: &LeafIndexValue, page: &mut Page, offset: usize) -> Result<(), String> {
    // Convert the InternalIndexValue to a Schema with empty column names
    let schema: Schema = vec![
        ("pagenum".to_string(), Column::I32),
        ("rownum".to_string(), Column::I32),
    ];
    let row: Row = vec![
        Value::I32(index_value.pagenum as i32),
        Value::I32(index_value.rownum as i32),
    ];
    write_row_at_offset(&schema, page, &row, offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::dbtype::Column;

    #[test]
    fn test_col_names_to_cols_in_index() {
        let schema: Schema = vec![
            ("col1".to_string(), Column::I32),
            ("col2".to_string(), Column::String(40)),
            ("col3".to_string(), Column::Float),
        ];
        let col_names: Vec<String> = vec!["col1".to_string(), "col3".to_string()];
        let index_key: ColsInIndex = col_names_to_cols_in_index(&col_names, &schema).unwrap();
        assert_eq!(index_key, vec![0, 2]);

        let col_names: Vec<String> = vec!["col3".to_string(), "col2".to_string(), "col1".to_string()];
        let index_key: ColsInIndex = col_names_to_cols_in_index(&col_names, &schema).unwrap();
        assert_eq!(index_key, vec![2, 1, 0]);

        let col_names: Vec<String> = vec!["col3".to_string(), "col4".to_string(), "col1".to_string()];
        assert_eq!(col_names_to_cols_in_index(&col_names, &schema).is_err(), true);
    }

    #[test]
    fn test_compare_indexes_less() {
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        assert_eq!(compare_indexes(&index1, &index2), KeyComparison::Less);

        let index3: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index4: IndexKey = vec![Value::I32(1), Value::String("b".to_string())];
        assert_eq!(compare_indexes(&index3, &index4), KeyComparison::Less);

        let index5: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index6: IndexKey = vec![Value::I32(2), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index5, &index6), KeyComparison::Less);

        let index7: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index8: IndexKey = vec![Value::I32(1), Value::String("az".to_string())];
        assert_eq!(compare_indexes(&index7, &index8), KeyComparison::Less);

        let index9: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        let index10: IndexKey = vec![Value::Bool(true), Value::Float(1.0124), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index9, &index10), KeyComparison::Less);

        let index11: IndexKey = vec![Value::Bool(false), Value::Float(1.0123), Value::String("a".to_string())];
        let index12: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index11, &index12), KeyComparison::Less);

        let index13: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:23".to_string()).unwrap())];
        let index14: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:24".to_string()).unwrap())];
        assert_eq!(compare_indexes(&index13, &index14), KeyComparison::Less);
    }

    #[test]
    fn test_compare_indexes_greater() {
        let index1: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index2: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index1, &index2), KeyComparison::Greater);

        let index3: IndexKey = vec![Value::I32(1), Value::String("b".to_string())];
        let index4: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index3, &index4), KeyComparison::Greater);

        let index5: IndexKey = vec![Value::I32(2), Value::String("a".to_string())];
        let index6: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index5, &index6), KeyComparison::Greater);

        let index7: IndexKey = vec![Value::I32(1), Value::String("az".to_string())];
        let index8: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index7, &index8), KeyComparison::Greater);

        let index9: IndexKey = vec![Value::Bool(true), Value::Float(1.0124), Value::String("a".to_string())];
        let index10: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index9, &index10), KeyComparison::Greater);

        let index11: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        let index12: IndexKey = vec![Value::Bool(false), Value::Float(1.0123), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index11, &index12), KeyComparison::Greater);

        let index13: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:24".to_string()).unwrap())];
        let index14: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:23".to_string()).unwrap())];
        assert_eq!(compare_indexes(&index13, &index14), KeyComparison::Greater);
    }

    #[test]
    fn test_compare_indexes_equal() {
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index1, &index2), KeyComparison::Equal);

        let index3: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        let index4: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index3, &index4), KeyComparison::Equal);

        let index5: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        let index6: IndexKey = vec![Value::Bool(true), Value::Float(1.0123), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index5, &index6), KeyComparison::Equal);

        let index7: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:23".to_string()).unwrap())];
        let index8: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:23".to_string()).unwrap())];
        assert_eq!(compare_indexes(&index7, &index8), KeyComparison::Equal);
    }

    #[test]
    fn test_incomparable_indexes() {
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::Bool(true), Value::String("b".to_string())];
        assert_eq!(compare_indexes(&index1, &index2), KeyComparison::Incomparable);

        let index3: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index4: IndexKey = vec![Value::I32(2), Value::Null];
        assert_eq!(compare_indexes(&index3, &index4), KeyComparison::Incomparable);

        let index5: IndexKey = vec![Value::Float(1.00123)];
        let index6: IndexKey = vec![Value::String("b".to_string())];
        assert_eq!(compare_indexes(&index5, &index6), KeyComparison::Incomparable);

        let index7: IndexKey = vec![Value::Double(1.123)];
        let index8: IndexKey = vec![Value::I64(123)];
        assert_eq!(compare_indexes(&index7, &index8), KeyComparison::Incomparable);

        let index9: IndexKey = vec![Value::Timestamp(parse_time(&"2020-01-23 12:00:23".to_string()).unwrap())];
        let index10: IndexKey = vec![Value::Double(123.002)];
        assert_eq!(compare_indexes(&index9, &index10), KeyComparison::Incomparable);
    }
}