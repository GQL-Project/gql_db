use crate::util::dbtype::*;
use crate::fileio::header::*;

/// The vector of column indices that make up the index
pub type ColsInIndex = Vec<u8>;
/// The vector of column types that make up the index key
pub type IndexKeyType = Vec<Column>;
/// The vector of column values that make up the index key
pub type IndexKey = Vec<Value>;
/// The page number that the key points to
pub type InternalIndexValue = u32;
/// The (pagenum, rownum) that the key points to
pub type LeafIndexValue = (u32, u16);
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
    }
}