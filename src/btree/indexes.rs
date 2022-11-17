use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};
use std::cmp::Ordering;
use std::mem::size_of;

use crate::executor::predicate::resolve_reference;
use crate::executor::query::{ColumnAliases, IndexRefs};
use crate::fileio::header::*;
use crate::fileio::pageio::*;
use crate::util::dbtype::*;
use crate::util::row::{Row, RowLocation};

/// The vector of column indices that make up the index
/// For example, if the index is on columns 1, 3, and 4, then this would be [1, 3, 4]
pub type IndexID = Vec<u8>;
/// The vector of column types that make up the index key
pub type IndexKeyType = Vec<Column>;
/// The vector of column values that make up an individual index key
pub type IndexKey = Vec<Value>;

/// The value of an index in an internal index page
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct InternalIndexValue {
    /// The page number that the key points to
    pub pagenum: u32,
}

/// The (pagenum, rownum) that the key points to
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
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

impl InternalIndexValue {
    /// Gets the size of the index value in bytes when written to disk
    pub fn size() -> usize {
        size_of::<u32>()
    }
}

impl LeafIndexValue {
    /// Gets the size of the index value in bytes when written to disk
    pub fn size() -> usize {
        size_of::<u32>() + size_of::<u16>()
    }

    /// Returns the index value as a row location
    pub fn to_row_location(&self) -> RowLocation {
        RowLocation {
            pagenum: self.pagenum,
            rownum: self.rownum,
        }
    }
}

/*************************************************************************************************/
/*                                Get Methods                                                    */
/*************************************************************************************************/

/// get_index_key_type_size returns the size of the index key type in bytes
pub fn get_index_key_type_size(index_key_type: &IndexKeyType) -> usize {
    index_key_type.iter().map(|col| col.size() as usize).sum()
}

/// Gets the IndexKey from the row using the IndexID
pub fn get_index_key_from_row(row: &Row, index_id: &IndexID) -> IndexKey {
    index_id
        .iter()
        .map(|col| row[*col as usize].clone())
        .collect()
}

/// Gets the index key type from the index key
fn get_index_key_type(index_key: &IndexKey) -> IndexKeyType {
    index_key.iter().map(|val| val.get_coltype()).collect()
}

/// Gets a default index key value from the index key type
pub fn get_default_index_key(index_key_type: &IndexKeyType) -> IndexKey {
    index_key_type
        .iter()
        .map(|col| col.get_default_value())
        .collect()
}

/// Gets the index id that corresponds to the given expression.
pub fn get_index_id_from_expr(
    expr: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
    table_name: &String
) -> Result<IndexID, String> {
    match expr {
        Expr::Identifier(x) => {
            let x: String = resolve_reference(x.value.to_string(), column_aliases)?;
            let index: usize = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;

            let split: Vec<String> = x.split(".").map(|s| s.to_string()).collect::<Vec<String>>();
            if split.len() > 1 && split[0] != *table_name {
                return Ok(vec![])
            }
            
            Ok(vec![index as u8])
        }
        Expr::CompoundIdentifier(list) => {
            // Join all the identifiers in the list with a dot, perform the same step as above
            let x: String = resolve_reference(
                list.iter()
                    .map(|x| x.value.to_string())
                    .collect::<Vec<String>>()
                    .join("."),
                column_aliases,
            )?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;

            let split: Vec<String> = x.split(".").map(|s| s.to_string()).collect::<Vec<String>>();
            if split.len() > 1 && split[0] != *table_name {
                return Ok(vec![])
            }

            Ok(vec![index as u8])
        }
        Expr::Value(_) => {
            // Discard values
            Ok(Vec::new())
        }
        Expr::IsFalse(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::IsNotFalse(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::IsTrue(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::IsNotTrue(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::IsNull(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::IsNotNull(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        Expr::BinaryOp { left, op, right } => match op {
            // Resolve values from the two sides of the expression, and then perform
            // the comparison on the two values
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Divide
            | BinaryOperator::Multiply
            | BinaryOperator::Modulo => {
                let left_index_id: IndexID =
                    get_index_id_from_expr(left.as_ref(), column_aliases, index_refs, table_name)?;
                let right_index_id: IndexID =
                    get_index_id_from_expr(right.as_ref(), column_aliases, index_refs, table_name)?;

                let mut combined_index_id: IndexID = left_index_id;
                for right_id in right_index_id {
                    if !combined_index_id.contains(&right_id) {
                        combined_index_id.push(right_id);
                    }
                }

                Ok(combined_index_id)
            }
            _ => Err(format!("Unsupported binary operator for Predicate: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not | UnaryOperator::Plus | UnaryOperator::Minus => Ok(
                get_index_id_from_expr(expr.as_ref(), column_aliases, index_refs, table_name)?,
            ),
            _ => Err(format!("Unsupported unary operator for Predicate: {}", op)),
        },
        Expr::Nested(pred) => Ok(get_index_id_from_expr(
            pred.as_ref(),
            column_aliases,
            index_refs,
            table_name
        )?),
        _ => Err(format!("Invalid Predicate Clause: {}", expr)),
    }
}

/*************************************************************************************************/
/*                                Conversion Methods                                             */
/*************************************************************************************************/

/// Maps the column names to the column indices in the schema to create an IndexKey.
/// Note: the col_names must be in order of the index. For example, if the index is
/// on (col1, col2), then col_names must be \["col1", "col2"\], NOT \["col2", "col1"\].
pub fn create_index_id(col_names: &Vec<String>, schema: &Schema) -> Result<IndexID, String> {
    let mut index_key: IndexID = Vec::new();
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

/// Maps the IndexID to the column types in the schema to create an IndexKeyType.
/// Note: the cols_in_index must be in order of the index. For example, if the index is
/// on (col1, col2), then cols_in_index must be \[0, 1\], NOT \[1, 0\].
pub fn cols_id_to_index_key_type(cols_in_index: &IndexID, schema: &Schema) -> IndexKeyType {
    let mut index_key_type: IndexKeyType = Vec::new();
    for col_idx in cols_in_index {
        index_key_type.push(schema[*col_idx as usize].1.clone());
    }
    index_key_type
}

/*************************************************************************************************/
/*                                Comparison Methods                                             */
/*************************************************************************************************/

/// This compares two index keys and returns the result of the comparison.
pub fn compare_indexes(index1: &IndexKey, index2: &IndexKey) -> KeyComparison {
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

/// This compares two rows using only the columns specified in the index id.
pub fn compare_rows_using_index_id(row1: &Row, row2: &Row, index_id: &IndexID) -> Ordering {
    for col_idx in index_id {
        match row1[*col_idx as usize].cmp(&row2[*col_idx as usize]) {
            std::cmp::Ordering::Less => return Ordering::Less,
            std::cmp::Ordering::Equal => continue,
            std::cmp::Ordering::Greater => return Ordering::Greater,
        }
    }
    Ordering::Equal
}

/// Checks if two index key types are comparable
pub fn are_comparable_index_types(index1: &IndexKeyType, index2: &IndexKeyType) -> bool {
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
pub fn are_comparable_indexes(index1: &IndexKey, index2: &IndexKey) -> bool {
    are_comparable_index_types(&get_index_key_type(&index1), &get_index_key_type(&index2))
}

/*************************************************************************************************/
/*                                Write Methods                                                  */
/*************************************************************************************************/

/// Writes an index key to a page at a specific offset
pub fn write_index_key_at_offset(
    index_key: &IndexKey,
    index_key_type: &IndexKeyType,
    page: &mut Page,
    offset: usize,
) -> Result<(), String> {
    let mut temp_offset: usize = offset;
    // Write the index key to the page
    for (col, val) in index_key_type.clone().iter().zip(index_key) {
        col.write(val, page, temp_offset)?;
        temp_offset += col.size();
    }
    Ok(())
}

/// Writes an internal index value to a page at a specific offset
pub fn write_internal_index_value_at_offset(
    index_value: &InternalIndexValue,
    page: &mut Page,
    offset: usize,
) -> Result<(), String> {
    write_type::<u32>(page, offset, index_value.pagenum)
}

/// Writes a leaf index value to a page at a specific offset
pub fn write_leaf_index_value_at_offset(
    index_value: &LeafIndexValue,
    page: &mut Page,
    offset: usize,
) -> Result<(), String> {
    write_type::<u32>(page, offset, index_value.pagenum)?;
    write_type::<u16>(page, offset + size_of::<u32>(), index_value.rownum)?;
    Ok(())
}

/*************************************************************************************************/
/*                                Read Methods                                                   */
/*************************************************************************************************/

/// Reads an index key from a page at a specific offset
pub fn read_index_key_at_offset(
    index_key_type: &IndexKeyType,
    page: &Page,
    offset: usize,
) -> Result<IndexKey, String> {
    let mut temp_offset: usize = offset;
    let mut index_key: IndexKey = Vec::new();
    // Read the index key from the page
    for col in index_key_type {
        index_key.push(col.read(page, temp_offset)?);
        temp_offset += col.size();
    }
    Ok(index_key)
}

/// Reads an internal index value from a page at a specific offset
pub fn read_internal_index_value_at_offset(
    page: &Page,
    offset: usize,
) -> Result<InternalIndexValue, String> {
    let pagenum: u32 = read_type::<u32>(page, offset)?;
    Ok(InternalIndexValue { pagenum })
}

/// Reads a leaf index value from a page at a specific offset
pub fn read_leaf_index_value_at_offset(
    page: &Page,
    offset: usize,
) -> Result<LeafIndexValue, String> {
    let pagenum: u32 = read_type::<u32>(page, offset)?;
    let rownum: u16 = read_type::<u16>(page, offset + size_of::<u32>())?;
    Ok(LeafIndexValue { pagenum, rownum })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;
    use crate::{executor::query::*, util::dbtype::Column};

    #[test]
    fn test_col_names_to_cols_in_index() {
        let schema: Schema = vec![
            ("col1".to_string(), Column::I32),
            ("col2".to_string(), Column::String(40)),
            ("col3".to_string(), Column::Float),
        ];
        let col_names: Vec<String> = vec!["col1".to_string(), "col3".to_string()];
        let index_key: IndexID = create_index_id(&col_names, &schema).unwrap();
        assert_eq!(index_key, vec![0, 2]);

        let col_names: Vec<String> =
            vec!["col3".to_string(), "col2".to_string(), "col1".to_string()];
        let index_key: IndexID = create_index_id(&col_names, &schema).unwrap();
        assert_eq!(index_key, vec![2, 1, 0]);

        let col_names: Vec<String> =
            vec!["col3".to_string(), "col4".to_string(), "col1".to_string()];
        assert_eq!(create_index_id(&col_names, &schema).is_err(), true);
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

        let index9: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        let index10: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0124),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index9, &index10), KeyComparison::Less);

        let index11: IndexKey = vec![
            Value::Bool(false),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        let index12: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index11, &index12), KeyComparison::Less);

        let index13: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:23".to_string()).unwrap(),
        )];
        let index14: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:24".to_string()).unwrap(),
        )];
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

        let index9: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0124),
            Value::String("a".to_string()),
        ];
        let index10: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index9, &index10), KeyComparison::Greater);

        let index11: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        let index12: IndexKey = vec![
            Value::Bool(false),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index11, &index12), KeyComparison::Greater);

        let index13: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:24".to_string()).unwrap(),
        )];
        let index14: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:23".to_string()).unwrap(),
        )];
        assert_eq!(compare_indexes(&index13, &index14), KeyComparison::Greater);
    }

    #[test]
    fn test_compare_indexes_equal() {
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        assert_eq!(compare_indexes(&index1, &index2), KeyComparison::Equal);

        let index3: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        let index4: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index3, &index4), KeyComparison::Equal);

        let index5: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        let index6: IndexKey = vec![
            Value::Bool(true),
            Value::Float(1.0123),
            Value::String("a".to_string()),
        ];
        assert_eq!(compare_indexes(&index5, &index6), KeyComparison::Equal);

        let index7: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:23".to_string()).unwrap(),
        )];
        let index8: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:23".to_string()).unwrap(),
        )];
        assert_eq!(compare_indexes(&index7, &index8), KeyComparison::Equal);
    }

    #[test]
    fn test_incomparable_indexes() {
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::Bool(true), Value::String("b".to_string())];
        assert_eq!(
            compare_indexes(&index1, &index2),
            KeyComparison::Incomparable
        );

        let index3: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index4: IndexKey = vec![Value::I32(2), Value::Null(Column::I32)];
        assert_eq!(
            compare_indexes(&index3, &index4),
            KeyComparison::Incomparable
        );

        let index5: IndexKey = vec![Value::Float(1.00123)];
        let index6: IndexKey = vec![Value::String("b".to_string())];
        assert_eq!(
            compare_indexes(&index5, &index6),
            KeyComparison::Incomparable
        );

        let index7: IndexKey = vec![Value::Double(1.123)];
        let index8: IndexKey = vec![Value::I64(123)];
        assert_eq!(
            compare_indexes(&index7, &index8),
            KeyComparison::Incomparable
        );

        let index9: IndexKey = vec![Value::Timestamp(
            parse_time(&"2020-01-23 12:00:23".to_string()).unwrap(),
        )];
        let index10: IndexKey = vec![Value::Double(123.002)];
        assert_eq!(
            compare_indexes(&index9, &index10),
            KeyComparison::Incomparable
        );
    }

    #[test]
    fn test_read_write_index_key() {
        let mut page: Page = [0; PAGE_SIZE];

        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(20)];
        let index_key_size: usize = get_index_key_type_size(&index_key_type);
        let index1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index2: IndexKey = vec![Value::I32(1), Value::String("b".to_string())];
        let index3: IndexKey = vec![Value::I32(1), Value::String("c".to_string())];
        let index4: IndexKey = vec![Value::I32(2), Value::String("a".to_string())];
        let index5: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index6: IndexKey = vec![Value::I32(2), Value::String("c".to_string())];

        // Write index1 at offset 0
        write_index_key_at_offset(&index1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 0).unwrap(),
            index1
        );

        // Write index2 at offset index_key_size
        write_index_key_at_offset(&index2, &index_key_type, &mut page, index_key_size).unwrap();
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, index_key_size).unwrap(),
            index2
        );

        // Write index3 at offset 2 * index_key_size
        write_index_key_at_offset(&index3, &index_key_type, &mut page, 2 * index_key_size).unwrap();
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 2 * index_key_size).unwrap(),
            index3
        );

        // Write index4, index5, and index6 at offset 3 * index_key_size, 4 * index_key_size, and 5 * index_key_size respectively
        write_index_key_at_offset(&index4, &index_key_type, &mut page, 3 * index_key_size).unwrap();
        write_index_key_at_offset(&index5, &index_key_type, &mut page, 4 * index_key_size).unwrap();
        write_index_key_at_offset(&index6, &index_key_type, &mut page, 5 * index_key_size).unwrap();
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 3 * index_key_size).unwrap(),
            index4
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 4 * index_key_size).unwrap(),
            index5
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 5 * index_key_size).unwrap(),
            index6
        );

        // Write index6 at offset 0 (overwrite index1)
        write_index_key_at_offset(&index6, &index_key_type, &mut page, 0).unwrap();

        // Check all values are written correctly
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 0).unwrap(),
            index6
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, index_key_size).unwrap(),
            index2
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 2 * index_key_size).unwrap(),
            index3
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 3 * index_key_size).unwrap(),
            index4
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 4 * index_key_size).unwrap(),
            index5
        );
        assert_eq!(
            read_index_key_at_offset(&index_key_type, &page, 5 * index_key_size).unwrap(),
            index6
        );
    }

    #[test]
    fn test_read_write_internal_index_value() {
        let mut page: Page = [0; PAGE_SIZE];

        let internal_index_value1: InternalIndexValue = InternalIndexValue { pagenum: 1 };
        let internal_index_value2: InternalIndexValue = InternalIndexValue { pagenum: 2 };
        let internal_index_value3: InternalIndexValue = InternalIndexValue { pagenum: 3 };

        // Write internal_index_value1 at offset 0
        write_internal_index_value_at_offset(&internal_index_value1, &mut page, 0).unwrap();
        assert_eq!(
            read_internal_index_value_at_offset(&page, 0).unwrap(),
            internal_index_value1
        );

        // Write internal_index_value2 at offset InternalIndexValue::size()
        write_internal_index_value_at_offset(
            &internal_index_value2,
            &mut page,
            InternalIndexValue::size(),
        )
        .unwrap();
        assert_eq!(
            read_internal_index_value_at_offset(&page, InternalIndexValue::size()).unwrap(),
            internal_index_value2
        );

        // Write internal_index_value3 at offset 2 * InternalIndexValue::size()
        write_internal_index_value_at_offset(
            &internal_index_value3,
            &mut page,
            2 * InternalIndexValue::size(),
        )
        .unwrap();
        assert_eq!(
            read_internal_index_value_at_offset(&page, 2 * InternalIndexValue::size()).unwrap(),
            internal_index_value3
        );

        // Write internal_index_value3 at offset 0 (overwrite internal_index_value1)
        write_internal_index_value_at_offset(&internal_index_value3, &mut page, 0).unwrap();

        // Check all values are written correctly
        assert_eq!(
            read_internal_index_value_at_offset(&page, 0).unwrap(),
            internal_index_value3
        );
        assert_eq!(
            read_internal_index_value_at_offset(&page, InternalIndexValue::size()).unwrap(),
            internal_index_value2
        );
        assert_eq!(
            read_internal_index_value_at_offset(&page, 2 * InternalIndexValue::size()).unwrap(),
            internal_index_value3
        );
    }

    #[test]
    fn test_read_write_leaf_index_value() {
        let mut page: Page = [0; PAGE_SIZE];

        let leaf_index_value1: LeafIndexValue = LeafIndexValue {
            pagenum: 1,
            rownum: 5,
        };
        let leaf_index_value2: LeafIndexValue = LeafIndexValue {
            pagenum: 2,
            rownum: 49,
        };
        let leaf_index_value3: LeafIndexValue = LeafIndexValue {
            pagenum: 3,
            rownum: 74,
        };

        // Write leaf_index_value1 at offset 0
        write_leaf_index_value_at_offset(&leaf_index_value1, &mut page, 0).unwrap();
        assert_eq!(
            read_leaf_index_value_at_offset(&page, 0).unwrap(),
            leaf_index_value1
        );

        // Write leaf_index_value2 at offset LeafIndexValue::size()
        write_leaf_index_value_at_offset(&leaf_index_value2, &mut page, LeafIndexValue::size())
            .unwrap();
        assert_eq!(
            read_leaf_index_value_at_offset(&page, LeafIndexValue::size()).unwrap(),
            leaf_index_value2
        );

        // Write leaf_index_value3 at offset 2 * LeafIndexValue::size()
        write_leaf_index_value_at_offset(&leaf_index_value3, &mut page, 2 * LeafIndexValue::size())
            .unwrap();
        assert_eq!(
            read_leaf_index_value_at_offset(&page, 2 * LeafIndexValue::size()).unwrap(),
            leaf_index_value3
        );

        // Write leaf_index_value3 at offset 0 (overwrite leaf_index_value1)
        write_leaf_index_value_at_offset(&leaf_index_value3, &mut page, 0).unwrap();

        // Check all values are written correctly
        assert_eq!(
            read_leaf_index_value_at_offset(&page, 0).unwrap(),
            leaf_index_value3
        );
        assert_eq!(
            read_leaf_index_value_at_offset(&page, LeafIndexValue::size()).unwrap(),
            leaf_index_value2
        );
        assert_eq!(
            read_leaf_index_value_at_offset(&page, 2 * LeafIndexValue::size()).unwrap(),
            leaf_index_value3
        );
    }

    #[test]
    fn test_get_index_id_from_expr() {
        let table_name: String = "test_table".to_string();
        let schema: Schema = vec![
            (String::from("id"), Column::I32),
            (String::from("name"), Column::String(20)),
        ];
        let column_aliases: ColumnAliases =
            gen_column_aliases_from_schema(&vec![(schema, table_name.clone())]);
        let index_refs: IndexRefs = get_index_refs(&column_aliases);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "1".to_string(),
                        true,
                    ))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "name".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "d".to_string(),
                        true,
                    ))),
                }),
            },
            &column_aliases,
            &index_refs,
            &table_name
        )
        .unwrap();
        assert_eq!(index_id, vec![0, 1]);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "name".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "d".to_string(),
                    true,
                ))),
            },
            &column_aliases,
            &index_refs,
            &table_name
        )
        .unwrap();
        assert_eq!(index_id, vec![1]);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "test_table.name".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "d".to_string(),
                    true,
                ))),
            },
            &column_aliases,
            &index_refs,
            &table_name
        )
        .unwrap();
        assert_eq!(index_id, vec![1]);

        assert_eq!(
            get_index_id_from_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "table2.name".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "d".to_string(),
                        true,
                    ))),
                },
                &column_aliases,
                &index_refs,
                &table_name
            )
            .is_err(),
            true
        );
    }

    #[test]
    fn test_get_index_id_from_expr2() {
        let table1_name: String = "test_table1".to_string();
        let table2_name: String = "test_table2".to_string();
        let schema: Schema = vec![
            (String::from("id"), Column::I32),
            (String::from("name"), Column::String(20)),
        ];
        let column_aliases: ColumnAliases =
            gen_column_aliases_from_schema(&vec![
                (schema.clone(), table1_name.clone()),
                (schema.clone(), table2_name.clone()),
                ]
            );
        let index_refs: IndexRefs = get_index_refs(&column_aliases);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table1.id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "1".to_string(),
                        true,
                    ))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table2.name".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "d".to_string(),
                        true,
                    ))),
                }),
            },
            &column_aliases,
            &index_refs,
            &table1_name
        )
        .unwrap();
        assert_eq!(index_id, vec![0]);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table1.id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "1".to_string(),
                        true,
                    ))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table2.name".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "d".to_string(),
                        true,
                    ))),
                }),
            },
            &column_aliases,
            &index_refs,
            &table2_name
        )
        .unwrap();
        assert_eq!(index_id, vec![3]);

        let index_id: IndexID = get_index_id_from_expr(
            &Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table2.id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "1".to_string(),
                        true,
                    ))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "test_table2.name".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        "d".to_string(),
                        true,
                    ))),
                }),
            },
            &column_aliases,
            &index_refs,
            &table2_name
        )
        .unwrap();
        assert_eq!(index_id, vec![2, 3]);
    }
}
