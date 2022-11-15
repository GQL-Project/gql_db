use std::mem::size_of;
use itertools::Itertools;
use sqlparser::ast::Expr;

use super::indexes::*;
use crate::{fileio::pageio::*, util::row::*, executor::query::*, executor::predicate::*};

const LEAF_PAGE_HEADER_SIZE: usize = size_of::<u16>();

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | NumValues<u16> | IndexValue | IndexKey | IndexValue | ... | IndexKey |
/// where there is an equal number of index values and index keys.
#[derive(Debug, Clone)]
pub struct LeafIndexPage {
    table_path: String,                       // The path to the table this page belongs to
    pagenum: u32,                             // The page number of this page
    indexes: Vec<(IndexKey, LeafIndexValue)>, // The values in this page
    index_key_type: IndexKeyType,             // The type of the index keys
    index_id: IndexID,                        // The columns used in this index
    key_size: u8,                             // The size of an individual member of index_keys
    page: Page,                               // The page data
}

impl LeafIndexPage {
    /// Creates a new leaf index page.
    pub fn new(
        table_path: String,
        pagenum: u32,
        index_id: &IndexID,
        index_key_type: &IndexKeyType,
    ) -> Result<Self, String> {
        let mut page: Page = [0; PAGE_SIZE];
        // write the number of values to the page
        write_type(&mut page, 0, 0u16)?;

        // Get the size of an individual key
        let key_size: u8 = get_index_key_type_size(&index_key_type) as u8;

        Ok(LeafIndexPage {
            table_path,
            pagenum,
            indexes: Vec::new(),
            index_key_type: index_key_type.clone(),
            index_id: index_id.clone(),
            key_size,
            page,
        })
    }

    /// Loads a leaf index page from disk at the specified page number.
    pub fn load_from_table(
        table_path: String,
        pagenum: u32,
        index_id: &IndexID,
        index_key_type: &IndexKeyType,
    ) -> Result<Self, String> {
        let (page, page_type) = read_page(pagenum, &table_path)?;
        if page_type != PageType::LeafIndex {
            return Err(format!(
                "Error page {} is not a leaf index page in {}",
                pagenum, table_path
            ));
        }

        // Get the size of an individual key
        let key_size: u8 = get_index_key_type_size(&index_key_type) as u8;

        // Read the number of values from the page
        let num_values: u16 = read_type::<u16>(&page, 0)?;

        // Read the values from the page
        let mut indexes: Vec<(IndexKey, LeafIndexValue)> = Vec::new();
        for i in 0..num_values {
            let index_key: IndexKey = Self::read_index_key(&index_key_type, &page, i as usize)?;
            let index_value: LeafIndexValue = Self::read_index_value(&index_key_type, &page, i as usize)?;
            indexes.push((index_key, index_value));
        }

        Ok(LeafIndexPage {
            table_path,
            pagenum,
            indexes,
            index_key_type: index_key_type.clone(),
            index_id: index_id.clone(),
            key_size,
            page: *page,
        })
    }

    /// Writes the page to disk at the specified page number.
    pub fn write_page(
        &mut self
    ) -> Result<(), String> {
        // Write the number of values to the page
        write_type::<u16>(&mut self.page, 0, self.indexes.len() as u16)?;
        // Write the keys into the page
        for (i, (key, value)) in self.indexes.clone().iter().enumerate().sorted() {
            Self::write_index_key(&key, &self.index_key_type, &mut self.page, i)?;
            Self::write_index_value(&value, &self.index_key_type, &mut self.page, i)?;
        }
        write_page(self.pagenum, &self.table_path, &self.page, PageType::LeafIndex)?;
        Ok(())
    }

    /// Gets the page number of the page where this page is stored
    pub fn get_pagenum(&self) -> u32 {
        self.pagenum
    }

    pub fn get_largest_index_key(&self) -> Option<IndexKey> {
        if self.indexes.len() == 0 {
            return None;
        }
        Some(self.indexes[self.indexes.len() - 1].0.clone())
    }

    /// Inserts a row into the page.
    /// Returns whether row was inserted or whether the page is full.
    pub fn add_pointer_to_row(
        &mut self,
        rowinfo: &RowInfo
    ) -> Result<bool, String> {
        if !self.has_room() {
            return Ok(false);
        }

        // Get the IndexKey from the row info
        let index_key: IndexKey = get_index_key_from_row(&rowinfo.row, &self.index_id);
        let index_value: LeafIndexValue = LeafIndexValue {
            pagenum: rowinfo.pagenum,
            rownum: rowinfo.rownum,
        };

        self.add_pointer_to_leaf_value(&index_key, index_value)
    }
    
    /// Inserts a new leaf index value into the page with the specified index key.
    /// Returns whether row was inserted or whether the page is full.
    pub fn add_pointer_to_leaf_value(
        &mut self,
        index_key: &IndexKey,
        leaf_value: LeafIndexValue
    ) -> Result<bool, String> {
        // Find the index where we need to insert the key. Locate the first index where the key is greater than the index_key we're inserting.
        let mut did_insert: bool = false;
        for (i, (key, _)) in self.indexes.clone().iter().enumerate().sorted() {
            if compare_indexes(&key, &index_key) == KeyComparison::Greater {
                self.indexes.insert(i, (index_key.clone(), leaf_value.clone()));
                did_insert = true;
                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if !did_insert {
            // Insert the key into the vector
            self.indexes.push((index_key.clone(), leaf_value));
        }

        Ok(true)
    }

    /// Removes a row from the page.
    pub fn remove_pointer_to_row(
        &mut self,
        rowinfo: &RowInfo
    ) -> Result<(), String> {
        if self.is_empty() {
            return Err(format!("Error: leaf index page {} is empty", self.pagenum));
        }

        // Remove the rows from the indexes
        self.indexes.retain(|(_, val)| {
            if val.pagenum == rowinfo.pagenum && val.rownum == rowinfo.rownum {
                return false;
            }
            true
        });

        Ok(())
    }

    /// Gets the row info for the rows that match the given expression.
    pub fn get_row_locations_using_pred_solver(
        &self,
        pred_solver: &PredicateSolver
    ) -> Result<Vec<RowLocation>, String> {
        let mut row_locations: Vec<RowLocation> = Vec::new();
        for (index_key, index_value) in &self.indexes {
            if pred_solver(index_key)? {
                row_locations.push(RowLocation {
                    pagenum: index_value.pagenum,
                    rownum: index_value.rownum,
                });
            }
        }
        
        Ok(row_locations)
    }

    /// Gets the rows that match the given index key.
    /// Returns a vector of RowLocations.
    pub fn get_row_locations_from_key(
        &self,
        index_key: &IndexKey
    ) -> Result<Vec<RowLocation>, String> {
        // Check if the index key is present
        if self.indexes
            .iter()
            .map(|(key, _)| key)
            .find(|key| 
                compare_indexes(key, index_key) == KeyComparison::Equal
            )
            .is_some() {

            // At least one key is present, so get the rows that match the key(s)
            let mut rows: Vec<RowLocation> = Vec::new();
            for (key, value) in self.indexes.clone() {
                if compare_indexes(&key, index_key) == KeyComparison::Equal {
                    rows.push(RowLocation {
                        pagenum: value.pagenum,
                        rownum: value.rownum
                    });
                }
            }

            return Ok(rows);
        }
        // The index is not present in the dictionary
        else {
            return Ok(Vec::new());
        }
    }

    /// Gets the lowest valued index key in the page.
    pub fn get_lowest_index_key(
        &self
    ) -> Option<IndexKey> {
        for (key, _) in self.indexes.clone().iter().sorted() {
            return Some(key.clone());
        }
        None
    }

    /// Gets all the index key values that are in the page
    pub fn get_all_key_values(
        &self
    ) -> Vec<(IndexKey, LeafIndexValue)> {
        self.indexes.clone()
    }

    /// Returns true if there is room for another index and value in this page.
    pub fn has_room(
        &self
    ) -> bool {
        let all_keys_size: usize = self.indexes.len() * self.key_size as usize;
        let all_values_size: usize = self.indexes.len() * LeafIndexValue::size();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + LeafIndexValue::size() <= (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) {
            return true;
        }
        false
    }

    /// Returns true if this leaf page has no key-value pairs.
    pub fn is_empty(
        &self
    ) -> bool {
        self.indexes.len() == 0
    }

    /***********************************************************************************************/
    /*                                       Public Static Methods                                 */
    /***********************************************************************************************/

    /// Gets the maximum number of index value pointers that can fit on a page.
    /// i.e. the number of data rows that a leaf index page can point to.
    pub fn get_max_index_pointers_per_page(
        index_key_type: &IndexKeyType
    ) -> usize {
        let idx_and_value_size: usize = get_index_key_type_size(index_key_type) +
                                        LeafIndexValue::size();
        (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / idx_and_value_size                       
    }

    /// Converts rowinfos into leaf index key and value pairs.
    /// It does NOT sort them.
    pub fn convert_to_key_vals(
        rowinfos: Vec<RowInfo>,
        index_id: &IndexID,
    ) -> Result<Vec<(IndexKey, LeafIndexValue)>, String> {
        let mut key_vals: Vec<(IndexKey, LeafIndexValue)> = Vec::new();
        for rowinfo in rowinfos {
            let index_key: IndexKey = get_index_key_from_row(&rowinfo.row, &index_id);
            let index_value: LeafIndexValue = LeafIndexValue {
                pagenum: rowinfo.pagenum,
                rownum: rowinfo.rownum,
            };
            key_vals.push((index_key, index_value));
        }
        Ok(key_vals)
    }

    /***********************************************************************************************/
    /*                                       Private Member Methods                                */
    /***********************************************************************************************/

    /// Gets the row info for the rows that match the given expression.
    fn get_row_locations_matching_expr(
        &self,
        pred: &Expr,
        column_aliases: &ColumnAliases,
        index_refs: &IndexRefs,
    ) -> Result<Vec<RowLocation>, String> {
        let x: PredicateSolver = solve_predicate(pred, column_aliases, index_refs)?;
        Ok(self.get_row_locations_using_pred_solver(&x)?)
    }

    /***********************************************************************************************/
    /*                                       Private Static Methods                                */
    /***********************************************************************************************/

    /// Writes an index key to a page at a specific offset
    fn write_index_key(
        index_key: &IndexKey,
        index_key_type: &IndexKeyType,
        page: &mut Page, 
        index_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            ((index_idx + 1) * LeafIndexValue::size()) +            // The offset of the index values
                            LEAF_PAGE_HEADER_SIZE;                                  // The offset of the page header
        write_index_key_at_offset(index_key, index_key_type, page, offset)
    }

    /// Writes an index key to a page at a specific offset
    fn write_index_value(
        index_value: &LeafIndexValue, 
        index_key_type: &IndexKeyType,
        page: &mut Page, 
        value_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * LeafIndexValue::size()) +                  // The offset of the index values
                            LEAF_PAGE_HEADER_SIZE;                                  // The offset of the page header
        write_leaf_index_value_at_offset(index_value, page, offset)
    }

    /// Reads an index key from a page at a specific index key's index
    /// Note: This is the index key's index.
    /// i.e. the first index key is index_idx=0, the second index key is index_idx=1, etc.
    fn read_index_key(
        index_key_type: &IndexKeyType,
        page: &Page, 
        index_idx: usize
    ) -> Result<IndexKey, String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            ((index_idx + 1) * LeafIndexValue::size()) +            // The offset of the index values
                            LEAF_PAGE_HEADER_SIZE;                                  // The offset of the page header
        read_index_key_at_offset(index_key_type, page, offset)
    }

    /// Reads an index value from a page at a specific index value's index
    /// Note: This is the index value's index, not the index key's index.
    /// i.e. the first index value is index value_idx=0, the second index value is index value_idx=1, etc.
    fn read_index_value(
        index_key_type: &IndexKeyType,
        page: &Page, 
        value_idx: usize
    ) -> Result<LeafIndexValue, String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * LeafIndexValue::size()) +                  // The offset of the index values
                            LEAF_PAGE_HEADER_SIZE;                                  // The offset of the page header
        read_leaf_index_value_at_offset(page, offset)
    }
}


#[cfg(test)]
mod tests {
    use serial_test::serial;
    use sqlparser::ast::{Ident, BinaryOperator};

    use super::*;
    use crate::{util::dbtype::*, fileio::{tableio::*, header::*}};

    #[test]
    fn test_read_write_index_key() {
        let mut page: Page = [0; PAGE_SIZE];

        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(20)];

        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index_key3: IndexKey = vec![Value::I32(3), Value::String("c".to_string())];

        // Write index_key1 to the page at index_idx=0
        LeafIndexPage::write_index_key(&index_key1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );

        // Write index_key2 to the page at index_idx=1
        LeafIndexPage::write_index_key(&index_key2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );

        // Write index_key3 to the page at index_idx=2
        LeafIndexPage::write_index_key(&index_key3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );

        // Read the index keys from the page
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
    }

    #[test]
    fn test_read_write_index_value() {
        let mut page: Page = [0; PAGE_SIZE];

        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(20)];

        let index_value1: LeafIndexValue = LeafIndexValue { pagenum: 4, rownum: 182 };
        let index_value2: LeafIndexValue = LeafIndexValue { pagenum: 50, rownum: 6 };
        let index_value3: LeafIndexValue = LeafIndexValue { pagenum: 235, rownum: 67 };

        // Write index_value1 to the page at index_idx=0
        LeafIndexPage::write_index_value(&index_value1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );

        // Write index_value2 to the page at index_idx=1
        LeafIndexPage::write_index_value(&index_value2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );

        // Write index_value3 to the page at index_idx=2
        LeafIndexPage::write_index_value(&index_value3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );

        // Read the index values from the page
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );
    }

    #[test]
    fn test_read_write_index_key_values() {
        let mut page: Page = [0; PAGE_SIZE];

        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(20)];

        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index_key3: IndexKey = vec![Value::I32(3), Value::String("c".to_string())];

        let index_value1: LeafIndexValue = LeafIndexValue { pagenum: 3, rownum: 0 };
        let index_value2: LeafIndexValue = LeafIndexValue { pagenum: 4, rownum: 123 };
        let index_value3: LeafIndexValue = LeafIndexValue { pagenum: 50, rownum: 89 };
        let index_value4: LeafIndexValue = LeafIndexValue { pagenum: 235, rownum: 201 };

        // Write index_key1 and index_value1 to the page at index_idx=0
        LeafIndexPage::write_index_key(&index_key1, &index_key_type, &mut page, 0).unwrap();
        LeafIndexPage::write_index_value(&index_value1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );

        // Write index_key2 and index_value2 to the page at index_idx=1
        LeafIndexPage::write_index_key(&index_key2, &index_key_type, &mut page, 1).unwrap();
        LeafIndexPage::write_index_value(&index_value2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );

        // Write index_key3 and index_value3 to the page at index_idx=2
        LeafIndexPage::write_index_key(&index_key3, &index_key_type, &mut page, 2).unwrap();
        LeafIndexPage::write_index_value(&index_value3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );

        // Write index_value4 to the page at index_idx=3
        LeafIndexPage::write_index_value(&index_value4, &index_key_type, &mut page, 3).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 3).unwrap(),
            index_value4
        );

        // Overwrite index_value2 with index_value4
        LeafIndexPage::write_index_value(&index_value4, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value4
        );

        // Read the index keys and values from the page
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value4
        );
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &page, 3).unwrap(),
            index_value4
        );
    }

    #[test]
    #[serial]
    fn test_leaf_load_from_table() {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("test_leaf_load_from_table");
        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(10)];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10))
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let leaf_page_num: u32 = 2;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap().0;
        write_page(leaf_page_num, &table.path, &[0; PAGE_SIZE], PageType::LeafIndex).unwrap();
        
        // Create the leaf page
        let mut leaf_page: LeafIndexPage = LeafIndexPage::new(
            table.path.clone(),
            leaf_page_num,
            &index_id,
            &index_key_type
        ).unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];

        let index_value1: LeafIndexValue = LeafIndexValue { pagenum: 3, rownum: 0 };
        let index_value2: LeafIndexValue = LeafIndexValue { pagenum: 4, rownum: 123 };
        let index_value3: LeafIndexValue = LeafIndexValue { pagenum: 219, rownum: 89 };

        // Write the index keys and values to the page structure
        LeafIndexPage::write_index_key(&index_key1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_key(&index_key2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_value(&index_value1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_value(&index_value2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_value(&index_value3, &index_key_type, &mut leaf_page.page, 2).unwrap();

        // Write the page to disk
        leaf_page.write_page().unwrap();

        // Load the page from disk
        let loaded_leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
            table.path.clone(),
            leaf_page_num,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the index keys and values are correct
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &loaded_leaf_page.page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            LeafIndexPage::read_index_key(&index_key_type, &loaded_leaf_page.page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &loaded_leaf_page.page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &loaded_leaf_page.page, 1).unwrap(),
            index_value2
        );
        assert_eq!(
            LeafIndexPage::read_index_value(&index_key_type, &loaded_leaf_page.page, 2).unwrap(),
            index_value3
        );

        // Check that the attributes are correct
        assert_eq!(
            loaded_leaf_page.index_id,
            index_id
        );
        assert_eq!(
            loaded_leaf_page.index_key_type,
            index_key_type
        );
        assert_eq!(
            loaded_leaf_page.pagenum,
            leaf_page_num
        );

        // Clean up the testing table
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_row_locations_from_key() {
        let (table, 
            leaf_pagenum, 
            index_key_type, 
            index_id,
            index_keys,
            index_values) = create_testing_table_and_leaf_page();

        // Load the page from disk
        let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
            table.path.clone(),
            leaf_pagenum,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the correct row locations are returned
        assert_eq!(
            leaf_page.get_row_locations_from_key(&index_keys[0]).unwrap(),
            vec![index_values[0].to_row_location()]
        );
        assert_eq!(
            leaf_page.get_row_locations_from_key(&index_keys[1]).unwrap(),
            vec![index_values[1].to_row_location()]
        );
        assert_eq!(
            leaf_page.get_row_locations_from_key(&index_keys[2]).unwrap(),
            vec![index_values[2].to_row_location()]
        );
        assert_eq!(
            leaf_page.get_row_locations_from_key(&index_keys[3]).unwrap(),
            vec![index_values[3].to_row_location()]
        );

        // Test a key that doesn't exist in the leaf
        assert_eq!(
            leaf_page.get_row_locations_from_key(&vec![Value::I32(1), Value::String("new_key".to_string())]).unwrap().len(),
            0  
        );

        // Clean up the testing table
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_row_locations_from_key_range() {
        let (table, 
            leaf_pagenum, 
            index_key_type, 
            index_id,
            _,
            index_values) = create_testing_table_and_leaf_page();

        // Load the page from disk
        let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
            table.path.clone(),
            leaf_pagenum,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the correct row locations are returned
        let tables: Vec<(Table, String)> = vec![(table.clone(), table.name.clone())];
        let column_aliases: ColumnAliases = gen_column_aliases(&tables);
        let index_refs: IndexRefs = get_index_refs(&column_aliases);
        // Test WHERE id > 2
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number("2".to_string(), true)))
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[2].to_row_location(), index_values[3].to_row_location()]
        );
        // Test WHERE id = 3 AND name = 'c'
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("3".to_string(), true)))
                        }
                    ),
                    op: BinaryOperator::And,
                    right: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "name".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("c".to_string(), true)))
                        }
                    ),
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[2].to_row_location()]
        );
        // Test WHERE id = 3 AND name = 'd'
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("3".to_string(), true)))
                        }
                    ),
                    op: BinaryOperator::And,
                    right: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "name".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("d".to_string(), true)))
                        }
                    ),
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            Vec::new()
        );
        // Test WHERE id = 3 OR name = 'd'
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("3".to_string(), true)))
                        }
                    ),
                    op: BinaryOperator::Or,
                    right: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "name".to_string(), quote_style: None })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("d".to_string(), true)))
                        }
                    ),
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[2].to_row_location(), index_values[3].to_row_location()]
        );
        // Test WHERE name > 'a'
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident { value: "name".to_string(), quote_style: None })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number("a".to_string(), true)))
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[1].to_row_location(), index_values[2].to_row_location(), index_values[3].to_row_location()]
        );
        // Test WHERE id > 1 AND name < 'd'
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("1".to_string(), true)))
                        }
                    ),
                    op: BinaryOperator::And,
                    right: Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident { value: "name".to_string(), quote_style: None })),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number("d".to_string(), true)))
                        }
                    ),
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[1].to_row_location(), index_values[2].to_row_location()]
        );

        // Clean up the testing table
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_row_locations_from_key_range2() {
        let (table, 
            leaf_pagenum, 
            index_key_type, 
            index_id,
            _,
            index_values) = create_testing_table_and_leaf_page2();

        // Load the page from disk
        let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
            table.path.clone(),
            leaf_pagenum,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the correct row locations are returned
        let tables: Vec<(Table, String)> = vec![(table.clone(), table.name.clone())];
        let column_aliases: ColumnAliases = gen_column_aliases(&tables);
        let index_refs: IndexRefs = get_index_refs(&column_aliases);
        assert_eq!(
            leaf_page.get_row_locations_matching_expr(
                &Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: None })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number("3".to_string(), true)))
                },
                &column_aliases,
                &index_refs
            ).unwrap(),
            vec![index_values[2].to_row_location()]
        );

        // Clean up the testing table
        clean_up_tests();
    }

    /// Creates a testing table and leaf page with 4 index keys and values.
    fn create_testing_table_and_leaf_page() -> (Table, u32, IndexKeyType, IndexID, Vec<IndexKey>, Vec<LeafIndexValue>) {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("testing_leaf_page_table");
        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(10)];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10))
        ];
        let index_column_names: Vec<String> = vec!["id".to_string(), "name".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let leaf_page_num: u32 = 2;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap().0;
        write_page(leaf_page_num, &table.path, &[0; PAGE_SIZE], PageType::LeafIndex).unwrap();
        
        // Create the leaf page
        let mut leaf_page: LeafIndexPage = LeafIndexPage::new(
            table.path.clone(),
            leaf_page_num,
            &index_id,
            &index_key_type
        ).unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index_key3: IndexKey = vec![Value::I32(3), Value::String("c".to_string())];
        let index_key4: IndexKey = vec![Value::I32(4), Value::String("d".to_string())];

        let index_value1: LeafIndexValue = LeafIndexValue { pagenum: 3, rownum: 0 };
        let index_value2: LeafIndexValue = LeafIndexValue { pagenum: 4, rownum: 123 };
        let index_value3: LeafIndexValue = LeafIndexValue { pagenum: 219, rownum: 89 };
        let index_value4: LeafIndexValue = LeafIndexValue { pagenum: 219, rownum: 90 };

        // Write the index keys and values to the page structure
        LeafIndexPage::write_index_key(&index_key1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_key(&index_key2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_key(&index_key3, &index_key_type, &mut leaf_page.page, 2).unwrap();
        LeafIndexPage::write_index_key(&index_key4, &index_key_type, &mut leaf_page.page, 3).unwrap();
        LeafIndexPage::write_index_value(&index_value1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_value(&index_value2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_value(&index_value3, &index_key_type, &mut leaf_page.page, 2).unwrap();
        LeafIndexPage::write_index_value(&index_value4, &index_key_type, &mut leaf_page.page, 3).unwrap();

        leaf_page.indexes.push((index_key1.clone(), index_value1.clone()));
        leaf_page.indexes.push((index_key2.clone(), index_value2.clone()));
        leaf_page.indexes.push((index_key3.clone(), index_value3.clone()));
        leaf_page.indexes.push((index_key4.clone(), index_value4.clone()));

        // Write the page to disk
        leaf_page.write_page().unwrap();

        (
            table, leaf_page_num,
            index_key_type,
            index_id,
            vec![index_key1, index_key2, index_key3, index_key4],
            vec![index_value1, index_value2, index_value3, index_value4]
        )
    }

    /// Creates a testing table and leaf page with 4 index keys and values.
    fn create_testing_table_and_leaf_page2() -> (Table, u32, IndexKeyType, IndexID, Vec<IndexKey>, Vec<LeafIndexValue>) {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("testing_leaf_page_table");
        let index_key_type: IndexKeyType = vec![Column::I32];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10))
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let leaf_page_num: u32 = 2;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap().0;
        write_page(leaf_page_num, &table.path, &[0; PAGE_SIZE], PageType::LeafIndex).unwrap();
        
        // Create the leaf page
        let mut leaf_page: LeafIndexPage = LeafIndexPage::new(
            table.path.clone(),
            leaf_page_num,
            &index_id,
            &index_key_type
        ).unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1)];
        let index_key2: IndexKey = vec![Value::I32(2)];
        let index_key3: IndexKey = vec![Value::I32(3)];
        let index_key4: IndexKey = vec![Value::I32(4)];

        let index_value1: LeafIndexValue = LeafIndexValue { pagenum: 3, rownum: 0 };
        let index_value2: LeafIndexValue = LeafIndexValue { pagenum: 4, rownum: 123 };
        let index_value3: LeafIndexValue = LeafIndexValue { pagenum: 219, rownum: 89 };
        let index_value4: LeafIndexValue = LeafIndexValue { pagenum: 219, rownum: 90 };

        // Write the index keys and values to the page structure
        LeafIndexPage::write_index_key(&index_key1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_key(&index_key2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_key(&index_key3, &index_key_type, &mut leaf_page.page, 2).unwrap();
        LeafIndexPage::write_index_key(&index_key4, &index_key_type, &mut leaf_page.page, 3).unwrap();
        LeafIndexPage::write_index_value(&index_value1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        LeafIndexPage::write_index_value(&index_value2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        LeafIndexPage::write_index_value(&index_value3, &index_key_type, &mut leaf_page.page, 2).unwrap();
        LeafIndexPage::write_index_value(&index_value4, &index_key_type, &mut leaf_page.page, 3).unwrap();

        leaf_page.indexes.push((index_key1.clone(), index_value1.clone()));
        leaf_page.indexes.push((index_key2.clone(), index_value2.clone()));
        leaf_page.indexes.push((index_key3.clone(), index_value3.clone()));
        leaf_page.indexes.push((index_key4.clone(), index_value4.clone()));

        // Write the page to disk
        leaf_page.write_page().unwrap();

        (
            table, leaf_page_num,
            index_key_type,
            index_id,
            vec![index_key1, index_key2, index_key3, index_key4],
            vec![index_value1, index_value2, index_value3, index_value4]
        )
    }

    fn clean_up_tests() {
        std::fs::remove_dir_all("./testing").unwrap();
    }
}