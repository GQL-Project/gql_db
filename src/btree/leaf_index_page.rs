use std::mem::size_of;
use itertools::Itertools;

use super::indexes::*;
use crate::{fileio::{header::*, pageio::*, tableio::Table}, util::{row::*, dbtype::Value}};

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
        write_page(self.pagenum, &self.table_path, &self.page, PageType::LeafIndex)?;
        Ok(())
    }

    /// Gets the page number of the page where this page is stored
    pub fn get_pagenum(&self) -> u32 {
        self.pagenum
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

        // Find the index where we need to insert the key. Locate the first index where the key is greater than the index_key we're inserting.
        let mut idx_to_insert: Option<usize> = None;
        for (i, (key, _)) in self.indexes.clone().iter().enumerate().sorted() {
            if compare_indexes(&key, &index_key) == KeyComparison::Greater {
                idx_to_insert = Some(i);

                // Insert the key at the index in the page
                Self::write_index_key(&index_key, &self.index_key_type, &mut self.page, i)?;
                Self::write_index_value(&index_value, &self.index_key_type, &mut self.page, i)?;

                // Write the rest of the keys and values that come after index i
                for (j, (key, value)) in self.indexes.clone().iter().enumerate().sorted().skip(i) {
                    Self::write_index_key(&key, &self.index_key_type, &mut self.page, j + 1)?;
                    Self::write_index_value(&value, &self.index_key_type, &mut self.page, j + 1)?;
                }

                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if idx_to_insert.is_none() {
            idx_to_insert = Some(self.indexes.len());
            Self::write_index_key(&index_key, &self.index_key_type, &mut self.page, self.indexes.len())?;
            Self::write_index_value(&index_value, &self.index_key_type, &mut self.page, self.indexes.len())?;
        }

        // Insert the key into the vector
        self.indexes.insert(idx_to_insert.unwrap(), (index_key.clone(), index_value.clone()));

        Ok(true)
    }

    /// Gets the rows that match the given index key.
    /// Returns a vector of RowLocations.
    pub fn get_rows_locations_from_key(
        &self,
        index_key: &IndexKey
    ) -> Result<Vec<RowLocation>, String> {
        // Check if the index key is present
        if self.indexes
            .iter()
            .map(|(key, _)| key)
            .sorted()
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

    /***********************************************************************************************/
    /*                                       Private Member Methods                                */
    /***********************************************************************************************/

    /// Returns true if there is room for another index and value in this page.
    fn has_room(&self) -> bool {
        let all_keys_size: usize = self.indexes.len() * self.key_size as usize;
        let all_values_size: usize = self.indexes.len() * LeafIndexValue::size();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + LeafIndexValue::size() <= (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) {
            return true;
        }
        false
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
    use super::*;
    use crate::util::dbtype::{Column, Value};

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
}