use std::mem::size_of;

use itertools::Itertools;

use super::indexes::*;
use crate::{fileio::{header::*, pageio::*, rowio::write_row, tableio::Table}, util::dbtype::Column};

const INTERNAL_PAGE_HEADER_SIZE: usize = size_of::<u16>();

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | NumValues<u16> | IndexValue | IndexKey | IndexValue | IndexKey | ... | IndexValue |
/// where there is 1 more index value than index keys.
/// Every InternalIndexPage is stored with at least 1 value.
/// The leftmost IndexValue ALWAYS points to another page.
/// If there are no keys in the page, that means the page pointer in the lefmost IndexValue points to an empty page.
#[derive(Debug, Clone)]
pub struct InternalIndexPage {
    table_path: String,                    // The path to the table this page belongs to
    pagenum: u32,                          // The page number of this page
    index_keys: Vec<IndexKey>,             // The keys in this page
    index_key_type: IndexKeyType,          // The type of the index keys
    index_values: Vec<InternalIndexValue>, // The values in this page
    index_id: IndexID,                     // The columns used in this index
    page_depth: u8,                        // The depth of this page in the btree. (0 is a leaf page)
    key_size: u8,                          // The size of an individual member of index_keys
    page: Page,                            // The page data
}

impl InternalIndexPage {
    /// Creates a new internal index page.
    pub fn new(
        table_path: String,
        pagenum: u32,
        index_id: &IndexID,
        index_key_type: &IndexKeyType,
        initial_value: &InternalIndexValue,
        node_depth_in_tree: u8, // The depth of this page in the btree where 0 is a leaf page
    ) -> Result<Self, String> {
        let mut page: Page = [0; PAGE_SIZE];
        // write the number of values to the page
        write_type(&mut page, 0, 0u16)?;

        // Get the size of an individual key
        let key_size: u8 = get_index_key_type_size(&index_key_type) as u8;

        // Insert the initial value
        Self::write_index_value(initial_value, index_key_type, &mut page, 0)?;

        Ok(InternalIndexPage {
            table_path,
            pagenum,
            index_keys: Vec::new(),
            index_key_type: index_key_type.clone(),
            index_values: vec![initial_value.clone()],
            index_id: index_id.clone(),
            page_depth: node_depth_in_tree,
            key_size,
            page,
        })
    }

    /// Writes the page to disk at the specified page number.
    pub fn write_page(
        &mut self
    ) -> Result<(), String> {
        write_type::<u16>(&mut self.page, 0, self.index_values.len() as u16)?;
        write_page(self.pagenum, &self.table_path, &self.page, PageType::InternalIndex)?;
        Ok(())
    }

    /// Gets the page number of the page where this page is stored
    pub fn get_pagenum(&self) -> u32 {
        self.pagenum
    }

    /// Gets the lowest valued index key in the page.
    pub fn get_lowest_index_key(
        &self
    ) -> Option<IndexKey> {
        for key in self.index_keys.clone() {
            return Some(key.clone());
        }
        None
    }
    
    /// Inserts a page pointer into the page.
    /// Returns whether the value was inserted or whether the page is full.
    pub fn add_pointer_to_page(
        &mut self,
        index_key: &IndexKey,
        index_value: &InternalIndexValue
    ) -> Result<bool, String> {
        if !self.has_room() {
            return Ok(false);
        }

        // Find the index where we need to insert the key. Locate the first index where the key is greater than the index_key we're inserting.
        let mut idx_to_insert: Option<usize> = None;
        for (i, key) in self.index_keys.clone().iter().enumerate() {
            if compare_indexes(&key, &index_key) == KeyComparison::Greater {
                idx_to_insert = Some(i);

                // Insert the key at the index in the page
                Self::write_index_key(&index_key, &self.index_key_type, &mut self.page, i)?;
                Self::write_index_value(&index_value, &self.index_key_type, &mut self.page, i + 1)?;

                // Write the rest of the keys and values that come after index i
                for (j, (key, value)) in self.index_keys.clone().iter().zip(self.index_values.iter().skip(i + 1)).enumerate().skip(i) {
                    Self::write_index_key(&key, &self.index_key_type, &mut self.page, j + 1)?;
                    Self::write_index_value(&value, &self.index_key_type, &mut self.page, j + 2)?;
                }

                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if idx_to_insert.is_none() {
            idx_to_insert = Some(self.index_keys.len());
            Self::write_index_key(&index_key, &self.index_key_type, &mut self.page, self.index_keys.len())?;
            Self::write_index_value(&index_value, &self.index_key_type, &mut self.page, self.index_keys.len() + 1)?;
        }

        let vector_index_to_insert: usize = idx_to_insert.unwrap();

        // Insert the key into the hashmap
        self.index_keys.insert(vector_index_to_insert, index_key.clone());
        self.index_values.insert(vector_index_to_insert + 1, index_value.clone());

        Ok(true)
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
                                        InternalIndexValue::size();
        let num_idx_val_pairs: usize = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE - InternalIndexValue::size()) / idx_and_value_size;
        num_idx_val_pairs + 1
    }

    /***********************************************************************************************/
    /*                                       Private Member Methods                                */
    /***********************************************************************************************/

    /// Returns true if there is room for another index and value in this page.
    fn has_room(
        &self
    ) -> bool {
        let all_keys_size: usize = self.index_keys.len() * self.key_size as usize;
        let all_values_size: usize = self.index_values.len() * InternalIndexValue::size();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + InternalIndexValue::size() <= (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) {
            return true;
        }
        false
    }

    /*
    fn get_pages_below_from_key(
        &self,
        index_key: &IndexKey
    ) -> Result<Vec<u32>, String> {
        let mut pages_below: Vec<u32> = Vec::new();
        for (key, value) in self.index_keys.clone().iter().zip(self.index_values.iter()) {
            if compare_indexes(&key, &index_key) == KeyComparison::Greater {
                break;
            }
            pages_below.push(value.get_page_num());
        }
        Ok(pages_below)
    }
    */

    /***********************************************************************************************/
    /*                                       Private Static Methods                                */
    /***********************************************************************************************/

    /// Writes an index key to a page at a specific index key's index
    /// Note: This is the index key's index.
    /// i.e. the first index key is index_idx=0, the second index key is index_idx=1, etc.
    fn write_index_key(
        index_key: &IndexKey,
        index_key_type: &IndexKeyType,
        page: &mut Page, 
        index_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            ((index_idx + 1) * InternalIndexValue::size()) +        // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE;                              // The offset of the page header
        write_index_key_at_offset(index_key, index_key_type, page, offset)
    }

    /// Writes an index key to a page at a specific index value's index
    /// Note: This is the index value's index, not the index key's index.
    /// i.e. the first index value is index value_idx=0, the second index value is index value_idx=1, etc.
    fn write_index_value(
        index_value: &InternalIndexValue, 
        index_key_type: &IndexKeyType,
        page: &mut Page, 
        value_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * InternalIndexValue::size()) +              // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE;                              // The offset of the page header
        write_internal_index_value_at_offset(index_value, page, offset)
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
                            ((index_idx + 1) * InternalIndexValue::size()) +        // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE;                              // The offset of the page header
        read_index_key_at_offset(index_key_type, page, offset)
    }

    /// Reads an index value from a page at a specific index value's index
    /// Note: This is the index value's index, not the index key's index.
    /// i.e. the first index value is index value_idx=0, the second index value is index value_idx=1, etc.
    fn read_index_value(
        index_key_type: &IndexKeyType,
        page: &Page, 
        value_idx: usize
    ) -> Result<InternalIndexValue, String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * InternalIndexValue::size()) +              // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE;                              // The offset of the page header
        read_internal_index_value_at_offset(page, offset)
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
        InternalIndexPage::write_index_key(&index_key1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );

        // Write index_key2 to the page at index_idx=1
        InternalIndexPage::write_index_key(&index_key2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );

        // Write index_key3 to the page at index_idx=2
        InternalIndexPage::write_index_key(&index_key3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );

        // Read the index keys from the page
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
    }

    #[test]
    fn test_read_write_index_value() {
        let mut page: Page = [0; PAGE_SIZE];

        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(20)];

        let index_value1: InternalIndexValue = InternalIndexValue { pagenum: 4 };
        let index_value2: InternalIndexValue = InternalIndexValue { pagenum: 50 };
        let index_value3: InternalIndexValue = InternalIndexValue { pagenum: 235 };

        // Write index_value1 to the page at index_idx=0
        InternalIndexPage::write_index_value(&index_value1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );

        // Write index_value2 to the page at index_idx=1
        InternalIndexPage::write_index_value(&index_value2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );

        // Write index_value3 to the page at index_idx=2
        InternalIndexPage::write_index_value(&index_value3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );

        // Read the index values from the page
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
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

        let index_value1: InternalIndexValue = InternalIndexValue { pagenum: 3 };
        let index_value2: InternalIndexValue = InternalIndexValue { pagenum: 4 };
        let index_value3: InternalIndexValue = InternalIndexValue { pagenum: 50 };
        let index_value4: InternalIndexValue = InternalIndexValue { pagenum: 235 };

        // Write index_key1 and index_value1 to the page at index_idx=0
        InternalIndexPage::write_index_key(&index_key1, &index_key_type, &mut page, 0).unwrap();
        InternalIndexPage::write_index_value(&index_value1, &index_key_type, &mut page, 0).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );

        // Write index_key2 and index_value2 to the page at index_idx=1
        InternalIndexPage::write_index_key(&index_key2, &index_key_type, &mut page, 1).unwrap();
        InternalIndexPage::write_index_value(&index_value2, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value2
        );

        // Write index_key3 and index_value3 to the page at index_idx=2
        InternalIndexPage::write_index_key(&index_key3, &index_key_type, &mut page, 2).unwrap();
        InternalIndexPage::write_index_value(&index_value3, &index_key_type, &mut page, 2).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );

        // Write index_value4 to the page at index_idx=3
        InternalIndexPage::write_index_value(&index_value4, &index_key_type, &mut page, 3).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 3).unwrap(),
            index_value4
        );

        // Overwrite index_value2 with index_value4
        InternalIndexPage::write_index_value(&index_value4, &index_key_type, &mut page, 1).unwrap();
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value4
        );

        // Read the index keys and values from the page
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 1).unwrap(),
            index_value4
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &page, 2).unwrap(),
            index_key3
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 2).unwrap(),
            index_value3
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &page, 3).unwrap(),
            index_value4
        );
    }
}