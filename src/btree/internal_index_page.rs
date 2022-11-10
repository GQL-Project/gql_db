use std::{mem::size_of, collections::HashSet};

use super::{indexes::*, leaf_index_page::LeafIndexPage};
use crate::{fileio::{header::*, pageio::*, rowio::*}, util::row::*};

const INTERNAL_PAGE_HEADER_SIZE: usize = size_of::<u16>() + size_of::<u8>();

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | NumValues<u16> | PageDepth<u8> | IndexValue | IndexKey | IndexValue | IndexKey | ... | IndexValue |
/// where there is 1 more index value than index keys.
/// IndexKeys:      | 1  | 5  | 9  | 15 |
/// IndexValues: | <1 | <5 | <9 | <15 | >=15 |
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
        write_type::<u16>(&mut page, 0, 1u16)?;
        write_type::<u8>(&mut page, size_of::<u16>(), node_depth_in_tree as u8)?;

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
    
    /// Loads a leaf index page from disk at the specified page number.
    pub fn load_from_table(
        table_path: String,
        pagenum: u32,
        index_id: &IndexID,
        index_key_type: &IndexKeyType,
    ) -> Result<Self, String> {
        let (page, page_type) = read_page(pagenum, &table_path)?;
        if page_type != PageType::InternalIndex {
            return Err(format!(
                "Error page {} is not a leaf index page in {}",
                pagenum, table_path
            ));
        }

        // Get the size of an individual key
        let key_size: u8 = get_index_key_type_size(&index_key_type) as u8;

        // Read the number of values and page_depth from the page
        let num_values: u16 = read_type::<u16>(&page, 0)?;
        let page_depth: u8 = read_type::<u8>(&page, size_of::<u16>())?;

        // Read the values from the page.
        // There are 1 more values than keys.
        let mut keys: Vec<IndexKey> = Vec::new();
        let mut values: Vec<InternalIndexValue> = Vec::new();
        for i in 0..num_values - 1 {
            let index_key: IndexKey = Self::read_index_key(&index_key_type, &page, i as usize)?;
            keys.push(index_key);
        }
        for i in 0..num_values {
            let index_value: InternalIndexValue = Self::read_index_value(&index_key_type, &page, i as usize)?;
            values.push(index_value);
        }

        Ok(InternalIndexPage {
            table_path,
            pagenum,
            index_keys: keys,
            index_values: values,
            index_key_type: index_key_type.clone(),
            index_id: index_id.clone(),
            key_size,
            page_depth,
            page: *page,
        })
    }

    /// Writes the page to disk at the specified page number.
    pub fn write_page(
        &mut self
    ) -> Result<(), String> {
        write_type::<u16>(&mut self.page, 0, self.index_values.len() as u16)?;
        write_type::<u8>(&mut self.page, size_of::<u16>(), self.page_depth as u8)?;
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

    /// Gets the rows that are stored from the specific index key
    pub fn get_rows_from_key(
        &self,
        index_key: &IndexKey,
        table_schema: &Schema
    ) -> Result<Vec<RowInfo>, String> {
        let leaf_pagenums: HashSet<u32> = self.get_leaf_pagenums_from_key(index_key)?;

        // Get all the row locations we need to read the rows from
        let mut row_locations: Vec<RowLocation> = Vec::new();
        for leaf_pagenum in leaf_pagenums {
            let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
                self.table_path.clone(),
                leaf_pagenum,
                &self.index_id,
                &self.index_key_type
            )?;
            let leaf_row_locations: Vec<RowLocation> = leaf_page.get_row_locations_from_key(index_key)?;
            row_locations.extend(leaf_row_locations);
        }

        // Sort the row_locations by pagenum so we reduce the number of page reads we need
        row_locations.sort_by(|a, b| a.pagenum.cmp(&b.pagenum));

        // Read the rows from the row locations
        let mut rows: Vec<RowInfo> = Vec::new();
        let mut current_pagenum: u32 = 0;
        let mut current_page: Option<Page> = None;
        for row_location in row_locations {
            if current_pagenum != row_location.pagenum || current_page.is_none() {
                current_pagenum = row_location.pagenum;
                let (page, page_type) = read_page(current_pagenum, &self.table_path)?;
                if page_type != PageType::Data {
                    return Err(format!("Expected page type to be Data, but got {:?}", page_type));
                }
                current_page = Some(*page);
            }
            let row: Option<Row> = read_row(table_schema,&current_page.unwrap(), row_location.rownum);
            if let Some(row_value) = row {
                rows.push(RowInfo {
                    row: row_value,
                    pagenum: row_location.pagenum,
                    rownum: row_location.rownum,
                });
            }
            else {
                return Err(format!("Could not read row from page {} row {}", row_location.pagenum, row_location.rownum));
            }
        }

        Ok(rows)
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

    /// Gets the leaf page numbers that correspond to the index key provided.
    /// Traverses all the index pages down to the leaf pages.
    fn get_leaf_pagenums_from_key(
        &self,
        index_key: &IndexKey
    ) -> Result<HashSet<u32>, String> {
        let mut lowest_internal_pages: HashSet<u32> = HashSet::new();
        
        // Iterate through each level of index pages to get the lowest level internal pages
        lowest_internal_pages.insert(self.pagenum);
        for _ in 1..self.page_depth {
            let current_page_numbers: HashSet<u32> = lowest_internal_pages.clone();
            lowest_internal_pages = HashSet::new();

            for page_number in current_page_numbers {
                let page: InternalIndexPage = InternalIndexPage::load_from_table(
                    self.table_path.clone(),
                    page_number,
                    &self.index_id,
                    &self.index_key_type
                ).unwrap();
                let values_from_key: Vec<InternalIndexValue> = page.get_index_values_from_key(index_key)?;
                let pages_below_this_page: Vec<u32> = values_from_key
                    .iter()
                    .map(|value| value.pagenum)
                    .collect();

                lowest_internal_pages.extend(pages_below_this_page);
            }
        }

        // Get the leaf pages from the lowest level internal pages
        let mut leaf_pages: HashSet<u32> = HashSet::new();
        for page_number in lowest_internal_pages {
            let page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                page_number,
                &self.index_id,
                &self.index_key_type
            ).unwrap();
            let values_from_key: Vec<InternalIndexValue> = page.get_index_values_from_key(index_key)?;
            let leaf_pages_below_this_page: Vec<u32> = values_from_key
                .iter()
                .map(|value| value.pagenum)
                .collect();

            leaf_pages.extend(leaf_pages_below_this_page);
        }
        Ok(leaf_pages)
    }

    /// Gets the internal index values that match the index key.
    /// Returns a vector of InternalIndexValue.
    fn get_index_values_from_key(
        &self,
        index_key: &IndexKey
    ) -> Result<Vec<InternalIndexValue>, String> {
        // If there are no keys
        if self.index_keys.len() == 0 {
            // There is always an index value in an internal page, even with no keys.
            if self.index_values.len() == 0 {
                return Err(format!("get_index_values_from_key(): Internal page {} has no index values.", self.pagenum));
            }
            return Ok(vec![self.index_values[0].clone()]);
        }
        else {
            let mut pointers: Vec<InternalIndexValue> = Vec::new();
            for (i, key) in self.index_keys.iter().enumerate() {
                // IndexKeys:      | 1  | 5  | 9  | 15 |
                // IndexValues: | <1 | <5 | <9 | <15 | >=15 |
                let comparison: KeyComparison = compare_indexes(&key, &index_key);
                match comparison {
                    KeyComparison::Equal => {
                        // we found the key, but we need to check for duplicate keys because we can have
                        // duplicate index values pointing to multiple pages.
                        let mut j: usize = i + 1;
                        loop {
                            pointers.push(self.index_values[j].clone());
                            if j + 1 >= self.index_keys.len() {
                                break;
                            }
                            if compare_indexes(&self.index_keys[j + 1], &index_key) != KeyComparison::Equal {
                                break;
                            }
                            j += 1;
                        };
                        break;
                    },
                    // If the key is greater than our index_key, then we want the value from index i
                    // and break out of the loop
                    KeyComparison::Greater => {
                        if self.index_values.len() > i {
                            pointers.push(self.index_values[i].clone());
                            break;
                        }
                        else {
                            return Err(
                                format!(
                                    "get_index_values_from_key(): Internal page {} doesn't have an index value at {}.",
                                    self.pagenum,
                                    i
                                )
                            );
                        }
                    },
                    _ => {},
                }
            }
            // We need to return the last index value if we didn't find a match
            if pointers.len() == 0 {
                // There is always an index value in an internal page, even with no keys.
                if self.index_values.len() == 0 {
                    return Err(format!("get_index_values_from_key(): Internal page {} has no index values.", self.pagenum));
                }
                pointers.push(self.index_values[self.index_values.len() - 1].clone());
            }
            return Ok(pointers);
        }
    }

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
    use serial_test::serial;

    use super::*;
    use crate::{util::dbtype::*, fileio::tableio::*};

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

    #[test]
    #[serial]
    fn test_internal_load_from_table() {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("test_leaf_load_from_table");
        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(10)];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10))
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let internal_page_num: u32 = 2;
        let internal_page_depth: u8 = 7;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap().0;
        write_page(internal_page_num, &table.path, &[0; PAGE_SIZE], PageType::InternalIndex).unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];

        let index_value1: InternalIndexValue = InternalIndexValue { pagenum: 3 };
        let index_value2: InternalIndexValue = InternalIndexValue { pagenum: 4 };
        let index_value3: InternalIndexValue = InternalIndexValue { pagenum: 219 };
        
        // Create the leaf page
        let mut leaf_page: InternalIndexPage = InternalIndexPage::new(
            table.path.clone(),
            internal_page_num,
            &index_id,
            &index_key_type,
            &index_value1,
            internal_page_depth
        ).unwrap();

        // Write the index keys and values to the page structure
        InternalIndexPage::write_index_key(&index_key1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        InternalIndexPage::write_index_key(&index_key2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        InternalIndexPage::write_index_value(&index_value1, &index_key_type, &mut leaf_page.page, 0).unwrap();
        InternalIndexPage::write_index_value(&index_value2, &index_key_type, &mut leaf_page.page, 1).unwrap();
        InternalIndexPage::write_index_value(&index_value3, &index_key_type, &mut leaf_page.page, 2).unwrap();

        // Write the page to disk
        leaf_page.write_page().unwrap();

        // Load the page from disk
        let loaded_internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            internal_page_num,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the index keys and values are correct
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &loaded_internal_page.page, 0).unwrap(),
            index_key1
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &loaded_internal_page.page, 1).unwrap(),
            index_key2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 0).unwrap(),
            index_value1
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 1).unwrap(),
            index_value2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 2).unwrap(),
            index_value3
        );

        // Check that the attributes are correct
        assert_eq!(
            loaded_internal_page.pagenum,
            internal_page_num
        );
        assert_eq!(
            loaded_internal_page.index_id,
            index_id
        );
        assert_eq!(
            loaded_internal_page.index_key_type,
            index_key_type
        );
        assert_eq!(
            loaded_internal_page.page_depth,
            internal_page_depth
        );

        // Clean up tests
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_index_values_from_key() {
        let (table, 
            internal_pagenum, 
            index_key_type, 
            index_id,
            index_keys,
            index_values) = create_testing_table_and_internal_page();

        // Load the page from disk
        let internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            internal_pagenum,
            &index_id,
            &index_key_type
        ).unwrap();

        // Check that the correct row locations are returned
        assert_eq!(
            internal_page.get_index_values_from_key(&index_keys[0]).unwrap(),
            vec![index_values[1].clone()]
        );
        assert_eq!(
            internal_page.get_index_values_from_key(&index_keys[1]).unwrap(),
            vec![index_values[2].clone()]
        );
        assert_eq!(
            internal_page.get_index_values_from_key(&index_keys[2]).unwrap(),
            vec![index_values[3].clone()]
        );
        assert_eq!(
            internal_page.get_index_values_from_key(&index_keys[3]).unwrap(),
            vec![index_values[4].clone()]
        );

        // Test a key that is greater than all the other keys in the page
        assert_eq!(
            internal_page.get_index_values_from_key(&vec![Value::I32(4), Value::String("e".to_string())]).unwrap(),
            vec![index_values[4].clone()]
        );

        // Test a key that is less than all the other keys in the page
        assert_eq!(
            internal_page.get_index_values_from_key(&vec![Value::I32(0), Value::String("a".to_string())]).unwrap(),
            vec![index_values[0].clone()]
        );

        // Test keys that are in between the 2nd and 3rd keys in the page
        assert_eq!(
            internal_page.get_index_values_from_key(&vec![Value::I32(2), Value::String("c".to_string())]).unwrap(),
            vec![index_values[2].clone()]
        );
        assert_eq!(
            internal_page.get_index_values_from_key(&vec![Value::I32(3), Value::String("a".to_string())]).unwrap(),
            vec![index_values[2].clone()]
        );

        // Clean up the testing table
        clean_up_tests();
    }

    /// Creates a testing table and leaf page with 4 index keys and values.
    fn create_testing_table_and_internal_page() -> (Table, u32, IndexKeyType, IndexID, Vec<IndexKey>, Vec<InternalIndexValue>) {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("testing_internal_page_table");
        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(10)];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10))
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let internal_page_num: u32 = 2;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap().0;
        write_page(internal_page_num, &table.path, &[0; PAGE_SIZE], PageType::LeafIndex).unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];
        let index_key3: IndexKey = vec![Value::I32(3), Value::String("c".to_string())];
        let index_key4: IndexKey = vec![Value::I32(4), Value::String("d".to_string())];

        let index_value1: InternalIndexValue = InternalIndexValue { pagenum: 3 };
        let index_value2: InternalIndexValue = InternalIndexValue { pagenum: 4 };
        let index_value3: InternalIndexValue = InternalIndexValue { pagenum: 219 };
        let index_value4: InternalIndexValue = InternalIndexValue { pagenum: 219 };
        let index_value5: InternalIndexValue = InternalIndexValue { pagenum: 578 };
        
        // Create the leaf page
        let mut internal_page: InternalIndexPage = InternalIndexPage::new(
            table.path.clone(),
            internal_page_num,
            &index_id,
            &index_key_type,
            &index_value1,
            1
        ).unwrap();

        // Write the index keys and values to the page structure
        InternalIndexPage::write_index_key(&index_key1, &index_key_type, &mut internal_page.page, 0).unwrap();
        InternalIndexPage::write_index_key(&index_key2, &index_key_type, &mut internal_page.page, 1).unwrap();
        InternalIndexPage::write_index_key(&index_key3, &index_key_type, &mut internal_page.page, 2).unwrap();
        InternalIndexPage::write_index_key(&index_key4, &index_key_type, &mut internal_page.page, 3).unwrap();
        InternalIndexPage::write_index_value(&index_value2, &index_key_type, &mut internal_page.page, 1).unwrap();
        InternalIndexPage::write_index_value(&index_value3, &index_key_type, &mut internal_page.page, 2).unwrap();
        InternalIndexPage::write_index_value(&index_value4, &index_key_type, &mut internal_page.page, 3).unwrap();
        InternalIndexPage::write_index_value(&index_value5, &index_key_type, &mut internal_page.page, 4).unwrap();

        internal_page.index_keys.push(index_key1.clone());
        internal_page.index_keys.push(index_key2.clone());
        internal_page.index_keys.push(index_key3.clone());
        internal_page.index_keys.push(index_key4.clone());

        internal_page.index_values.push(index_value2.clone());
        internal_page.index_values.push(index_value3.clone());
        internal_page.index_values.push(index_value4.clone());
        internal_page.index_values.push(index_value5.clone());

        // Write the page to disk
        internal_page.write_page().unwrap();

        (
            table, internal_page_num,
            index_key_type,
            index_id,
            vec![index_key1, index_key2, index_key3, index_key4],
            vec![index_value1, index_value2, index_value3, index_value4, index_value5]
        )
    }

    fn clean_up_tests() {
        std::fs::remove_dir_all("./testing").unwrap();
    }
}