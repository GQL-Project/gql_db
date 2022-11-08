use std::{collections::HashMap, mem::size_of};
use super::btree::*;
use crate::fileio::{header::*, pageio::*};

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | IndexValue | IndexKey | IndexValue | ... | IndexKey |
/// where there is an equal number of index values and index keys.
pub struct LeafIndexPage {
    pagenum: u32,                               // The page number that this page is stored at
    indexes: HashMap<IndexKey, LeafIndexValue>, // The values in this page
    index_key_type: IndexKeyType,               // The type of the index keys
    cols_used_in_index: ColsInIndex,            // The columns used in this index
    key_size: u8,                               // The size of an individual member of index_keys
}

impl LeafIndexPage {
    /// Creates a new leaf index page on disk at the given page number.
    pub fn create(
        pagenum: u32,
        cols_used_in_index: ColsInIndex,
        initial_key: IndexKey,
        initial_value: LeafIndexValue,
        table_schema: Schema,
        table_path: &String
    ) -> Result<Self, String> {
        let mut new_page: Page = [0; PAGE_SIZE];

        let mut index_key_type: IndexKeyType = Vec::new();
        for col_idx in &cols_used_in_index {
            index_key_type.push(table_schema[*col_idx as usize].1.clone());
        }

        // Get the size of an individual key
        let key_size: u8 = index_key_type.iter().map(|col| col.size() as u8).sum();

        // Insert the initial values
        Self::write_index_value(&initial_value, &mut new_page, 0)?;
        Self::write_index_key(&initial_key, &index_key_type, &mut new_page, 0)?;

        // Write the page with the intial values to disk
        write_page(pagenum, &table_path, &new_page, PageType::Index)?;

        Ok(LeafIndexPage {
            pagenum,
            indexes: HashMap::new(),
            index_key_type,
            cols_used_in_index,
            key_size,
        })
    }

    /***********************************************************************************************/
    /*                                       Private Methods                                       */
    /***********************************************************************************************/

    /// Returns true if there is room for another index and value in this page.
    fn has_room(&self) -> bool {
        let all_keys_size: usize = self.indexes.len() * self.key_size as usize;
        let all_values_size: usize = self.indexes.len() * size_of::<LeafIndexValue>();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + size_of::<LeafIndexValue>() <= PAGE_SIZE {
            return true;
        }
        false
    }

    /// Writes an index key to a page at a specific offset
    fn write_index_key(
        index_key: &IndexKey,
        index_key_type: &IndexKeyType,
        page: &mut Page, 
        index_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * size_of::<IndexKey>()) +                 // The offset of the index keys
                            ((index_idx + 1) * size_of::<InternalIndexValue>()) + // The offset of the index values
                            PAGE_HEADER_SIZE;                                     // The offset of the page header
        write_index_key_at_offset(index_key, index_key_type, page, offset)
    }

    /// Writes an index key to a page at a specific offset
    fn write_index_value(
        index_value: &LeafIndexValue, 
        page: &mut Page, 
        value_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * size_of::<IndexKey>()) +           // The offset of the index keys
                            (value_idx * size_of::<InternalIndexValue>()) + // The offset of the index values
                            PAGE_HEADER_SIZE;                               // The offset of the page header
        write_leaf_index_value_at_offset(index_value, page, offset)
    }
}