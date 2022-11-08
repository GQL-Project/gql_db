use std::mem::size_of;

use super::btree::*;
use crate::{fileio::{header::*, pageio::*, rowio::write_row}, util::dbtype::Column};

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | IndexValue | IndexKey | IndexValue | IndexKey | ... | IndexValue |
/// where there is 1 more index value than index keys.
/// Every InternalIndexPage is stored with at least 1 key.
/// The leftmost IndexValue ALWAYS points to another page.
pub struct InternalIndexPage {
    pagenum: u32,                          // The page number that this page is stored at
    index_keys: Vec<IndexKey>,             // The keys in this page
    index_key_type: IndexKeyType,          // The type of the index keys
    index_values: Vec<InternalIndexValue>, // The values in this page
    cols_used_in_index: ColsInIndex,       // The columns used in this index
    page_depth: u8,                        // The depth of this page in the btree. (0 is a leaf page)
    key_size: u8,                          // The size of an individual member of index_keys
}

impl InternalIndexPage {
    /// Creates a new internal index page on disk at the given page number.
    pub fn create(
        pagenum: u32,
        cols_used_in_index: ColsInIndex,
        initial_key: IndexKey,
        initial_values: (InternalIndexValue, InternalIndexValue),
        node_depth_in_tree: u8, // The depth of this page in the btree where 0 is a leaf page
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
        let left_initial_value: InternalIndexValue = initial_values.0;
        let right_initial_value: InternalIndexValue = initial_values.1;

        Self::write_index_value(&left_initial_value, &mut new_page, 0)?;
        Self::write_index_key(&initial_key, &index_key_type, &mut new_page, 0)?;
        Self::write_index_value(&right_initial_value, &mut new_page, 1)?;

        // Write the page with the intial values to disk
        write_page(pagenum, &table_path, &new_page, PageType::Index)?;

        Ok(InternalIndexPage {
            pagenum,
            index_keys: Vec::new(),
            index_key_type,
            index_values: Vec::new(),
            cols_used_in_index,
            page_depth: node_depth_in_tree,
            key_size,
        })
    }

    /// Inserts a new index and value into this page.
    /// Returns true if successfully inserted, false if there is no room.
    pub fn insert_index(
        &mut self, 
        new_index_key: IndexKey, 
        new_index_value: InternalIndexValue
    ) -> Result<bool, String> {
        // Check that the index keys are comparable
        if !are_comparable_index_types(&self.index_key_type, &get_index_key_type(&new_index_key)) {
            return Err(format!(
                "Index key {:?} is not comparable to index key type {:?}",
                new_index_key, self.index_key_type
            ));
        }

        // Check that there is room for this index
        if !self.has_room() {
            return Ok(false);
        }

        // If there are no keys in this page, just insert the new key and value
        if self.index_keys.len() == 0 {
            self.index_keys.push(new_index_key);
            self.index_values.push(new_index_value);
            return Ok(true);
        }

        // Find the index to insert the new key at
        

        // Insert the new key and value
        //self.index_keys.insert(insert_idx, new_index_key);
        //self.index_values.insert(insert_idx, new_index_value);

        Ok(true)
    }

    /***********************************************************************************************/
    /*                                       Private Methods                                       */
    /***********************************************************************************************/

    /// Returns true if there is room for another index and value in this page.
    fn has_room(
        &self
    ) -> bool {
        let all_keys_size: usize = self.index_keys.len() * self.key_size as usize;
        let all_values_size: usize = self.index_values.len() * size_of::<InternalIndexValue>();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + size_of::<InternalIndexValue>() <= PAGE_SIZE {
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
        index_value: &InternalIndexValue, 
        page: &mut Page, 
        value_idx: usize
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * size_of::<IndexKey>()) +           // The offset of the index keys
                            (value_idx * size_of::<InternalIndexValue>()) + // The offset of the index values
                            PAGE_HEADER_SIZE;                               // The offset of the page header
        write_internal_index_value_at_offset(index_value, page, offset)
    }
}