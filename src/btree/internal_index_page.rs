use std::mem::size_of;

use super::btree::*;
use crate::{fileio::{header::*, pageio::*}, util::dbtype::Column};

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | IndexValue | IndexKey | IndexValue | IndexKey | ... | IndexValue |
/// where there is 1 more index value than index keys.
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
        node_depth_in_tree: u8, // The depth of this page in the btree where 0 is a leaf page
        table_schema: Schema,
        table_path: &String
    ) -> Result<Self, String> {
        let new_page: Page = [0; PAGE_SIZE];
        write_page(pagenum, &table_path, &new_page, PageType::Index)?;

        let mut index_key_type: IndexKeyType = Vec::new();
        for col_idx in &cols_used_in_index {
            index_key_type.push(table_schema[*col_idx as usize].1.clone());
        }

        // Get the size of an individual key
        let key_size: u8 = index_key_type.iter().map(|col| col.size() as u8).sum();

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
    pub fn insert_index(
        &mut self, 
        new_index_key: IndexKey, 
        new_index_value: InternalIndexValue
    ) -> Result<(), String> {
        // Check that the index key is the correct type
        for (i, col) in self.index_key_type.iter().enumerate() {
            //if col != &new_index_key[i] {
            //    return Err(format!(
            //        "InternalIndexPage::insert_index: Index key is not the correct type. Expected {:?} but got {:?}",
            //        col, new_index_key[i]
            //    ));
            //}
        }

        // Check that there is room for this index
        if !self.has_room() {
            return Err(format!(
                "InternalIndexPage::insert_index: This page is full. Cannot insert index {:?} with value {:?}",
                new_index_key, new_index_value
            ));
        }

        // Find the index to insert the new key at
        

        // Insert the new key and value
        //self.index_keys.insert(insert_idx, new_index_key);
        //self.index_values.insert(insert_idx, new_index_value);

        Ok(())
    }

    /// Returns true if there is room for another index and value in this page.
    pub fn has_room(&self) -> bool {
        let all_keys_size: usize = self.index_keys.len() * self.key_size as usize;
        let all_values_size: usize = self.index_values.len() * size_of::<InternalIndexValue>();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + size_of::<InternalIndexValue>() <= PAGE_SIZE {
            return true;
        }
        false
    }
}