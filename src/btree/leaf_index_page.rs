use std::collections::HashMap;
use super::btree::*;
use crate::fileio::{header::*, pageio::*};

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | IndexKey | IndexValue | IndexKey | ... | IndexValue |
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

        Ok(LeafIndexPage {
            pagenum,
            indexes: HashMap::new(),
            index_key_type,
            cols_used_in_index,
            key_size,
        })
    }
}