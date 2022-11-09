use std::{collections::HashMap, mem::size_of};
use itertools::Itertools;

use super::indexes::*;
use crate::{fileio::{header::*, pageio::*, tableio::Table}, util::{row::*, dbtype::Value}};

const LEAF_PAGE_HEADER_SIZE: usize = size_of::<u16>();

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | NumValues<i16> | IndexValue | IndexKey | IndexValue | ... | IndexKey |
/// where there is an equal number of index values and index keys.
#[derive(Debug, Clone)]
pub struct LeafIndexPage {
    table_path: String,                         // The path to the table this page belongs to
    pagenum: u32,                               // The page number of this page
    indexes: HashMap<IndexKey, LeafIndexValue>, // The values in this page
    index_key_type: IndexKeyType,               // The type of the index keys
    index_id: IndexID,                          // The columns used in this index
    key_size: u8,                               // The size of an individual member of index_keys
    page: Page,                                 // The page data
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
            indexes: HashMap::new(),
            index_key_type: index_key_type.clone(),
            index_id: index_id.clone(),
            key_size,
            page,
        })
    }

    /// Writes the page to disk at the specified page number.
    pub fn write_page(
        &self
    ) -> Result<(), String> {
        write_page(self.pagenum, &self.table_path, &self.page, PageType::Index)?;
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
        let index_key_type: IndexKeyType = get_index_key_type(&index_key);
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
                Self::write_index_key(&index_key, &index_key_type, &mut self.page, i)?;
                Self::write_index_value(&index_value, &index_key_type, &mut self.page, i)?;

                // Write the rest of the keys and values that come after index i
                for (j, (key, value)) in self.indexes.clone().iter().enumerate().sorted().skip(i) {
                    Self::write_index_key(&key, &index_key_type, &mut self.page, j + 1)?;
                    Self::write_index_value(&value, &index_key_type, &mut self.page, j + 1)?;
                }

                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if idx_to_insert.is_none() {
            Self::write_index_key(&index_key, &index_key_type, &mut self.page, self.indexes.len())?;
            Self::write_index_value(&index_value, &index_key_type, &mut self.page, self.indexes.len())?;
        }

        // Insert the key into the hashmap
        self.indexes.insert(index_key.clone(), index_value.clone());

        Ok(true)
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
}