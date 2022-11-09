use std::mem::size_of;

use itertools::Itertools;

use super::indexes::*;
use crate::{fileio::{header::*, pageio::*, rowio::write_row, tableio::Table}, util::dbtype::Column};

const INTERNAL_PAGE_HEADER_SIZE: usize = size_of::<u16>();

/// This represents an index page for the btree.
/// It is formatted on disk like so:
/// | NumValues<i16> | IndexValue | IndexKey | IndexValue | IndexKey | ... | IndexValue |
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
            index_values: Vec::new(),
            index_id: index_id.clone(),
            page_depth: node_depth_in_tree,
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
        
        let index_key_type: IndexKeyType = get_index_key_type(&index_key);

        // Find the index where we need to insert the key. Locate the first index where the key is greater than the index_key we're inserting.
        let mut idx_to_insert: Option<usize> = None;
        for (i, key) in self.index_keys.clone().iter().enumerate() {
            if compare_indexes(&key, &index_key) == KeyComparison::Greater {
                idx_to_insert = Some(i);

                // Insert the key at the index in the page
                Self::write_index_key(&index_key, &index_key_type, &mut self.page, i)?;
                Self::write_index_value(&index_value, &index_key_type, &mut self.page, i + 1)?;

                // Write the rest of the keys and values that come after index i
                for (j, (key, value)) in self.index_keys.clone().iter().zip(self.index_values.iter().skip(i + 1)).enumerate().skip(i) {
                    Self::write_index_key(&key, &index_key_type, &mut self.page, j + 1)?;
                    Self::write_index_value(&value, &index_key_type, &mut self.page, j + 2)?;
                }

                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if idx_to_insert.is_none() {
            idx_to_insert = Some(self.index_keys.len());
            Self::write_index_key(&index_key, &index_key_type, &mut self.page, self.index_keys.len())?;
            Self::write_index_value(&index_value, &index_key_type, &mut self.page, self.index_keys.len() + 1)?;
        }

        // Insert the key into the hashmap
        self.index_keys.insert(idx_to_insert.unwrap(), index_key.clone());
        self.index_values.insert(idx_to_insert.unwrap() + 1, index_value.clone());

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
                            ((index_idx + 1) * InternalIndexValue::size()) +        // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE;                              // The offset of the page header
        write_index_key_at_offset(index_key, index_key_type, page, offset)
    }

    /// Writes an index key to a page at a specific offset
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
}