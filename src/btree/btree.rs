use crate::fileio::pageio::*;
use crate::fileio::rowio::*;
use crate::fileio::tableio::*;
use crate::util::dbtype::*;
use crate::fileio::header::*;
use crate::util::row::*;
use super::indexes::*;
use super::leaf_index_page::*;
use super::internal_index_page::*;

pub struct BTree {
    index_key_type: IndexKeyType, // The type of the index keys
}

impl BTree {
    /// Create an index on one or more columns
    pub fn create_btree_index(
        table_dir: &String,
        table_name: &String,
        table_extension: Option<&String>, // Optionally specify a file extension. Defaults to TABLE_FILE_EXTENSION.
        columns: Vec<String>
    ) -> Result<Self, String> {
        let mut table: Table = Table::new(table_dir, table_name, table_extension)?;
        
        // Get the index key composed of those column names
        let index_id: IndexID = col_names_to_index_id(&columns, &table.schema)?;
        let index_key_type: IndexKeyType = cols_id_to_index_key_type(&index_id, &table.schema);

        // Check that the index doesn't already exist
        if table.indexes.contains_key(&index_id) {
            return Err(format!("Index already exists on columns: {:?}", columns));
        }

        // Get the first empty page to use for the index
        //let index_pagenum: u32 = table.max_pages;
        /*
        let new_page: Page = [0; PAGE_SIZE];
        table.max_pages += 1;
        write_page(index_pagenum, &table.path, &new_page, PageType::Index)?;
        */

        // Keep track of the index's top level table page
        //table.indexes.insert(index_id.clone(), index_pagenum);

        // Get the rows in the table
        let mut table_rows: Vec<RowInfo> = Vec::new();
        for rowinfo in table.clone() {
            table_rows.push(rowinfo);
        }
        let num_rows: usize = table_rows.len();
        // Sort all the rows using the index key
        table_rows.sort_by(|a, b| compare_rows_using_index_id(&a.row, &b.row, &index_id));

        // The number of pages per level is built from the bottom up.
        // The number of pages at the bottom level (idx 0) is the number of leaf pages.
        let mut num_pages_per_level: Vec<u32> = Vec::new();

        // Calculate how many rows pointers we can fit on a single leaf page
        let max_pointers_per_leaf: usize = LeafIndexPage::get_max_index_pointers_per_page(&index_key_type);

        // Calculate how many leaf pages we need to store all the rows
        // Double it because we want to keep the leaves between 50% and 100% full
        let num_leaf_pages: u32 = ((num_rows as f64 / max_pointers_per_leaf as f64).ceil() as u32) * 2;
        num_pages_per_level.push(num_leaf_pages);

        // Calculate how many pointers we can fit on a single internal page
        let max_pointers_per_internal: usize = InternalIndexPage::get_max_index_pointers_per_page(&index_key_type);

        // Calculate how many internal nodes we need to point to all the leaf pages
        let mut pages_on_level_below: u32 = num_leaf_pages;
        loop {
            // Divide the max_pointers_per_internal by 2 because we want to keep the internal nodes between 50% and 100% full 
            let num_internal_pages_for_level: u32 = (pages_on_level_below as f64 / (max_pointers_per_internal as f64 / 2 as f64)).ceil() as u32;
            num_pages_per_level.push(num_internal_pages_for_level);
            if num_internal_pages_for_level == 1 {
                break;
            }
            pages_on_level_below = num_internal_pages_for_level;
        }

        // Create the leaf pages
        let mut leaf_pages: Vec<LeafIndexPage> = Vec::new();
        for _ in 0..num_leaf_pages {
            let leaf_page: LeafIndexPage = LeafIndexPage::new(
                table.path.clone(), 
                table.max_pages, 
                &index_id, 
                &index_key_type
            )?;
            table.max_pages += 1;
            leaf_pages.push(leaf_page);
        }

        // Fill the leaf pages with the rows
        let mut leaf_page_idx: usize = 0;
        let mut rows_in_leaf_page: usize = 0;
        for row in table_rows {
            leaf_pages[leaf_page_idx].add_pointer_to_row(&row)?;
            rows_in_leaf_page += 1;
            if rows_in_leaf_page > (max_pointers_per_leaf / 2) {
                // Advance the leaf page idx up until the last leaf page
                if leaf_page_idx < (leaf_pages.len() - 1) {
                    leaf_page_idx += 1;
                }
                rows_in_leaf_page = 0;
            }
        }

        // Write the leaf pages to disk
        for leaf_page in leaf_pages.clone() {
            leaf_page.write_page()?;
        }

        // For each leaf page, get the page number, and the lowest value IndexKey in the page
        let mut pages_on_level_below: Vec<(u32, IndexKey)> = Vec::new();
        for leaf_page in leaf_pages {
            if let Some(smallest_key) = leaf_page.get_lowest_index_key() {
                pages_on_level_below.push((leaf_page.get_pagenum(), smallest_key));
            }
            else {
                return Err(format!("Leaf page {} has no index keys", leaf_page.get_pagenum()));
            }
        }

        // Create the internal pages for each level starting from the bottom
        let mut internal_pages: Vec<Vec<InternalIndexPage>> = Vec::new();
        for level in 1..num_pages_per_level.len() {
            let mut internal_pages_for_level: Vec<InternalIndexPage> = Vec::new();
            let mut internal_page_idx: usize = 0;
            let mut num_pointers_in_page: Option<usize> = None;
            // Insert each page on the level below into an internal page within this level
            for (page_below, page_below_lowest_key) in pages_on_level_below {
                // Create a new internal page if we need to
                if num_pointers_in_page.is_none() {
                    let internal_page: InternalIndexPage = InternalIndexPage::new(
                        table.path.clone(), 
                        table.max_pages, 
                        &index_id, 
                        &index_key_type,
                        &InternalIndexValue { pagenum: page_below },
                        (level + 1) as u8
                    )?;
                    table.max_pages += 1;
                    num_pointers_in_page = Some(1);
                    internal_pages_for_level.push(internal_page);
                }
                // Insert the page on the level below into the current internal page
                else {
                    internal_pages_for_level[internal_page_idx].add_pointer_to_page(&page_below_lowest_key, &InternalIndexValue { pagenum: page_below })?;
                    num_pointers_in_page = Some(num_pointers_in_page.unwrap() + 1);

                    // If the internal page is bet, advance to the next one
                    if num_pointers_in_page.unwrap() > (max_pointers_per_internal / 2) {
                        num_pointers_in_page = None;
                        internal_page_idx += 1;
                        // Advance the leaf page idx up until the last leaf page
                        if internal_page_idx < (num_pages_per_level[level] - 1) as usize {
                            internal_page_idx += 1;
                        }
                    }
                }
            }
            // We should have filled part of every page on this level
            assert_eq!(internal_page_idx, num_pages_per_level[level] as usize);

            // Update pages_on_level_below with the pages on this level
            pages_on_level_below = Vec::new();
            for internal_page in internal_pages_for_level.clone() {
                if let Some(smallest_key) = internal_page.get_lowest_index_key() {
                    pages_on_level_below.push((internal_page.get_pagenum(), smallest_key));
                }
                else {
                    return Err(format!("Internal page {} has no index keys", internal_page.get_pagenum()));
                }
            }

            internal_pages.push(internal_pages_for_level);
        }

        // Write the internal pages to disk
        for internal_pages_for_level in internal_pages.clone() {
            for internal_page in internal_pages_for_level {
                internal_page.write_page()?;
            }
        }


        // Update the header
        let new_header: Header = Header {
            num_pages: table.max_pages,
            schema: table.schema.clone(),
            index_top_level_pages: table.indexes.clone(),
        };
        write_header(&table.path, &new_header)?;

        Ok(BTree {
            index_key_type,
        })
    }
}