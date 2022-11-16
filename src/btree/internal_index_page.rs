use std::{collections::HashSet, mem::size_of};

use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};

use super::{btree::BTree, indexes::*, leaf_index_page::LeafIndexPage};
use crate::{
    executor::{predicate::*, query::*},
    fileio::{header::*, pageio::*, rowio::*, tableio::Table},
    util::{dbtype::*, row::*},
};

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
    table_path: String,           // The path to the table this page belongs to
    table_name: String,           // The name of the table this page belongs to
    table_schema: Schema,         // The schema of the table this page belongs to
    pagenum: u32,                 // The page number of this page
    index_keys: Vec<IndexKey>,    // The keys in this page
    index_key_type: IndexKeyType, // The type of the index keys
    index_values: Vec<InternalIndexValue>, // The values in this page
    index_id: IndexID,            // The columns used in this index
    page_depth: u8,               // The depth of this page in the btree. (0 is a leaf page)
    key_size: u8,                 // The size of an individual member of index_keys
    page: Page,                   // The page data
}

impl InternalIndexPage {
    /// Creates a new internal index page.
    pub fn new(
        table: &Table,
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
            table_path: table.path.clone(),
            table_name: table.name.clone(),
            table_schema: table.schema.clone(),
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
        table_name: String,
        table_schema: Schema,
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
            let index_value: InternalIndexValue =
                Self::read_index_value(&index_key_type, &page, i as usize)?;
            values.push(index_value);
        }

        Ok(InternalIndexPage {
            table_path,
            table_name,
            table_schema,
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
    pub fn write_page(&mut self) -> Result<(), String> {
        write_type::<u16>(&mut self.page, 0, self.index_values.len() as u16)?;
        write_type::<u8>(&mut self.page, size_of::<u16>(), self.page_depth as u8)?;
        write_page(
            self.pagenum,
            &self.table_path,
            &self.page,
            PageType::InternalIndex,
        )?;
        Ok(())
    }

    /// Gets the page number of the page where this page is stored
    pub fn get_pagenum(&self) -> u32 {
        self.pagenum
    }

    /// Gets the lowest valued index key in the page.
    pub fn get_lowest_index_key(&self) -> Option<IndexKey> {
        for key in self.index_keys.clone() {
            return Some(key.clone());
        }
        None
    }

    /// Inserts a row into the leaf page following all the pointers from this page down to the leaves.
    pub fn insert_row(&mut self, rowinfo: &RowInfo, index_name: String) -> Result<(), String> {
        let index_key: IndexKey = get_index_key_from_row(&rowinfo.row, &self.index_id);
        let mut leaf_page: LeafIndexPage = self.get_leaf_page_for_key(index_key)?;

        // Insert the row into the leaf page
        leaf_page.add_pointer_to_row(rowinfo)?;

        // Write the leaf page back to disk
        leaf_page.write_page()?;

        // Check if the leaf page is full or empty
        if !leaf_page.has_room() {
            // Rebalance the B-Tree
            self.rebalance(index_name)?;
        }

        Ok(())
    }

    /// Removes a row from the leaf page following all the pointers from this page down to the leaves.
    pub fn remove_row(&mut self, rowinfo: &RowInfo, index_name: String) -> Result<(), String> {
        let index_key: IndexKey = get_index_key_from_row(&rowinfo.row, &self.index_id);
        let mut leaf_page: LeafIndexPage = self.get_leaf_page_for_key(index_key)?;

        // Remove the row from the leaf page
        leaf_page.remove_pointer_to_row(rowinfo)?;

        // Write the leaf page back to disk
        leaf_page.write_page()?;

        // Check if the leaf page is full or empty
        if leaf_page.is_empty() {
            // Rebalance the B-Tree
            self.rebalance(index_name)?;
        }

        Ok(())
    }

    /// Gets the leaf page that either contains the specified key,
    /// or the page that would contain the key if it were inserted.
    fn get_leaf_page_for_key(&self, index_key: IndexKey) -> Result<LeafIndexPage, String> {
        let mut last_internal_page: InternalIndexPage = self.clone();

        for _ in 1..self.page_depth {
            // Find the index where the key should be inserted
            let mut index: usize = 0;
            for key in &last_internal_page.index_keys {
                if index_key < *key {
                    break;
                }
                index += 1;
            }
            let next_internal_pagenum: u32 = last_internal_page.index_values[index].pagenum;

            let next_internal_index_page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                self.table_name.clone(),
                self.table_schema.clone(),
                next_internal_pagenum,
                &self.index_id,
                &self.index_key_type,
            )?;

            last_internal_page = next_internal_index_page;
        }

        // Find the index where the key should be inserted
        let mut index: usize = 0;
        for key in &last_internal_page.index_keys {
            if index_key < *key {
                break;
            }
            index += 1;
        }
        let leaf_pagenum: u32 = last_internal_page.index_values[index].pagenum;

        // Get the leaf page
        LeafIndexPage::load_from_table(
            self.table_path.clone(),
            leaf_pagenum,
            &self.index_id,
            &self.index_key_type,
        )
    }

    /// Inserts a page pointer into the page.
    /// Returns whether the value was inserted or whether the page is full.
    pub fn add_pointer_to_page(
        &mut self,
        index_key: &IndexKey,
        index_value: &InternalIndexValue,
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
                for (j, (key, value)) in self
                    .index_keys
                    .clone()
                    .iter()
                    .zip(self.index_values.iter().skip(i + 1))
                    .enumerate()
                    .skip(i)
                {
                    Self::write_index_key(&key, &self.index_key_type, &mut self.page, j + 1)?;
                    Self::write_index_value(&value, &self.index_key_type, &mut self.page, j + 2)?;
                }

                break;
            }
        }
        // If we didn't insert in the middle of the page, then we need to insert at the end
        if idx_to_insert.is_none() {
            idx_to_insert = Some(self.index_keys.len());
            Self::write_index_key(
                &index_key,
                &self.index_key_type,
                &mut self.page,
                self.index_keys.len(),
            )?;
            Self::write_index_value(
                &index_value,
                &self.index_key_type,
                &mut self.page,
                self.index_keys.len() + 1,
            )?;
        }

        let vector_index_to_insert: usize = idx_to_insert.unwrap();

        // Insert the key into the hashmap
        self.index_keys
            .insert(vector_index_to_insert, index_key.clone());
        self.index_values
            .insert(vector_index_to_insert + 1, index_value.clone());

        Ok(true)
    }

    /// Gets the rows that match the specified expression.
    pub fn get_rows_matching_expr(&self, expr: &Expr) -> Result<Vec<RowInfo>, String> {
        // Get the leaf page numbers we need to search
        let column_aliases: ColumnAliases = gen_column_aliases_from_schema(&vec![(
            self.table_schema.clone(),
            self.table_name.clone(),
        )]);
        let index_refs: IndexRefs = get_index_refs(&column_aliases);
        let leaf_pagenums: HashSet<u32> =
            self.get_leaf_pagenums_matching_expr(&expr, &column_aliases, &index_refs)?;

        let mut leaf_col_aliases: ColumnAliases = Vec::new();
        for (i, col_alias) in column_aliases.iter().enumerate() {
            if self.index_id.contains(&(i as u8)) {
                leaf_col_aliases.push(col_alias.clone());
            }
        }
        let leaf_idx_refs: IndexRefs = get_index_refs(&leaf_col_aliases);

        let leaf_pred_solver: PredicateSolver =
            solve_predicate(&expr, &leaf_col_aliases, &leaf_idx_refs)?;

        // Get all the row locations we need to read the rows from
        let mut row_locations: Vec<RowLocation> = Vec::new();
        for leaf_pagenum in leaf_pagenums {
            let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
                self.table_path.clone(),
                leaf_pagenum,
                &self.index_id,
                &self.index_key_type,
            )?;
            let leaf_row_locations: Vec<RowLocation> =
                leaf_page.get_row_locations_using_pred_solver(&leaf_pred_solver)?;
            row_locations.extend(leaf_row_locations);
        }

        // Sort the row_locations by pagenum so we reduce the number of page reads we need
        row_locations.sort_by(|a, b| a.pagenum.cmp(&b.pagenum));

        // Read the rows from the row locations
        Ok(self.read_rowinfos_from_locations(&row_locations)?)
    }

    /// Gets the rows that are stored from the specific index key
    pub fn get_rows_from_key(&self, index_key: &IndexKey) -> Result<Vec<RowInfo>, String> {
        let leaf_pagenums: HashSet<u32> = self.get_leaf_pagenums_from_key(index_key)?;

        // Get all the row locations we need to read the rows from
        let mut row_locations: Vec<RowLocation> = Vec::new();
        for leaf_pagenum in leaf_pagenums {
            let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
                self.table_path.clone(),
                leaf_pagenum,
                &self.index_id,
                &self.index_key_type,
            )?;
            let leaf_row_locations: Vec<RowLocation> =
                leaf_page.get_row_locations_from_key(index_key)?;
            row_locations.extend(leaf_row_locations);
        }

        // Sort the row_locations by pagenum so we reduce the number of page reads we need
        row_locations.sort_by(|a, b| a.pagenum.cmp(&b.pagenum));

        // Read the rows from the row locations
        Ok(self.read_rowinfos_from_locations(&row_locations)?)
    }

    /***********************************************************************************************/
    /*                                       Public Static Methods                                 */
    /***********************************************************************************************/

    /// Gets the maximum number of index value pointers that can fit on a page.
    /// i.e. the number of data rows that a leaf index page can point to.
    pub fn get_max_index_pointers_per_page(index_key_type: &IndexKeyType) -> usize {
        let idx_and_value_size: usize =
            get_index_key_type_size(index_key_type) + InternalIndexValue::size();
        let num_idx_val_pairs: usize =
            (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE - InternalIndexValue::size())
                / idx_and_value_size;
        num_idx_val_pairs + 1
    }

    /***********************************************************************************************/
    /*                                       Private Member Methods                                */
    /***********************************************************************************************/

    /// Returns true if there is room for another index and value in this page.
    fn has_room(&self) -> bool {
        let all_keys_size: usize = self.index_keys.len() * self.key_size as usize;
        let all_values_size: usize = self.index_values.len() * InternalIndexValue::size();
        let combined_size: usize = all_keys_size + all_values_size;

        // If we have room for another key and value, return true
        if combined_size + self.key_size as usize + InternalIndexValue::size()
            <= (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE)
        {
            return true;
        }
        false
    }

    /// Rebalances the tree below this page.
    /// It balances the tree by keeping all the pages non-empty and non-full.
    fn rebalance(&mut self, index_name: String) -> Result<(), String> {
        // We need to go through each of the leaf pages.
        // If the leaf page is empty, we need to combine its values with another page.
        // If the leaf page is full, we need to split the page into two pages.
        // If the leaf page is neither empty nor full, we do nothing.

        let mut last_internal_pagenums: HashSet<u32> = HashSet::new();
        last_internal_pagenums.insert(self.pagenum);
        for _ in 1..self.page_depth {
            let curr_internal_pagenums: HashSet<u32> = last_internal_pagenums.clone();
            last_internal_pagenums.clear();

            // Load the internal pages
            for i_pagenum in curr_internal_pagenums {
                let i_page: InternalIndexPage = InternalIndexPage::load_from_table(
                    self.table_path.clone(),
                    self.table_name.clone(),
                    self.table_schema.clone(),
                    i_pagenum,
                    &self.index_id,
                    &self.index_key_type,
                )?;

                // Go through each of the internal index values
                for i_value in i_page.index_values {
                    last_internal_pagenums.insert(i_value.pagenum);
                }
            }
        }

        // Get the leaf pages
        let mut leaf_pagenums: HashSet<u32> = HashSet::new();
        for i_pagenum in last_internal_pagenums {
            let i_page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                self.table_name.clone(),
                self.table_schema.clone(),
                i_pagenum,
                &self.index_id,
                &self.index_key_type,
            )?;

            // Go through each of the internal index values
            for i_value in i_page.index_values {
                leaf_pagenums.insert(i_value.pagenum);
            }
        }

        // Get all the leaf index key-value pairs from the leaf pages
        let mut leaf_index_values: Vec<(IndexKey, LeafIndexValue)> = Vec::new();
        for leaf_pagenum in leaf_pagenums {
            let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
                self.table_path.clone(),
                leaf_pagenum,
                &self.index_id,
                &self.index_key_type,
            )?;
            leaf_index_values.extend(leaf_page.get_all_key_values());
        }

        // Sort the leaf index key-value pairs
        leaf_index_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Recreate the new pages
        let new_root: Self = BTree::create_pages_for_btree(
            &mut Table::new_from_path(self.table_path.clone(), self.table_name.clone())?,
            leaf_index_values,
            &self.index_id,
            &self.index_key_type,
            index_name,
        )?;

        // Make self the new root
        self.index_keys = new_root.index_keys;
        self.page_depth = new_root.page_depth;
        self.index_values = new_root.index_values;
        self.pagenum = new_root.pagenum;
        self.page = new_root.page;

        Ok(())
    }

    /// Gets the row info for the rows that match the given expression.
    fn get_leaf_pagenums_matching_expr(
        &self,
        pred: &Expr,
        column_aliases: &ColumnAliases,
        index_refs: &IndexRefs,
    ) -> Result<HashSet<u32>, String> {
        let pred_solver: PredicateSolver =
            Self::solve_index_predicate(pred, column_aliases, index_refs, &self.index_id)?;

        let mut lowest_internal_pages: HashSet<u32> = HashSet::new();

        // Iterate through each level of index pages to get the lowest level internal pages
        lowest_internal_pages.insert(self.pagenum);
        for _ in 1..self.page_depth {
            let current_page_numbers: HashSet<u32> = lowest_internal_pages.clone();
            lowest_internal_pages = HashSet::new();

            for page_number in current_page_numbers {
                let page: InternalIndexPage = InternalIndexPage::load_from_table(
                    self.table_path.clone(),
                    self.table_name.clone(),
                    self.table_schema.clone(),
                    page_number,
                    &self.index_id,
                    &self.index_key_type,
                )
                .unwrap();
                let values_from_key: HashSet<InternalIndexValue> =
                    page.get_values_using_pred_solver(&pred_solver)?;
                let pages_below_this_page: Vec<u32> =
                    values_from_key.iter().map(|value| value.pagenum).collect();

                lowest_internal_pages.extend(pages_below_this_page);
            }
        }

        // Get the leaf pages from the lowest level internal pages
        let mut leaf_pages: HashSet<u32> = HashSet::new();
        for page_number in lowest_internal_pages {
            let page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                self.table_name.clone(),
                self.table_schema.clone(),
                page_number,
                &self.index_id,
                &self.index_key_type,
            )
            .unwrap();
            let values_from_key: HashSet<InternalIndexValue> =
                page.get_values_using_pred_solver(&pred_solver)?;
            let leaf_pages_below_this_page: Vec<u32> =
                values_from_key.iter().map(|value| value.pagenum).collect();

            leaf_pages.extend(leaf_pages_below_this_page);
        }

        Ok(leaf_pages)
    }

    fn get_values_using_pred_solver(
        &self,
        pred_solver: &PredicateSolver,
    ) -> Result<HashSet<InternalIndexValue>, String> {
        let mut values: HashSet<InternalIndexValue> = HashSet::new();

        for (i, index_key) in self.index_keys.iter().enumerate() {
            if pred_solver(index_key)? {
                values.insert(self.index_values[i].clone());
                values.insert(self.index_values[i + 1].clone());
            }
        }

        // If we don't have any values, we need to check the last value
        if values.len() == 0 {
            // Get the last index key following all the rightmost pointers from index pages below this one
            let mut lowest_internal_pagenum: u32 = self.pagenum;
            for _ in 1..self.page_depth {
                let lowest_page: InternalIndexPage = InternalIndexPage::load_from_table(
                    self.table_path.clone(),
                    self.table_name.clone(),
                    self.table_schema.clone(),
                    lowest_internal_pagenum,
                    &self.index_id,
                    &self.index_key_type,
                )
                .unwrap();
                lowest_internal_pagenum = lowest_page.index_values.last().unwrap().pagenum;
            }

            // Read the lowest internal page
            let lowest_internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                self.table_name.clone(),
                self.table_schema.clone(),
                lowest_internal_pagenum,
                &self.index_id,
                &self.index_key_type,
            )
            .unwrap();

            // Get the leaf page from the lowest internal page
            let leaf_page: LeafIndexPage = LeafIndexPage::load_from_table(
                self.table_path.clone(),
                lowest_internal_page.index_values.last().unwrap().pagenum,
                &self.index_id,
                &self.index_key_type,
            )
            .unwrap();

            let rightmost_index_key: Option<IndexKey> = leaf_page.get_largest_index_key();

            if rightmost_index_key.is_some() && pred_solver(&rightmost_index_key.unwrap())? {
                values.insert(self.index_values.last().unwrap().clone());
            }
        }

        Ok(values)
    }

    /// Reads the rowinfos from teh table from the given row locations
    fn read_rowinfos_from_locations(
        &self,
        row_locations: &Vec<RowLocation>,
    ) -> Result<Vec<RowInfo>, String> {
        // Read the rows from the row locations
        let mut rows: Vec<RowInfo> = Vec::new();
        let mut current_pagenum: u32 = 0;
        let mut current_page: Option<Page> = None;
        for row_location in row_locations {
            if current_pagenum != row_location.pagenum || current_page.is_none() {
                current_pagenum = row_location.pagenum;
                let (page, page_type) = read_page(current_pagenum, &self.table_path)?;
                if page_type != PageType::Data {
                    return Err(format!(
                        "Expected page type to be Data, but got {:?}",
                        page_type
                    ));
                }
                current_page = Some(*page);
            }
            let row: Option<Row> = read_row(
                &self.table_schema,
                &current_page.unwrap(),
                row_location.rownum,
            );
            if let Some(row_value) = row {
                rows.push(RowInfo {
                    row: row_value,
                    pagenum: row_location.pagenum,
                    rownum: row_location.rownum,
                });
            } else {
                return Err(format!(
                    "Could not read row from page {} row {}",
                    row_location.pagenum, row_location.rownum
                ));
            }
        }

        Ok(rows)
    }

    /// We know a lot of information already about the expression, so we can 'reduce' it
    /// into just a function that takes a row and outputs true or false. This way, we don't
    /// have to re-parse the function every time, and we have a direct function to call
    /// when we need to filter rows.
    /// Currently, this is implemented recursively, see if we can do it iteratively
    fn solve_index_predicate(
        pred: &Expr,
        column_aliases: &ColumnAliases,
        index_refs: &IndexRefs,
        index_id: &IndexID,
    ) -> Result<PredicateSolver, String> {
        match pred {
            Expr::Identifier(_) => {
                let solve_value =
                    Self::solve_index_value(pred, column_aliases, index_refs, index_id)?;
                Ok(Box::new(move |row| {
                    // Figure out the whether the value of the column cell is a boolean or not.
                    let value = solve_value(row)?;
                    match value {
                        JointValues::DBValue(Value::Bool(x)) => Ok(x),
                        JointValues::SQLValue(SqlValue::Boolean(x)) => Ok(x),
                        _ => Err(format!("Cannot compare value {:?} to bool", value)),
                    }
                }))
            }
            Expr::IsFalse(pred) => {
                let pred = Self::solve_index_predicate(pred, column_aliases, index_refs, index_id)?;
                Ok(Box::new(move |row| Ok(!pred(row)?)))
            }
            Expr::IsNotFalse(pred) => {
                Self::solve_index_predicate(pred, column_aliases, index_refs, index_id)
            }
            Expr::IsTrue(pred) => {
                Self::solve_index_predicate(pred, column_aliases, index_refs, index_id)
            }
            Expr::IsNotTrue(pred) => {
                let pred = Self::solve_index_predicate(pred, column_aliases, index_refs, index_id)?;
                Ok(Box::new(move |row| Ok(!pred(row)?)))
            }
            Expr::IsNull(pred) => {
                let pred = Self::solve_index_value(pred, column_aliases, index_refs, index_id)?;
                Ok(Box::new(move |row| match pred(row)? {
                    JointValues::DBValue(Value::Null(_)) => Ok(true),
                    JointValues::SQLValue(SqlValue::Null) => Ok(true),
                    _ => Ok(false),
                }))
            }
            Expr::IsNotNull(pred) => {
                let pred = Self::solve_index_value(pred, column_aliases, index_refs, index_id)?;
                Ok(Box::new(move |row| match pred(row)? {
                    JointValues::DBValue(Value::Null(_)) => Ok(false),
                    JointValues::SQLValue(SqlValue::Null) => Ok(false),
                    _ => Ok(true),
                }))
            }
            Expr::BinaryOp { left, op, right } => match op {
                // Resolve values from the two sides of the expression, and then perform
                // the comparison on the two values
                BinaryOperator::Gt => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        Ok(left.gt(&right))
                    }))
                }
                BinaryOperator::Lt => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        Ok(left.lt(&right))
                    }))
                }
                BinaryOperator::GtEq => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        Ok(left.ge(&right))
                    }))
                }
                BinaryOperator::LtEq => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        Ok(left.le(&right))
                    }))
                }
                BinaryOperator::Eq => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        Ok(left.ge(&right))
                    }))
                }
                BinaryOperator::NotEq => Ok(Box::new(move |_| Ok(true))),
                // Create functions for the LHS and RHS of the 'and' operation, and then
                // combine them into a single function that returns true if both functions return true
                // Note how this would also indirectly handle short-circuiting
                BinaryOperator::And => {
                    let left =
                        Self::solve_index_predicate(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_predicate(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| Ok(left(row)? && right(row)?)))
                }
                BinaryOperator::Or => {
                    let left =
                        Self::solve_index_predicate(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_predicate(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| Ok(left(row)? || right(row)?)))
                }
                _ => Err(format!("Unsupported binary operator for Predicate: {}", op)),
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => {
                    let expr =
                        Self::solve_index_predicate(expr, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| Ok(!expr(row)?)))
                }
                _ => Err(format!("Unsupported unary operator for Predicate: {}", op)),
            },
            Expr::Nested(pred) => {
                Self::solve_index_predicate(pred, column_aliases, index_refs, index_id)
            }
            _ => Err(format!("Invalid Predicate Clause: {}", pred)),
        }
    }

    /// Similar to solve_index_predicate, this is another function that takes a Row and reduces it to the
    /// value described by the expression. In the most simple case, if we have an Expression just
    /// referencing a column name, we just take a row and then apply the index on that row.
    /// The main difference between this and solve_index_predicate is that we can return a Value, instead of
    /// a boolean.
    fn solve_index_value(
        expr: &Expr,
        column_aliases: &ColumnAliases,
        index_refs: &IndexRefs,
        index_id: &IndexID,
    ) -> Result<ValueSolver, String> {
        match expr {
            // This would mean that we're referencing a column name, so we just need to figure out the
            // index of that column name in the row, and then return a function that references this index
            // in the provided row.
            Expr::Identifier(x) => {
                let x: String = resolve_reference(x.value.to_string(), column_aliases)?;
                let index: usize = *index_refs
                    .get(&x)
                    .ok_or(format!("Column {} does not exist in the table", x))?;

                // Get the index of the index within the index key type
                let key_index: usize = index_id
                    .iter()
                    .position(|id| index == (*id as usize))
                    .unwrap();

                // Force the closure to take `index` ownership (the index value is copied into the function below)
                // Then, create a closure that takes in a row and returns the value at the index
                Ok(Box::new(move |row: &Row| {
                    Ok(JointValues::DBValue(row[key_index].clone()))
                }))
            }
            Expr::CompoundIdentifier(list) => {
                // Join all the identifiers in the list with a dot, perform the same step as above
                let x = resolve_reference(
                    list.iter()
                        .map(|x| x.value.to_string())
                        .collect::<Vec<String>>()
                        .join("."),
                    column_aliases,
                )?;
                let index = *index_refs
                    .get(&x)
                    .ok_or(format!("Column {} does not exist in the table", x))?;

                // Get the index of the index within the index key type
                let key_index: usize = index_id
                    .iter()
                    .position(|id| index == (*id as usize))
                    .unwrap();

                Ok(Box::new(move |row: &Row| {
                    Ok(JointValues::DBValue(row[key_index].clone()))
                }))
            }
            Expr::Nested(x) => Self::solve_index_value(x, column_aliases, index_refs, index_id),
            Expr::Value(x) => {
                // Create a copy of the value
                let val = x.clone();
                // Move a reference of this value into the closure, so that we can reference
                // it when we wish to respond with a Value.
                Ok(Box::new(move |_| Ok(JointValues::SQLValue(val.clone()))))
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Plus => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        left.add(&right)
                    }))
                }
                BinaryOperator::Minus => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        left.subtract(&right)
                    }))
                }
                BinaryOperator::Multiply => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        left.multiply(&right)
                    }))
                }
                BinaryOperator::Divide => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        left.divide(&right)
                    }))
                }
                BinaryOperator::Modulo => {
                    let left = Self::solve_index_value(left, column_aliases, index_refs, index_id)?;
                    let right =
                        Self::solve_index_value(right, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let left = left(row)?;
                        let right = right(row)?;
                        left.modulo(&right)
                    }))
                }
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq => {
                    let binary =
                        Self::solve_index_predicate(expr, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let pred = binary(row)?;
                        Ok(JointValues::DBValue(Value::Bool(pred)))
                    }))
                }
                _ => Err(format!("Invalid Binary Operator for Value: {}", op)),
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => {
                    let expr = Self::solve_index_value(expr, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| expr(row)))
                }
                UnaryOperator::Minus => {
                    let expr = Self::solve_index_value(expr, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let val = expr(row)?;
                        JointValues::DBValue(Value::I32(0)).subtract(&val)
                    }))
                }
                UnaryOperator::Not => {
                    // Solve the inner value, expecting it's return type to be a boolean, and negate it.
                    let binary =
                        Self::solve_index_predicate(expr, column_aliases, index_refs, index_id)?;
                    Ok(Box::new(move |row| {
                        let pred = binary(row)?;
                        Ok(JointValues::DBValue(Value::Bool(!pred)))
                    }))
                }
                _ => Err(format!("Invalid Unary Operator for Value: {}", op)),
            },
            _ => Err(format!("Unexpected Value Clause: {}", expr)),
        }
    }

    /// Gets the leaf page numbers that correspond to the index key provided.
    /// Traverses all the index pages down to the leaf pages.
    fn get_leaf_pagenums_from_key(&self, index_key: &IndexKey) -> Result<HashSet<u32>, String> {
        let mut lowest_internal_pages: HashSet<u32> = HashSet::new();

        // Iterate through each level of index pages to get the lowest level internal pages
        lowest_internal_pages.insert(self.pagenum);
        for _ in 1..self.page_depth {
            let current_page_numbers: HashSet<u32> = lowest_internal_pages.clone();
            lowest_internal_pages = HashSet::new();

            for page_number in current_page_numbers {
                let page: InternalIndexPage = InternalIndexPage::load_from_table(
                    self.table_path.clone(),
                    self.table_name.clone(),
                    self.table_schema.clone(),
                    page_number,
                    &self.index_id,
                    &self.index_key_type,
                )
                .unwrap();
                let values_from_key: Vec<InternalIndexValue> =
                    page.get_index_values_from_key(index_key)?;
                let pages_below_this_page: Vec<u32> =
                    values_from_key.iter().map(|value| value.pagenum).collect();

                lowest_internal_pages.extend(pages_below_this_page);
            }
        }

        // Get the leaf pages from the lowest level internal pages
        let mut leaf_pages: HashSet<u32> = HashSet::new();
        for page_number in lowest_internal_pages {
            let page: InternalIndexPage = InternalIndexPage::load_from_table(
                self.table_path.clone(),
                self.table_name.clone(),
                self.table_schema.clone(),
                page_number,
                &self.index_id,
                &self.index_key_type,
            )
            .unwrap();
            let values_from_key: Vec<InternalIndexValue> =
                page.get_index_values_from_key(index_key)?;
            let leaf_pages_below_this_page: Vec<u32> =
                values_from_key.iter().map(|value| value.pagenum).collect();

            leaf_pages.extend(leaf_pages_below_this_page);
        }
        Ok(leaf_pages)
    }

    /// Gets the internal index values that match the index key.
    /// Returns a vector of InternalIndexValue.
    fn get_index_values_from_key(
        &self,
        index_key: &IndexKey,
    ) -> Result<Vec<InternalIndexValue>, String> {
        // If there are no keys
        if self.index_keys.len() == 0 {
            // There is always an index value in an internal page, even with no keys.
            if self.index_values.len() == 0 {
                return Err(format!(
                    "get_index_values_from_key(): Internal page {} has no index values.",
                    self.pagenum
                ));
            }
            return Ok(vec![self.index_values[0].clone()]);
        } else {
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
                            if compare_indexes(&self.index_keys[j + 1], &index_key)
                                != KeyComparison::Equal
                            {
                                break;
                            }
                            j += 1;
                        }
                        break;
                    }
                    // If the key is greater than our index_key, then we want the value from index i
                    // and break out of the loop
                    KeyComparison::Greater => {
                        if self.index_values.len() > i {
                            pointers.push(self.index_values[i].clone());
                            break;
                        } else {
                            return Err(
                                format!(
                                    "get_index_values_from_key(): Internal page {} doesn't have an index value at {}.",
                                    self.pagenum,
                                    i
                                )
                            );
                        }
                    }
                    _ => {}
                }
            }
            // We need to return the last index value if we didn't find a match
            if pointers.len() == 0 {
                // There is always an index value in an internal page, even with no keys.
                if self.index_values.len() == 0 {
                    return Err(format!(
                        "get_index_values_from_key(): Internal page {} has no index values.",
                        self.pagenum
                    ));
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
        index_idx: usize,
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            ((index_idx + 1) * InternalIndexValue::size()) +        // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE; // The offset of the page header
        write_index_key_at_offset(index_key, index_key_type, page, offset)
    }

    /// Writes an index key to a page at a specific index value's index
    /// Note: This is the index value's index, not the index key's index.
    /// i.e. the first index value is index value_idx=0, the second index value is index value_idx=1, etc.
    fn write_index_value(
        index_value: &InternalIndexValue,
        index_key_type: &IndexKeyType,
        page: &mut Page,
        value_idx: usize,
    ) -> Result<(), String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * InternalIndexValue::size()) +              // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE; // The offset of the page header
        write_internal_index_value_at_offset(index_value, page, offset)
    }

    /// Reads an index key from a page at a specific index key's index
    /// Note: This is the index key's index.
    /// i.e. the first index key is index_idx=0, the second index key is index_idx=1, etc.
    fn read_index_key(
        index_key_type: &IndexKeyType,
        page: &Page,
        index_idx: usize,
    ) -> Result<IndexKey, String> {
        // Calculte the offset of the index_idx
        let offset: usize = (index_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            ((index_idx + 1) * InternalIndexValue::size()) +        // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE; // The offset of the page header
        read_index_key_at_offset(index_key_type, page, offset)
    }

    /// Reads an index value from a page at a specific index value's index
    /// Note: This is the index value's index, not the index key's index.
    /// i.e. the first index value is index value_idx=0, the second index value is index value_idx=1, etc.
    fn read_index_value(
        index_key_type: &IndexKeyType,
        page: &Page,
        value_idx: usize,
    ) -> Result<InternalIndexValue, String> {
        // Calculte the offset of the index_idx
        let offset: usize = (value_idx * get_index_key_type_size(index_key_type)) + // The offset of the index keys
                            (value_idx * InternalIndexValue::size()) +              // The offset of the index values
                            INTERNAL_PAGE_HEADER_SIZE; // The offset of the page header
        read_internal_index_value_at_offset(page, offset)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serial_test::serial;
    use sqlparser::ast::{BinaryOperator, Ident};

    use super::*;
    use crate::fileio::tableio::*;

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
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let internal_page_num: u32 = 2;
        let internal_page_depth: u8 = 7;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir)
            .unwrap()
            .0;
        write_page(
            internal_page_num,
            &table.path,
            &[0; PAGE_SIZE],
            PageType::InternalIndex,
        )
        .unwrap();

        // Create the index keys and values
        let index_key1: IndexKey = vec![Value::I32(1), Value::String("a".to_string())];
        let index_key2: IndexKey = vec![Value::I32(2), Value::String("b".to_string())];

        let index_value1: InternalIndexValue = InternalIndexValue { pagenum: 3 };
        let index_value2: InternalIndexValue = InternalIndexValue { pagenum: 4 };
        let index_value3: InternalIndexValue = InternalIndexValue { pagenum: 219 };

        // Create the leaf page
        let mut leaf_page: InternalIndexPage = InternalIndexPage::new(
            &table,
            internal_page_num,
            &index_id,
            &index_key_type,
            &index_value1,
            internal_page_depth,
        )
        .unwrap();

        // Write the index keys and values to the page structure
        InternalIndexPage::write_index_key(&index_key1, &index_key_type, &mut leaf_page.page, 0)
            .unwrap();
        InternalIndexPage::write_index_key(&index_key2, &index_key_type, &mut leaf_page.page, 1)
            .unwrap();
        InternalIndexPage::write_index_value(
            &index_value1,
            &index_key_type,
            &mut leaf_page.page,
            0,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value2,
            &index_key_type,
            &mut leaf_page.page,
            1,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value3,
            &index_key_type,
            &mut leaf_page.page,
            2,
        )
        .unwrap();

        // Write the page to disk
        leaf_page.write_page().unwrap();

        // Load the page from disk
        let loaded_internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            table.name.clone(),
            table.schema.clone(),
            internal_page_num,
            &index_id,
            &index_key_type,
        )
        .unwrap();

        // Check that the index keys and values are correct
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &loaded_internal_page.page, 0)
                .unwrap(),
            index_key1
        );
        assert_eq!(
            InternalIndexPage::read_index_key(&index_key_type, &loaded_internal_page.page, 1)
                .unwrap(),
            index_key2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 0)
                .unwrap(),
            index_value1
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 1)
                .unwrap(),
            index_value2
        );
        assert_eq!(
            InternalIndexPage::read_index_value(&index_key_type, &loaded_internal_page.page, 2)
                .unwrap(),
            index_value3
        );

        // Check that the attributes are correct
        assert_eq!(loaded_internal_page.pagenum, internal_page_num);
        assert_eq!(loaded_internal_page.index_id, index_id);
        assert_eq!(loaded_internal_page.index_key_type, index_key_type);
        assert_eq!(loaded_internal_page.page_depth, internal_page_depth);

        // Clean up tests
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_index_values_from_key() {
        let (table, internal_pagenum, index_key_type, index_id, index_keys, index_values) =
            create_testing_table_and_internal_page();

        // Load the page from disk
        let internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            table.name.clone(),
            table.schema.clone(),
            internal_pagenum,
            &index_id,
            &index_key_type,
        )
        .unwrap();

        // Check that the correct row locations are returned
        assert_eq!(
            internal_page
                .get_index_values_from_key(&index_keys[0])
                .unwrap(),
            vec![index_values[1].clone()]
        );
        assert_eq!(
            internal_page
                .get_index_values_from_key(&index_keys[1])
                .unwrap(),
            vec![index_values[2].clone()]
        );
        assert_eq!(
            internal_page
                .get_index_values_from_key(&index_keys[2])
                .unwrap(),
            vec![index_values[3].clone()]
        );
        assert_eq!(
            internal_page
                .get_index_values_from_key(&index_keys[3])
                .unwrap(),
            vec![index_values[4].clone()]
        );

        // Test a key that is greater than all the other keys in the page
        assert_eq!(
            internal_page
                .get_index_values_from_key(&vec![Value::I32(4), Value::String("e".to_string())])
                .unwrap(),
            vec![index_values[4].clone()]
        );

        // Test a key that is less than all the other keys in the page
        assert_eq!(
            internal_page
                .get_index_values_from_key(&vec![Value::I32(0), Value::String("a".to_string())])
                .unwrap(),
            vec![index_values[0].clone()]
        );

        // Test keys that are in between the 2nd and 3rd keys in the page
        assert_eq!(
            internal_page
                .get_index_values_from_key(&vec![Value::I32(2), Value::String("c".to_string())])
                .unwrap(),
            vec![index_values[2].clone()]
        );
        assert_eq!(
            internal_page
                .get_index_values_from_key(&vec![Value::I32(3), Value::String("a".to_string())])
                .unwrap(),
            vec![index_values[2].clone()]
        );

        // Clean up the testing table
        clean_up_tests();
    }

    #[test]
    #[serial]
    fn test_get_leaf_pagenums_matching_expr() {
        let (table, internal_pagenum, index_key_type, index_id, _, index_values) =
            create_testing_table_and_internal_page();

        // Load the page from disk
        let internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            table.name.clone(),
            table.schema.clone(),
            internal_pagenum,
            &index_id,
            &index_key_type,
        )
        .unwrap();

        // Check that the correct row locations are returned
        let tables: Vec<(Table, String)> = vec![(table.clone(), table.name.clone())];
        let column_aliases: ColumnAliases = gen_column_aliases(&tables);
        let index_refs: IndexRefs = get_index_refs(&column_aliases);
        // Test WHERE id > 2
        assert_eq!(
            internal_page
                .get_leaf_pagenums_matching_expr(
                    &Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "id".to_string(),
                            quote_style: None
                        })),
                        op: BinaryOperator::Gt,
                        right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                            "2".to_string(),
                            true
                        )))
                    },
                    &column_aliases,
                    &index_refs
                )
                .unwrap()
                .iter()
                .cloned()
                .collect_vec()
                .iter()
                .sorted()
                .cloned()
                .collect::<Vec<u32>>(),
            vec![index_values[3].pagenum, index_values[4].pagenum]
        );
        // Test WHERE id = 2 AND name = 'c'
        assert_eq!(
            internal_page
                .get_leaf_pagenums_matching_expr(
                    &Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "id".to_string(),
                                quote_style: None
                            })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                                "2".to_string(),
                                true
                            )))
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "name".to_string(),
                                quote_style: None
                            })),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                                "c".to_string(),
                                true
                            )))
                        }),
                    },
                    &column_aliases,
                    &index_refs
                )
                .unwrap()
                .contains(&index_values[2].pagenum),
            true
        );
        // Test WHERE id > 1 AND name < 'd'
        assert_eq!(
            internal_page
                .get_leaf_pagenums_matching_expr(
                    &Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "id".to_string(),
                                quote_style: None
                            })),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                                "1".to_string(),
                                true
                            )))
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "name".to_string(),
                                quote_style: None
                            })),
                            op: BinaryOperator::Lt,
                            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                                "d".to_string(),
                                true
                            )))
                        }),
                    },
                    &column_aliases,
                    &index_refs
                )
                .unwrap()
                .iter()
                .cloned()
                .collect_vec()
                .iter()
                .sorted()
                .cloned()
                .collect::<Vec<u32>>(),
            vec![index_values[1].pagenum, index_values[2].pagenum]
        );

        // Clean up the testing table
        clean_up_tests();
    }

    /// Creates a testing table and leaf page with 4 index keys and values.
    fn create_testing_table_and_internal_page() -> (
        Table,
        u32,
        IndexKeyType,
        IndexID,
        Vec<IndexKey>,
        Vec<InternalIndexValue>,
    ) {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("testing_internal_page_table");
        let index_key_type: IndexKeyType = vec![Column::I32, Column::String(10)];
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["id".to_string(), "name".to_string()];
        let index_id: IndexID = create_index_id(&index_column_names, &table_schema).unwrap();
        let internal_page_num: u32 = 2;

        // Create the table
        let table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir)
            .unwrap()
            .0;
        write_page(
            internal_page_num,
            &table.path,
            &[0; PAGE_SIZE],
            PageType::LeafIndex,
        )
        .unwrap();

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
            &table,
            internal_page_num,
            &index_id,
            &index_key_type,
            &index_value1,
            1,
        )
        .unwrap();

        // Write the index keys and values to the page structure
        InternalIndexPage::write_index_key(
            &index_key1,
            &index_key_type,
            &mut internal_page.page,
            0,
        )
        .unwrap();
        InternalIndexPage::write_index_key(
            &index_key2,
            &index_key_type,
            &mut internal_page.page,
            1,
        )
        .unwrap();
        InternalIndexPage::write_index_key(
            &index_key3,
            &index_key_type,
            &mut internal_page.page,
            2,
        )
        .unwrap();
        InternalIndexPage::write_index_key(
            &index_key4,
            &index_key_type,
            &mut internal_page.page,
            3,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value2,
            &index_key_type,
            &mut internal_page.page,
            1,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value3,
            &index_key_type,
            &mut internal_page.page,
            2,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value4,
            &index_key_type,
            &mut internal_page.page,
            3,
        )
        .unwrap();
        InternalIndexPage::write_index_value(
            &index_value5,
            &index_key_type,
            &mut internal_page.page,
            4,
        )
        .unwrap();

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
            table,
            internal_page_num,
            index_key_type,
            index_id,
            vec![index_key1, index_key2, index_key3, index_key4],
            vec![
                index_value1,
                index_value2,
                index_value3,
                index_value4,
                index_value5,
            ],
        )
    }

    fn clean_up_tests() {
        std::fs::remove_dir_all("./testing").unwrap();
    }
}
