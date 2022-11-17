use sqlparser::ast::Expr;

use super::indexes::*;
use super::internal_index_page::*;
use super::leaf_index_page::*;
use crate::fileio::header::*;
use crate::fileio::tableio::*;
use crate::util::row::*;
use crate::version_control::diff::{IndexCreateDiff, IndexRemoveDiff};

#[derive(Clone)]
pub struct BTree {
    index_key_type: IndexKeyType, // The type of the index keys
    index_name: String,           // The name of the index
    root_page: InternalIndexPage, // The highest level internal index page (root of the tree)
    table: Table,                 // The table that this index is for
}

impl BTree {
    /// Create an index on one or more columns
    /// It automatically updates the table to include the index
    pub fn create_btree_index(
        table_dir: &String,
        table_name: &String,
        table_extension: Option<&String>, // Optionally specify a file extension. Defaults to TABLE_FILE_EXTENSION.
        columns: Vec<String>,
        index_name: String,
    ) -> Result<(Self, IndexCreateDiff), String> {
        let mut table: Table = Table::new(table_dir, table_name, table_extension)?;

        // Get the index key composed of those column names
        let index_id: IndexID = create_index_id(&columns, &table.schema)?;
        let index_key_type: IndexKeyType = cols_id_to_index_key_type(&index_id, &table.schema);

        // Check that the index doesn't already exist
        if table.indexes.contains_key(&index_id) {
            return Err(format!("Index already exists on columns: {:?}", columns));
        }

        // Get the rows in the table
        let mut table_rows: Vec<RowInfo> = Vec::new();
        for rowinfo in table.clone() {
            table_rows.push(rowinfo);
        }
        // Sort all the rows using the index key
        table_rows.sort_by(|a, b| compare_rows_using_index_id(&a.row, &b.row, &index_id));

        // Get the root internal index page
        let root_page: InternalIndexPage = Self::create_pages_for_btree(
            &mut table,
            LeafIndexPage::convert_to_key_vals(table_rows, &index_id)?,
            &index_id,
            &index_key_type,
            index_name.clone(),
        )?;

        Ok((
            BTree {
                index_key_type,
                index_name: index_name.clone(),
                root_page,
                table: table.clone(),
            },
            IndexCreateDiff {
                table_name: table.name.clone(),
                schema: table.schema.clone(),
                indexes: vec![(index_name, index_id)],
            },
        ))
    }

    /// Removes a btree index from the table
    pub fn drop_btree_index(
        table_dir: &String,
        table_name: &String,
        table_extension: Option<&String>, // Optionally specify a file extension. Defaults to TABLE_FILE_EXTENSION.
        index_name: &String,
    ) -> Result<IndexRemoveDiff, String> {
        let mut table: Table = Table::new(table_dir, table_name, table_extension)?;

        // Get the index_id from the index name
        let mut index_id: Option<IndexID> = None;
        table.indexes.iter().position(|(id, index)| {
            if index.1 == *index_name {
                index_id = Some(id.clone());
                return true;
            }
            false
        });

        if let Some(index_id) = index_id {
            // Remove the index from the table
            table.indexes.remove(&index_id);

            // Update the header
            let new_header: Header = Header {
                num_pages: table.max_pages,
                schema: table.schema.clone(),
                index_top_level_pages: table.indexes.clone(),
            };
            write_header(&table.path, &new_header)?;

            Ok(IndexRemoveDiff {
                table_name: table.name.clone(),
                schema: table.schema.clone(),
                indexes: vec![(index_name.clone(), index_id)],
            })
        } else {
            Err(format!("Index {} does not exist", index_name))
        }
    }

    /// Loads a btree from a given root page
    pub fn load_btree_from_root_page(
        table: &Table,
        pagenum: u32,
        index_id: IndexID,
        index_key_type: IndexKeyType,
        index_name: String,
    ) -> Result<Self, String> {
        let internal_page: InternalIndexPage = InternalIndexPage::load_from_table(
            table.path.clone(),
            table.name.clone(),
            table.schema.clone(),
            pagenum,
            &index_id,
            &index_key_type,
        )?;

        Ok(BTree {
            index_key_type,
            index_name,
            root_page: internal_page,
            table: table.clone(),
        })
    }

    /// Gets the rows corresponding to the given index key
    pub fn get_rows(&self, index_key: &IndexKey) -> Result<Vec<RowInfo>, String> {
        self.root_page.get_rows_from_key(index_key)
    }

    /// Gets the rows corresponding to the given predicate
    pub fn get_rows_matching_expr(&self, pred: &Expr) -> Result<Vec<RowInfo>, String> {
        self.root_page.get_rows_matching_expr(pred)
    }

    /// Inserts rows into the btree
    pub fn insert_rows(&mut self, rows: &Vec<RowInfo>) -> Result<(), String> {
        for row in rows {
            self.root_page.insert_row(row, self.index_name.clone())?;
        }
        Ok(())
    }

    /// Removes rows from the btree
    pub fn remove_rows(&mut self, rows: &Vec<RowInfo>) -> Result<(), String> {
        for row in rows {
            self.root_page.remove_row(row, self.index_name.clone())?;
        }
        Ok(())
    }

    /// Updates a row in the btree.
    /// This is done by removing the old row and inserting the new row.
    /// It's slow, but simple to implement.
    pub fn update_rows(
        &mut self,
        old_rows: &Vec<RowInfo>,
        new_rows: &Vec<RowInfo>,
    ) -> Result<(), String> {
        self.remove_rows(old_rows)?;
        self.insert_rows(new_rows)?;
        Ok(())
    }

    /// Creates the pages for a btree index on a table using the given key-value pairs.
    pub fn create_pages_for_btree(
        table: &mut Table,
        key_values: Vec<(IndexKey, LeafIndexValue)>,
        index_id: &IndexID,
        index_key_type: &IndexKeyType,
        index_name: String,
    ) -> Result<InternalIndexPage, String> {
        let num_rows: usize = key_values.len();

        // If there are no rows, we need to create 2 empty leaf pages and an internal index page that points to both
        if num_rows == 0 {
            // Create the leaf pages
            let mut leaf_page1: LeafIndexPage = LeafIndexPage::new(
                table.path.clone(),
                table.max_pages,
                index_id,
                index_key_type,
            )?;
            table.max_pages += 1;
            let mut leaf_page2: LeafIndexPage = LeafIndexPage::new(
                table.path.clone(),
                table.max_pages,
                index_id,
                index_key_type,
            )?;
            table.max_pages += 1;

            // Create the internal index page
            let mut internal_page: InternalIndexPage = InternalIndexPage::new(
                table,
                table.max_pages,
                index_id,
                index_key_type,
                &InternalIndexValue {
                    pagenum: leaf_page1.get_pagenum(),
                },
                1u8,
            )?;
            table.max_pages += 1;

            internal_page.add_pointer_to_page(
                &index_key_type
                    .iter()
                    .map(|x| x.get_default_value())
                    .collect(),
                &InternalIndexValue {
                    pagenum: leaf_page2.get_pagenum(),
                },
            )?;

            // Write the pages to disk
            leaf_page1.write_page()?;
            leaf_page2.write_page()?;
            internal_page.write_page()?;

            // Update the header
            table
                .indexes
                .insert(index_id.clone(), (internal_page.get_pagenum(), index_name));
            let new_header: Header = Header {
                num_pages: table.max_pages,
                schema: table.schema.clone(),
                index_top_level_pages: table.indexes.clone(),
            };
            write_header(&table.path, &new_header)?;

            return Ok(internal_page);
        }

        // The number of pages per level is built from the bottom up.
        // The number of pages at the bottom level (idx 0) is the number of leaf pages.
        let mut num_pages_per_level: Vec<u32> = Vec::new();

        // Calculate how many rows pointers we can fit on a single leaf page
        let max_pointers_per_leaf: usize =
            LeafIndexPage::get_max_index_pointers_per_page(&index_key_type);

        // Calculate how many leaf pages we need to store all the rows
        // Double it because we want to keep the leaves between 50% and 100% full
        let num_leaf_pages: u32 =
            ((num_rows as f64 / max_pointers_per_leaf as f64).ceil() as u32) * 2;
        num_pages_per_level.push(num_leaf_pages);

        // Calculate how many pointers we can fit on a single internal page
        let max_pointers_per_internal: usize =
            InternalIndexPage::get_max_index_pointers_per_page(&index_key_type);

        // Calculate how many internal nodes we need to point to all the leaf pages
        let mut pages_on_level_below: u32 = num_leaf_pages;
        loop {
            // Divide the max_pointers_per_internal by 2 because we want to keep the internal nodes between 50% and 100% full
            let num_internal_pages_for_level: u32 = (pages_on_level_below as f64
                / (max_pointers_per_internal as f64 / 2 as f64))
                .ceil() as u32;
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
                index_id,
                &index_key_type,
            )?;
            table.max_pages += 1;
            leaf_pages.push(leaf_page);
        }

        // Fill the leaf pages with the rows
        let mut leaf_page_idx: usize = 0;
        let mut rows_in_leaf_page: usize = 0;
        for (i, (key, val)) in key_values.iter().enumerate() {
            leaf_pages[leaf_page_idx].add_pointer_to_leaf_value(key, val.clone())?;
            rows_in_leaf_page += 1;
            // Evenly distribute the rows across the leaf pages
            let num_leaf_pages_unfilled: usize = (leaf_pages.len() - leaf_page_idx) - 1;
            if rows_in_leaf_page > (num_rows / leaf_pages.len())
                || i == (num_rows - 1) - num_leaf_pages_unfilled
            {
                // Advance the leaf page idx up until the last leaf page
                if leaf_page_idx < (leaf_pages.len() - 1) {
                    leaf_page_idx += 1;
                }
                rows_in_leaf_page = 0;
            }
        }

        // Write the leaf pages to disk
        for mut leaf_page in leaf_pages.clone() {
            leaf_page.write_page()?;
        }

        // For each leaf page, get the page number, and the lowest value IndexKey in the page
        let mut pages_on_level_below: Vec<(u32, IndexKey)> = Vec::new();
        for leaf_page in leaf_pages.clone() {
            if let Some(smallest_key) = leaf_page.get_lowest_index_key() {
                pages_on_level_below.push((leaf_page.get_pagenum(), smallest_key));
            } else {
                return Err(format!(
                    "Leaf page {} has no index keys",
                    leaf_page.get_pagenum()
                ));
            }
        }

        // Create the internal pages for each level starting from the bottom
        let mut internal_pages: Vec<Vec<InternalIndexPage>> = Vec::new();
        for level in 1..num_pages_per_level.len() {
            let mut internal_pages_for_level: Vec<InternalIndexPage> = Vec::new();
            let mut internal_page_idx: usize = 0;
            let mut num_pointers_in_page: Option<usize> = None;
            let num_pages_on_level_below: usize = pages_on_level_below.len();
            // Insert each page on the level below into an internal page within this level
            for (page_below, page_below_lowest_key) in pages_on_level_below {
                // Create a new internal page if we need to
                if num_pointers_in_page.is_none() {
                    let internal_page: InternalIndexPage = InternalIndexPage::new(
                        &table,
                        table.max_pages,
                        &index_id,
                        &index_key_type,
                        &InternalIndexValue {
                            pagenum: page_below,
                        },
                        level as u8,
                    )?;
                    table.max_pages += 1;
                    num_pointers_in_page = Some(1);
                    internal_pages_for_level.push(internal_page);
                }
                // Insert the page on the level below into the current internal page
                else {
                    internal_pages_for_level[internal_page_idx].add_pointer_to_page(
                        &page_below_lowest_key,
                        &InternalIndexValue {
                            pagenum: page_below,
                        },
                    )?;
                    num_pointers_in_page = Some(num_pointers_in_page.unwrap() + 1);

                    // If the internal page is bet, advance to the next one
                    if num_pointers_in_page.unwrap()
                        > (num_pages_on_level_below / num_pages_per_level[level] as usize)
                    {
                        num_pointers_in_page = None;
                        internal_page_idx += 1;
                        // Advance the page idx up until the last page
                        if internal_page_idx < (num_pages_per_level[level] - 1) as usize {
                            internal_page_idx += 1;
                        }
                    }
                }
            }
            // We should have filled part of every page on this level
            assert_eq!(
                internal_page_idx,
                (num_pages_per_level[level] - 1) as usize,
                "Internal Index Pages on level {} were not all filled",
                level
            );

            // Update pages_on_level_below with the pages on this level
            pages_on_level_below = Vec::new();
            for internal_page in internal_pages_for_level.clone() {
                if let Some(smallest_key) = internal_page.get_lowest_index_key() {
                    pages_on_level_below.push((internal_page.get_pagenum(), smallest_key));
                } else {
                    return Err(format!(
                        "Internal page {} has no index keys",
                        internal_page.get_pagenum()
                    ));
                }
            }

            internal_pages.push(internal_pages_for_level);
        }

        // Write the internal pages to disk
        for internal_pages_for_level in internal_pages.clone() {
            for mut internal_page in internal_pages_for_level {
                internal_page.write_page()?;
            }
        }

        let root_page: InternalIndexPage = internal_pages[internal_pages.len() - 1][0].clone();

        // Update the header
        table
            .indexes
            .insert(index_id.clone(), (root_page.get_pagenum(), index_name));
        let new_header: Header = Header {
            num_pages: table.max_pages,
            schema: table.schema.clone(),
            index_top_level_pages: table.indexes.clone(),
        };
        write_header(&table.path, &new_header)?;

        // Get the root internal index page
        Ok(root_page)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::{
        executor::query::*,
        fileio::databaseio::{delete_db_instance, get_db_instance},
        parser::parser::parse,
        user::userdata::User,
        util::{bench::{create_huge_bench_db, create_huge_bench_db_2_tables}, dbtype::*},
        version_control::diff::*,
    };
    use serial_test::serial;
    use sqlparser::ast::{BinaryOperator, Ident};

    #[test]
    #[serial]
    fn test_create_btree_index() {
        let table_dir: String = String::from("./testing");
        let table_name: String = String::from("create_btree_test_table");
        let index_name: String = String::from("test_index");
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let table_rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("a".to_string())],
            vec![Value::I32(4), Value::String("b".to_string())],
            vec![Value::I32(7), Value::String("c".to_string())],
            vec![Value::I32(10), Value::String("d".to_string())],
            vec![Value::I32(13), Value::String("e".to_string())],
            vec![Value::I32(16), Value::String("f".to_string())],
            vec![Value::I32(19), Value::String("g".to_string())],
            vec![Value::I32(49), Value::String("h".to_string())],
            vec![Value::I32(25), Value::String("i".to_string())],
            vec![Value::I32(28), Value::String("j".to_string())],
            vec![Value::I32(31), Value::String("k".to_string())],
            vec![Value::I32(34), Value::String("l".to_string())],
            vec![Value::I32(37), Value::String("m".to_string())],
            vec![Value::I32(40), Value::String("n".to_string())],
            vec![Value::I32(43), Value::String("o".to_string())],
            vec![Value::I32(68), Value::String("p".to_string())],
            vec![Value::I32(46), Value::String("q".to_string())],
            vec![Value::I32(49), Value::String("r".to_string())],
            vec![Value::I32(52), Value::String("s".to_string())],
            vec![Value::I32(55), Value::String("t".to_string())],
        ];

        // Create the table
        let mut table: Table = create_table_in_dir(&table_name, &table_schema, &table_dir)
            .unwrap()
            .0;

        // Insert the rows
        let insert_diff: InsertDiff = table.insert_rows(table_rows.clone()).unwrap();

        // Create the index
        let (btree, _): (BTree, IndexCreateDiff) = BTree::create_btree_index(
            &table_dir,
            &table_name,
            None,
            index_column_names,
            index_name,
        )
        .unwrap();

        // Get the row that has a key of 1
        let index_key: IndexKey = vec![Value::I32(1)];
        let rows: Vec<RowInfo> = btree.get_rows(&index_key).unwrap();
        // There should be one row with a key of 1
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            insert_diff
                .rows
                .iter()
                .find(|rowinfo| rowinfo.row[0] == Value::I32(1))
                .unwrap()
                .clone()
        );

        // Get the rows that have a key of 49
        let index_key: IndexKey = vec![Value::I32(49)];
        let rows: Vec<RowInfo> = btree.get_rows(&index_key).unwrap();
        // There should be two rows with a key of 49
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .clone()
                .iter()
                .filter(|rowinfo| rowinfo.row[0] == Value::I32(49))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows that have a key of 100
        let index_key: IndexKey = vec![Value::I32(100)];
        let rows: Vec<RowInfo> = btree.get_rows(&index_key).unwrap();
        // There should be no rows with a key of 100
        assert_eq!(rows.len(), 0);

        // Clean up
        std::fs::remove_dir_all("./testing").unwrap();
    }

    #[test]
    #[serial]
    fn test_get_rows_matching_expr() {
        let (_, btree, insert_diff): (_, BTree, InsertDiff) = setup_test();

        fn compare_rows_using_id_eq_num(id_value: i32, btree: &BTree, insert_diff: &InsertDiff) {
            // Get the rows from the table that have match 'id = id_value'
            let mut rows: Vec<RowInfo> = btree
                .get_rows_matching_expr(&Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                        id_value.to_string(),
                        true,
                    ))),
                })
                .unwrap();

            // Get the rows from the insert_diff that match 'id = id_value'
            let mut expected_rows: Vec<RowInfo> = insert_diff
                .rows
                .clone()
                .iter()
                .filter(|rowinfo| rowinfo.row[0] == Value::I32(id_value))
                .cloned()
                .collect::<Vec<RowInfo>>();

            // sort each
            rows.sort();
            expected_rows.sort();

            // Compare the two
            assert_eq!(rows, expected_rows);
        }

        // Get the rows from the table that have match 'id = 5'
        // There should be none
        compare_rows_using_id_eq_num(5, &btree, &insert_diff);

        // Get the rows from the table that have match 'id = 19'
        // There should be one
        compare_rows_using_id_eq_num(19, &btree, &insert_diff);

        // Get the rows from the table that have match 'id = 55'
        // There should be one
        compare_rows_using_id_eq_num(55, &btree, &insert_diff);

        // Get the rows from the table that have match 'id = 49'
        // There should be two
        compare_rows_using_id_eq_num(49, &btree, &insert_diff);

        // Clean up
        std::fs::remove_dir_all("./testing").unwrap();
    }

    #[test]
    #[serial]
    fn test_create_index_immediately() {
        let table_dir: String = String::from("./testing");
        let index_name: String = String::from("test_index");
        let table_name: String = String::from("create_btree_test_table");
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];

        // Create the table
        create_table_in_dir(&table_name, &table_schema, &table_dir)
            .unwrap()
            .0;

        // Create the index
        BTree::create_btree_index(
            &table_dir,
            &table_name,
            None,
            index_column_names,
            index_name,
        )
        .unwrap();

        // Clean up
        cleanup_test();
    }

    #[test]
    #[serial]
    fn test_insert_rows() {
        let (_, btree, insert_diff): (_, BTree, InsertDiff) = setup_test();

        // Get the rows from the table that have match 'id = 5'
        // There should be none
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "5".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows, vec![]);

        // Get the rows from the table that have match 'id = 19'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "19".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(19))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows from the table that have match 'id = 49'
        // There should be two
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "49".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(49))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Clean up
        cleanup_test();
    }

    #[test]
    #[serial]
    fn test_remove_rows() {
        let (table, btree, insert_diff): (Table, BTree, InsertDiff) = setup_test();

        // Remove rows that match 'id = 49'
        table
            .remove_rows(
                insert_diff
                    .rows
                    .iter()
                    .filter(|row_info| row_info.row[0] == Value::I32(49))
                    .map(|row_info| RowLocation {
                        pagenum: row_info.pagenum,
                        rownum: row_info.rownum,
                    })
                    .collect::<Vec<RowLocation>>(),
            )
            .unwrap();

        // Get the rows from the table that have match 'id = 49'
        // There should be none
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "49".to_string(),
                    true,
                ))),
            })
            .unwrap();
        assert_eq!(rows.len(), 0);

        // Get the rows from the table that have match 'id = 19'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "19".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(19))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Clean up
        cleanup_test();
    }

    #[test]
    #[serial]
    fn test_update_rows() {
        let (table, btree, insert_diff): (Table, BTree, InsertDiff) = setup_test();

        // Udate rows that match 'id = 49'
        let update_diff: UpdateDiff = table
            .rewrite_rows(
                insert_diff
                    .rows
                    .iter()
                    .filter(|row_info| row_info.row[0] == Value::I32(49))
                    .map(|row_info| RowInfo {
                        row: vec![Value::I32(49), Value::String("z".to_string())],
                        pagenum: row_info.pagenum,
                        rownum: row_info.rownum,
                    })
                    .collect::<Vec<RowInfo>>(),
            )
            .unwrap();

        // Get the rows from the table that match 'id = 49'
        // There should be two
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "49".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows,
            update_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(49))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows from the table that have match 'id = 19'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "19".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(19))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Clean up
        cleanup_test();
    }

    #[test]
    #[serial]
    fn test_write_rows() {
        let (mut table, btree, insert_diff): (Table, BTree, InsertDiff) = setup_test();

        let mut rows_to_write: Vec<RowInfo> = insert_diff
            .rows
            .iter()
            .filter(|row_info| row_info.row[0] == Value::I32(49))
            .map(|row_info| RowInfo {
                row: vec![Value::I32(49), Value::String("z".to_string())],
                pagenum: row_info.pagenum,
                rownum: row_info.rownum,
            })
            .collect::<Vec<RowInfo>>();

        rows_to_write.push(RowInfo {
            row: vec![Value::I32(100), Value::String("abc".to_string())],
            pagenum: 5,
            rownum: 0,
        });

        let write_diff: InsertDiff = table.write_rows(rows_to_write).unwrap();

        // Get the rows from the table that match 'id = 49'
        // There should be two
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "49".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows,
            write_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(49))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows from the table that match 'id = 100'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "100".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            write_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(100))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows from the table that have match 'id = 19'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "id".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "19".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[0] == Value::I32(19))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Clean up
        cleanup_test();
    }

    #[test]
    #[serial]
    fn test_btree_speed_huge() {
        let mut user: User = create_huge_bench_db(1000, false);
        let index_name: String = String::from("test_index");

        // Time the query
        let start_time_no_index: Instant = Instant::now();

        let (_, results) = execute_query(
            &parse("select * from huge_table WHERE id1 = 1", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let duration_no_index: Duration = Instant::now() - start_time_no_index;

        // Assert that the results are correct
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::I32(1));

        let table_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);

        // Create the index
        BTree::create_btree_index(
            &table_dir,
            &"huge_table".to_string(),
            None,
            vec!["id1".to_string()],
            index_name,
        )
        .unwrap();

        // Time the query
        let start_time_with_index: Instant = Instant::now();

        let (_, results) = execute_query(
            &parse("select * from huge_table WHERE id1 = 1", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let duration_with_index: Duration = Instant::now() - start_time_with_index;

        // Assert that the results are correct
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::I32(1));

        println!("Duration without index: {:?}", duration_no_index);
        println!("Duration with index: {:?}", duration_with_index);

        // Assert that the query with the index is faster
        assert!(duration_with_index < duration_no_index);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_btree_speed_huge_two_tables() {
        let (mut user, _, _): (User, _, _) = create_huge_bench_db_2_tables(1000, false);
        let index_name: String = String::from("test_index");

        /*
        // Time the query
        let start_time_no_index: Instant = Instant::now();

        let (_, results) = execute_query(
            &parse(
                "select * from huge_table, huge_table2 WHERE huge_table.id1 = huge_table2.id1 AND huge_table.id1 = 1",
                false
            ).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let duration_no_index: Duration = Instant::now() - start_time_no_index;

        // Assert that the results are correct
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::I32(1));
        */

        let table_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);

        // Create the index
        BTree::create_btree_index(
            &table_dir,
            &"huge_table".to_string(),
            None,
            vec!["id1".to_string()],
            index_name,
        )
        .unwrap();

        // Time the query
        let start_time_with_index: Instant = Instant::now();

        let (_, results) = execute_query(
            &parse(
                "select * from huge_table, huge_table2 WHERE huge_table.id1 = huge_table2.id1 AND huge_table.id1 = 1",
                false
            ).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let duration_with_index: Duration = Instant::now() - start_time_with_index;

        // Assert that the results are correct
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::I32(1));

        //println!("Duration without index: {:?}", duration_no_index);
        println!("Duration with index: {:?}", duration_with_index);

        // Assert that the query with the index is faster
        //assert!(duration_with_index < duration_no_index);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_non_first_key_index() {
        let table_dir: String = String::from("./testing");
        let index_name: String = String::from("test_index");
        let table_name: String = String::from("create_btree_test_table");
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["name".to_string()];
        let table_rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("a".to_string())],
            vec![Value::I32(4), Value::String("b".to_string())],
            vec![Value::I32(7), Value::String("c".to_string())],
            vec![Value::I32(10), Value::String("d".to_string())],
            vec![Value::I32(13), Value::String("e".to_string())],
            vec![Value::I32(16), Value::String("f".to_string())],
            vec![Value::I32(19), Value::String("g".to_string())],
            vec![Value::I32(49), Value::String("h".to_string())],
            vec![Value::I32(25), Value::String("i".to_string())],
            vec![Value::I32(28), Value::String("j".to_string())],
            vec![Value::I32(31), Value::String("a".to_string())],
            vec![Value::I32(34), Value::String("l".to_string())],
            vec![Value::I32(37), Value::String("m".to_string())],
            vec![Value::I32(40), Value::String("n".to_string())],
            vec![Value::I32(43), Value::String("o".to_string())],
            vec![Value::I32(68), Value::String("p".to_string())],
            vec![Value::I32(46), Value::String("q".to_string())],
            vec![Value::I32(49), Value::String("r".to_string())],
            vec![Value::I32(52), Value::String("a".to_string())],
            vec![Value::I32(55), Value::String("t".to_string())],
        ];

        // Create the table
        create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap();

        // Create the index
        let (btree, _): (BTree, IndexCreateDiff) = BTree::create_btree_index(
            &table_dir,
            &table_name,
            None,
            index_column_names,
            index_name,
        )
        .unwrap();

        // Re-read the table
        let mut table: Table = Table::new(&table_dir, &table_name, None).unwrap();

        // Insert the rows
        let insert_diff: InsertDiff = table.insert_rows(table_rows.clone()).unwrap();

        // Get the rows from the table that match 'name = "e"'
        // There should be one
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "name".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "e".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[1] == Value::String("e".to_string()))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Get the rows from the table that match 'name = "a"'
        // There should be three
        let rows: Vec<RowInfo> = btree
            .get_rows_matching_expr(&Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident {
                    value: "name".to_string(),
                    quote_style: None,
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                    "a".to_string(),
                    true,
                ))),
            })
            .unwrap();

        // Compare the two
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows,
            insert_diff
                .rows
                .iter()
                .filter(|row_info| row_info.row[1] == Value::String("a".to_string()))
                .cloned()
                .collect::<Vec<RowInfo>>()
        );

        // Clean up
        cleanup_test();
    }

    fn setup_test() -> (Table, BTree, InsertDiff) {
        let table_dir: String = String::from("./testing");
        let index_name: String = String::from("test_index");
        let table_name: String = String::from("create_btree_test_table");
        let table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(10)),
        ];
        let index_column_names: Vec<String> = vec!["id".to_string()];
        let table_rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("a".to_string())],
            vec![Value::I32(4), Value::String("b".to_string())],
            vec![Value::I32(7), Value::String("c".to_string())],
            vec![Value::I32(10), Value::String("d".to_string())],
            vec![Value::I32(13), Value::String("e".to_string())],
            vec![Value::I32(16), Value::String("f".to_string())],
            vec![Value::I32(19), Value::String("g".to_string())],
            vec![Value::I32(49), Value::String("h".to_string())],
            vec![Value::I32(25), Value::String("i".to_string())],
            vec![Value::I32(28), Value::String("j".to_string())],
            vec![Value::I32(31), Value::String("k".to_string())],
            vec![Value::I32(34), Value::String("l".to_string())],
            vec![Value::I32(37), Value::String("m".to_string())],
            vec![Value::I32(40), Value::String("n".to_string())],
            vec![Value::I32(43), Value::String("o".to_string())],
            vec![Value::I32(68), Value::String("p".to_string())],
            vec![Value::I32(46), Value::String("q".to_string())],
            vec![Value::I32(49), Value::String("r".to_string())],
            vec![Value::I32(52), Value::String("s".to_string())],
            vec![Value::I32(55), Value::String("t".to_string())],
        ];

        // Create the table
        create_table_in_dir(&table_name, &table_schema, &table_dir).unwrap();

        // Create the index
        let (btree, _): (BTree, IndexCreateDiff) = BTree::create_btree_index(
            &table_dir,
            &table_name,
            None,
            index_column_names,
            index_name,
        )
        .unwrap();

        // Re-read the table
        let mut table: Table = Table::new(&table_dir, &table_name, None).unwrap();

        // Insert the rows
        let insert_diff: InsertDiff = table.insert_rows(table_rows.clone()).unwrap();

        (table, btree, insert_diff)
    }

    fn cleanup_test() {
        std::fs::remove_dir_all("./testing").unwrap();
    }
}
