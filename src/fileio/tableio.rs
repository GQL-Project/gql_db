use super::{databaseio::*, header::*, pageio::*, rowio::*};
use crate::{util::row::*, version_control::diff::*};

pub const TABLE_FILE_EXTENSION: &str = ".db";

#[derive(Clone)]
pub struct Table {
    pub name: String, // The name of the table without the file extension.
    pub schema: Schema,
    pub page: Box<Page>,
    pub path: String,
    pub page_num: u32,
    pub row_num: u16,
    pub max_pages: u32,
    pub schema_size: usize,
}

impl Table {
    /// Construct a new table from an already existing file.
    /// The table_extension parameter is optional. If you do not specify it, it will use the default.
    pub fn new(
        directory: &String,
        table_name: &String,
        table_extension: Option<&String>, // Optionally specify a file extension. Defaults to TABLE_FILE_EXTENSION.
    ) -> Result<Table, String> {
        // Construct the path to the table file.
        let mut path: String;
        if directory.len() == 0 {
            path = table_name.clone();
        } else {
            path = format!("{}{}{}", directory, std::path::MAIN_SEPARATOR, table_name);
        }
        // Add table extension
        path = format!(
            "{}{}",
            path,
            table_extension.unwrap_or(&TABLE_FILE_EXTENSION.to_string())
        );

        // If the file doesn't exist, return an error.
        if !std::path::Path::new(&path).exists() {
            return Err(format!("Table file {} does not exist.", path));
        }

        let header = read_header(&path)?;
        let page = Box::new([0u8; PAGE_SIZE]);
        Ok(Table {
            name: table_name.to_string(),
            schema_size: schema_size(&header.schema),
            schema: header.schema,
            page,
            path,
            page_num: 0,
            row_num: 0,
            max_pages: header.num_pages,
        })
    }

    fn get_offset(&self) -> usize {
        self.row_num as usize * self.schema_size
    }
}

/// This allows for table scans (but read-only)
/// We can now perform iteration over the table like:
/// table = Table::new("test.db");
/// for row in table {
///    println!("{:?}", row);
/// }
impl Iterator for Table {
    type Item = RowInfo;

    // This iterator should only return None when the table is exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        // Load the first page, only when iteration begins.
        if self.page_num == 0 {
            load_page(1, &self.path, self.page.as_mut()).ok()?;
            self.page_num = 1;
        }
        let mut rowinfo = None;
        // Keep reading rows until we find one that isn't empty, or we run out of rows.
        while rowinfo.is_none() {
            if self.page_num >= self.max_pages {
                return None;
            }
            if check_bounds(self.get_offset(), self.schema_size).is_err() {
                load_page(self.page_num + 1, &self.path, self.page.as_mut()).ok()?;
                self.page_num += 1;
                self.row_num = 0;
            }
            if let Some(row) = read_row(&self.schema, self.page.as_ref(), self.row_num) {
                rowinfo = Some(RowInfo {
                    row,
                    pagenum: self.page_num,
                    rownum: self.row_num,
                });
            } else {
                rowinfo = None;
            };
            self.row_num += 1;
        }
        rowinfo
    }
}

/// Create a new table within a given directory named <table_name><TABLE_FILE_EXTENSION>
pub fn create_table_in_dir(
    table_name: &String,
    schema: &Schema,
    table_dir: &String,
) -> Result<(Table, TableCreateDiff), String> {
    // Construct the path to the table file.
    let filename: String = table_name.clone() + &TABLE_FILE_EXTENSION.to_string();
    let mut table_path = filename.clone();
    if table_dir.len() > 0 {
        table_path = table_dir.clone() + std::path::MAIN_SEPARATOR.to_string().as_str() + &filename;
    }

    // Create the file
    create_file(&table_path).map_err(|e| e.to_string())?;

    // Write the header
    let header = Header {
        num_pages: 2,
        schema: schema.clone(),
    };
    write_header(&table_path, &header)?;

    // Write a blank page to the table
    let page = [0u8; PAGE_SIZE];
    write_page(1, &table_path, &page)?;

    // Return the table and the diff
    Ok((
        Table::new(&table_dir.clone(), &table_name.clone(), None)?,
        TableCreateDiff {
            table_name: table_name.clone(),
            schema: schema.clone(),
        },
    ))
}

/// Delete a table from the given database.
pub fn delete_table(table_name: &String, database: &Database) -> Result<TableRemoveDiff, String> {
    let table_dir: String = database.get_current_branch_path();
    // Delete the table file and return it
    delete_table_in_dir(table_name, &table_dir)
}

/// Delete a table from the given directory.
pub fn delete_table_in_dir(
    table_name: &String,
    table_dir: &String,
) -> Result<TableRemoveDiff, String> {
    // Create the path to the table file.
    let filename: String = table_name.clone() + &TABLE_FILE_EXTENSION.to_string();
    let mut table_path = filename.clone();
    //Extracting Schema to use when commits are being reverted
    let schema = Table::new(&table_dir.clone(), &table_name.clone(), None)?.schema;
    if table_dir.len() > 0 {
        table_path = table_dir.clone() + std::path::MAIN_SEPARATOR.to_string().as_str() + &filename;
    }

    // Delete the table file
    std::fs::remove_file(&table_path).map_err(|e| e.to_string())?;

    // Return the diff
    Ok(TableRemoveDiff {
        table_name: table_name.clone(),
        schema: schema.clone(),
    })
}

impl Table {
    /// This function is helpful when doing Updates
    /// It allows us to rewrite a specific row from the table.
    /// It returns a diff of the rows that were updated.
    pub fn rewrite_rows(&self, mut rows: Vec<RowInfo>) -> Result<UpdateDiff, String> {
        //TODO: Update rewrite rows to account for the revert commit case
        // Keep track of how the rows have changed.
        let mut diff: UpdateDiff =
            UpdateDiff::new(self.name.clone(), self.schema.clone(), Vec::new());

        if rows.len() < 1 {
            return Ok(diff);
        }

        // To reduce page updates, we sort the rows by page number.
        rows.sort();
        let mut pagenum = rows[0].pagenum;
        let mut page = read_page(pagenum, &self.path)?;
        for row in rows {
            if pagenum != row.pagenum {
                write_page(pagenum, &self.path, page.as_ref())?;
                pagenum = row.pagenum;
                load_page(row.pagenum, &self.path, page.as_mut())?;
            }
            write_row(&self.schema, &mut page, &row.row, row.rownum)?;
            // Add the row to the diff
            diff.rows.push(row.clone());
        }
        // Write the last page
        write_page(pagenum, &self.path, page.as_ref())?;
        Ok(diff)
    }

    /// This function is helpful when doing Inserts
    /// It allows us to insert a row into the table, allocating space when needed.
    /// It returns a diff of the rows that were inserted.
    pub fn insert_rows(&mut self, rows: Vec<Row>) -> Result<InsertDiff, String> {
        // Keep track of how the rows have changed.
        let mut diff: InsertDiff =
            InsertDiff::new(self.name.clone(), self.schema.clone(), Vec::new());

        // Just return right away if we aren't inserting any rows
        if rows.len() == 0 {
            return Ok(diff);
        }

        let mut pagenum = 1;
        let mut page = read_page(pagenum, &self.path)?;
        for row in rows {
            // Keep track of the rownum that we insert the row into
            let mut rownum_inserted: Option<u16> = insert_row(&self.schema, page.as_mut(), &row)?;
            while rownum_inserted.is_none() {
                write_page(pagenum, &self.path, page.as_ref())?;
                pagenum += 1;
                if pagenum > self.max_pages
                    || load_page(pagenum, &self.path, page.as_mut()).is_err()
                {
                    // Allocate a new page
                    page = Box::new([0; 4096]);
                    self.max_pages += 1;
                }
                rownum_inserted = insert_row(&self.schema, page.as_mut(), &row)?;
            }

            // Add the information to the diff
            diff.rows.push(RowInfo {
                row: row.clone(),
                pagenum,
                rownum: rownum_inserted.unwrap(), // This is fine to unwrap because we just checked that it wasn't None
            });
        }
        // Write the last page
        write_page(pagenum, &self.path, page.as_mut())?;
        Ok(diff)
    }

    /// This function is helpful when building a table from a diff.
    /// It allows us to insert a row into the table, at a specific location
    /// This is a dangerous operation because it could overwrite existing data.
    pub fn write_rows(&mut self, mut rows: Vec<RowInfo>) -> Result<InsertDiff, String> {
        // Keep track of how the rows have changed.
        let mut diff: InsertDiff =
            InsertDiff::new(self.name.clone(), self.schema.clone(), Vec::new());

        // Just return right away if we aren't inserting any rows
        if rows.len() == 0 {
            return Ok(diff);
        }

        // To reduce page updates, we sort the rows by page number.
        rows.sort();
        let mut pagenum: u32 = 0;
        let mut page: Box<Page> = Box::new([0; 4096]);
        for rowinfo in rows {
            pagenum = rowinfo.pagenum;

            // Allocate new pages if needed
            while rowinfo.pagenum >= self.max_pages {
                let new_page = Box::new([0; 4096]);
                self.max_pages += 1;
                write_page(self.max_pages - 1, &self.path, new_page.as_ref())?;
            }
            // Read in the page
            page = read_page(rowinfo.pagenum, &self.path)?;

            // Write the row to the page at the row number
            write_row(&self.schema, page.as_mut(), &rowinfo.row, rowinfo.rownum)?;
            write_page(rowinfo.pagenum, &self.path, page.as_ref())?;

            // Add the information to the diff
            diff.rows.push(RowInfo {
                row: rowinfo.row.clone(),
                pagenum: rowinfo.pagenum,
                rownum: rowinfo.rownum,
            });
        }
        // Write the last page
        write_page(pagenum, &self.path, page.as_mut())?;
        Ok(diff)
    }

    /// This function is helpful when doing Deletes
    /// It removes the rows from the table specified by the tuples (pagenum, rownum)
    /// It returns a diff of the rows that were removed.
    pub fn remove_rows(&self, rows: Vec<RowInfo>) -> Result<RemoveDiff, String> {
        // Keep track of how the rows have changed.
        let schema = self.schema.clone();
        let mut diff: RemoveDiff = RemoveDiff::new(self.name.clone(), schema, Vec::new());

        // Return right away if we aren't removing any rows
        if rows.len() == 0 {
            return Ok(diff);
        }

        // Keep track of the row number we are removing
        let mut curr_page = 1;
        let mut page = read_page(curr_page, &self.path)?;
        for row_location in rows {
            let pagenum: u32 = row_location.pagenum;
            let rownum: u16 = row_location.rownum;
            if curr_page != pagenum {
                write_page(curr_page, &self.path, page.as_ref())?;
                curr_page = pagenum;
                load_page(pagenum, &self.path, page.as_mut())?;
            }
            clear_row(&self.schema, page.as_mut(), rownum)?;

            // Add changes to the diff
            diff.rows_removed.push(row_location);
        }
        // Write the last page
        write_page(curr_page, &self.path, page.as_ref())?;
        Ok(diff)
    }

    /// Get the row from the table specified by the tuple (pagenum, rownum)
    pub fn get_row(&self, row_location: &RowLocation) -> Result<Row, String> {
        // Read the page from the table file
        let page: Page = *read_page(row_location.pagenum, &self.path)?;

        // Get the row from the page based on the schema size
        match read_row(&self.schema, &page, row_location.rownum) {
            Some(row) => {
                return Ok(row);
            }
            None => Err("Row not found".to_string()),
        }
    }
}
#[cfg(test)]
mod tests {

    use rand::prelude::*;
    use std::iter::repeat;

    use super::*;
    use crate::{
        fileio::{
            header::{write_header, Header},
            pageio::{create_file, write_page, PAGE_SIZE},
            rowio::insert_row,
        },
        util::dbtype::{Column, Value},
    };

    #[test]
    fn test_read_iterator() {
        let path = "test_readterator".to_string();
        let table = create_table(&path);
        // Zip iterator with index
        for (i, rowinfo) in table.enumerate() {
            let row = rowinfo.row;
            if i < 69 {
                assert_eq!(row[0], Value::I32(1));
                assert_eq!(row[1], Value::String("John Constantine".to_string()));
                assert_eq!(row[2], Value::I32(20));
                assert_eq!(rowinfo.pagenum, 1);
                assert_eq!(rowinfo.rownum, i as u16);
            } else {
                assert_eq!(row[0], Value::I32(2));
                assert_eq!(row[1], Value::String("Adam Sandler".to_string()));
                assert_eq!(row[2], Value::I32(40));
                assert_eq!(rowinfo.pagenum, 2);
                assert_eq!(rowinfo.rownum, (i - 69) as u16);
            }
        }
        clean_table(&path);
    }

    #[test]
    fn test_replaces() {
        let path = "test_replacerator".to_string();
        let table = create_table(&path);
        let row = vec![
            Value::I32(3),
            Value::String("Alexander Hamilton".to_string()),
            Value::I32(60),
        ];
        let rows: Vec<Vec<Value>> = repeat(row).take(56).collect();
        // We're essentially inserting 56 rows into the table, right in the middle of two pages
        let mut rowinfos: Vec<RowInfo> = rows
            .iter()
            .enumerate()
            .map(|(i, row)| RowInfo {
                row: row.clone(),
                pagenum: if i > 20 { 1 } else { 2 },
                rownum: if i < 25 {
                    (68 - i) as u16
                } else {
                    (i - 25) as u16
                },
            })
            .collect();

        rowinfos.shuffle(&mut rand::thread_rng());
        table.rewrite_rows(rowinfos).unwrap();

        let mut count = 0;
        for rowinfo in table {
            if rowinfo.row[0] == Value::I32(3) {
                count += 1;
            }
        }
        // Assert that we have 56 rows with the value 3 (the value we inserted)
        assert_eq!(count, 56);
        // Clean up by removing file
        clean_table(&path);
    }

    #[test]
    fn test_removes() {
        //TODO: Complete Test post changes made to remove_diff
        assert!(
            false,
            "TODO: Complete Test post changes made to remove_diff"
        );
        /* let path = "test_removerator".to_string();
        let table = create_table(&path);

        let rows: Vec<(u32, u16)> = (10..50)
            .map(|i| (1, i as u16))
            .chain((10..30).map(|i| (2, i as u16)))
            .collect();
        // Cast rows to a vector of Rowlocations
        let rowlocations: Vec<RowLocation> = rows
            .iter()
            .map(|(pagenum, rownum)| RowLocation {
                pagenum: *pagenum,
                rownum: *rownum,
            })
            .collect();
        table.remove_rows(rowlocations).unwrap();
        // Assert that we have (69 * 2 - 60) rows remaining
        assert_eq!(table.into_iter().count(), 78);
        // Clean up by removing file
        clean_table(&path); */
    }

    #[test]
    fn test_inserts() {
        let path = "test_inserterator".to_string();
        let mut table = create_table(&path);
        let row = vec![
            Value::I32(3),
            Value::String("Alexander Hamilton".to_string()),
            Value::I32(60),
        ];
        let rows: Vec<Vec<Value>> = repeat(row).take(69).collect();
        table.insert_rows(rows).unwrap();

        let row = vec![
            Value::I32(4),
            Value::String("Aaron Burr".to_string()),
            Value::I32(40),
        ];
        let rows: Vec<Vec<Value>> = repeat(row).take(69).collect();
        table.insert_rows(rows).unwrap();

        for (i, rowinfo) in table.enumerate() {
            let row = rowinfo.row;

            if i < 69 {
                assert_eq!(row[0], Value::I32(1));
                assert_eq!(row[1], Value::String("John Constantine".to_string()));
                assert_eq!(row[2], Value::I32(20));
                assert_eq!(rowinfo.pagenum, 1);
                assert_eq!(rowinfo.rownum, i as u16);
            } else if i < 138 {
                assert_eq!(row[0], Value::I32(2));
                assert_eq!(row[1], Value::String("Adam Sandler".to_string()));
                assert_eq!(row[2], Value::I32(40));
                assert_eq!(rowinfo.pagenum, 2);
                assert_eq!(rowinfo.rownum, (i - 69) as u16);
            } else if i < 207 {
                assert_eq!(row[0], Value::I32(3));
                assert_eq!(row[1], Value::String("Alexander Hamilton".to_string()));
                assert_eq!(row[2], Value::I32(60));
                assert_eq!(rowinfo.pagenum, 3);
                assert_eq!(rowinfo.rownum, (i - 138) as u16);
            } else {
                assert_eq!(row[0], Value::I32(4));
                assert_eq!(row[1], Value::String("Aaron Burr".to_string()));
                assert_eq!(row[2], Value::I32(40));
                assert_eq!(rowinfo.pagenum, 4);
                assert_eq!(rowinfo.rownum, (i - 207) as u16);
            }
        }
        // Clean up by removing file
        clean_table(&path);
    }

    #[test]
    fn test_diffs() {
        let path = "test_differator".to_string();
        let mut table = create_table(&path);

        let mut rows: Vec<Vec<Value>> = Vec::new();
        rows.push(vec![
            Value::I32(3),
            Value::String("Alexander Hamilton".to_string()),
            Value::I32(60),
        ]);
        rows.push(vec![
            Value::I32(4),
            Value::String("Aaron Burr".to_string()),
            Value::I32(40),
        ]);

        // Try InsertDiff
        let insert_diff: InsertDiff = table.insert_rows(rows).unwrap();
        // Verify that the insert_diff is correct
        assert_eq!(insert_diff.table_name, "test_differator".to_string());
        // Verify that the first row is correct
        assert_eq!(insert_diff.rows[0].pagenum, 3);
        assert_eq!(insert_diff.rows[0].rownum, 0);
        assert_eq!(insert_diff.rows[0].row[0], Value::I32(3));
        assert_eq!(
            insert_diff.rows[0].row[1],
            Value::String("Alexander Hamilton".to_string())
        );
        assert_eq!(insert_diff.rows[0].row[2], Value::I32(60));
        // Verify that the second row is correct
        assert_eq!(insert_diff.rows[1].pagenum, 3);
        assert_eq!(insert_diff.rows[1].rownum, 1);
        assert_eq!(insert_diff.rows[1].row[0], Value::I32(4));
        assert_eq!(
            insert_diff.rows[1].row[1],
            Value::String("Aaron Burr".to_string())
        );
        assert_eq!(insert_diff.rows[1].row[2], Value::I32(40));

        // Try UpdateDiff
        let mut rows_to_update: Vec<RowInfo> = vec![insert_diff.rows[1].clone()];
        rows_to_update[0].row[0] = Value::I32(6);
        rows_to_update[0].row[1] = Value::String("Aaron Burr the 2nd".to_string());
        rows_to_update[0].row[2] = Value::I32(50);
        let update_diff: UpdateDiff = table.rewrite_rows(rows_to_update).unwrap();
        // Verify that the update_diff is correct
        assert_eq!(update_diff.table_name, "test_differator".to_string());
        // Verify that the row is correct
        assert_eq!(update_diff.rows[0].pagenum, 3);
        assert_eq!(update_diff.rows[0].rownum, 1);
        assert_eq!(update_diff.rows[0].row[0], Value::I32(6));
        assert_eq!(
            update_diff.rows[0].row[1],
            Value::String("Aaron Burr the 2nd".to_string())
        );
        assert_eq!(update_diff.rows[0].row[2], Value::I32(50));

        // Try RemoveDiff
        let rows_to_remove: Vec<RowLocation> = vec![RowLocation {
            pagenum: insert_diff.rows[0].clone().pagenum,
            rownum: insert_diff.rows[0].clone().rownum,
        }];
        assert!(
            false,
            "TODO: Complete Test post changes made to remove_diff"
        );
        /* let remove_diff: RemoveDiff = table.remove_rows(rows_to_remove).unwrap();
        // Verify that the remove_diff is correct
        assert_eq!(remove_diff.table_name, "test_differator".to_string());
        // Verify that the row is correct
        assert_eq!(remove_diff.row_locations[0].pagenum, 3);
        assert_eq!(remove_diff.row_locations[0].rownum, 0);

        // Clean up by removing file */
        clean_table(&path);
    }

    #[test]
    fn test_delete_table() {
        let tablename = "test_delete_table".to_string();
        let tablename_with_extension = tablename.clone() + &TABLE_FILE_EXTENSION.to_string();
        create_table(&tablename);

        // Verify that the table exists
        assert!(std::path::Path::new(&tablename_with_extension).exists());

        delete_table_in_dir(&tablename, &"".to_string()).unwrap();

        // Verify that the table does not exist
        assert!(!std::path::Path::new(&tablename).exists());
    }

    #[test]
    fn test_write_rows() {
        let path: String = "test_write_rows".to_string();
        let schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let mut table = create_table_in_dir(&path, &schema, &"".to_string())
            .unwrap()
            .0;

        let mut rows: Vec<RowInfo> = Vec::new();
        rows.push(RowInfo {
            row: vec![
                Value::I32(3),
                Value::String("Alexander Hamilton".to_string()),
                Value::I32(60),
            ],
            pagenum: 3,
            rownum: 4,
        });
        rows.push(RowInfo {
            row: vec![
                Value::I32(4),
                Value::String("Aaron Burr".to_string()),
                Value::I32(40),
            ],
            pagenum: 6,
            rownum: 0,
        });

        // Try InsertDiff
        let insert_diff: InsertDiff = table.write_rows(rows).unwrap();

        // Verify that the max pages were updated
        assert_eq!(table.max_pages, 7);

        // Verify that the insert_diff is correct
        assert_eq!(insert_diff.table_name, "test_write_rows".to_string());
        // Verify that the first row is correct
        assert_eq!(insert_diff.rows[0].pagenum, 3);
        assert_eq!(insert_diff.rows[0].rownum, 4);
        assert_eq!(insert_diff.rows[0].row[0], Value::I32(3));
        assert_eq!(
            insert_diff.rows[0].row[1],
            Value::String("Alexander Hamilton".to_string())
        );
        assert_eq!(insert_diff.rows[0].row[2], Value::I32(60));
        // Verify that the second row is correct
        assert_eq!(insert_diff.rows[1].pagenum, 6);
        assert_eq!(insert_diff.rows[1].rownum, 0);
        assert_eq!(insert_diff.rows[1].row[0], Value::I32(4));
        assert_eq!(
            insert_diff.rows[1].row[1],
            Value::String("Aaron Burr".to_string())
        );
        assert_eq!(insert_diff.rows[1].row[2], Value::I32(40));

        // Collect all rows in the table
        let mut rows_from_table: Vec<RowInfo> = Vec::new();
        for row in table {
            rows_from_table.push(row);
        }
        // Assert that there are two rows
        assert_eq!(rows_from_table.len(), 2);

        let row1: RowInfo = rows_from_table[0].clone();
        // Verify that the first row is correct
        if row1.pagenum == 3 && row1.rownum == 4 {
            assert_eq!(row1.row[0], Value::I32(3));
            assert_eq!(row1.row[1], Value::String("Alexander Hamilton".to_string()));
            assert_eq!(row1.row[2], Value::I32(60));
        }
        let row2: RowInfo = rows_from_table[1].clone();
        // Verify that the second row is correct
        if row2.pagenum == 6 && row2.rownum == 0 {
            assert_eq!(row2.row[0], Value::I32(4));
            assert_eq!(row2.row[1], Value::String("Aaron Burr".to_string()));
            assert_eq!(row2.row[2], Value::I32(40));
        }

        // Clean up by removing file
        clean_table(&path);
    }

    fn create_table(path: &String) -> Table {
        // Creates a file table
        let filepath: String = path.clone() + &TABLE_FILE_EXTENSION.to_string();
        create_file(&filepath).unwrap();
        let schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let header = Header {
            num_pages: 3,
            schema: schema.clone(),
        };
        write_header(&filepath, &header).unwrap();
        let row = vec![
            Value::I32(1),
            Value::String("John Constantine".to_string()),
            Value::I32(20),
        ];
        let mut page = [0u8; PAGE_SIZE];
        while insert_row(&schema, &mut page, &row).unwrap().is_some() {}
        write_page(1, &filepath, &page).unwrap();

        let row = vec![
            Value::I32(2),
            Value::String("Adam Sandler".to_string()),
            Value::I32(40),
        ];
        let mut page = [0u8; PAGE_SIZE];
        while insert_row(&schema, &mut page, &row).unwrap().is_some() {}
        write_page(2, &filepath, &page).unwrap();
        // Clean up by removing file
        Table::new(&"".to_string(), &path.to_string(), None).unwrap()
    }

    fn clean_table(path: &String) {
        let filepath: String = path.clone() + &TABLE_FILE_EXTENSION.to_string();
        std::fs::remove_file(&filepath).unwrap();
    }
}
