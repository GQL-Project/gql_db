use crate::util::row::{Row, RowInfo};

use super::{
    header::{read_header, schema_size, Schema},
    pageio::{check_bounds, load_page, read_page, write_page, Page, PAGE_SIZE},
    rowio::{insert_row, read_row, write_row},
};

pub struct Table {
    pub schema: Schema,
    pub page: Box<Page>,
    pub path: String,
    pub page_num: u32,
    pub row_num: u16,
    pub max_pages: u32,
    pub schema_size: usize,
}

impl Table {
    // Construct a new table.
    pub fn new(path: String) -> Result<Table, String> {
        let header = read_header(&path)?;
        let page = Box::new([0u8; PAGE_SIZE]);
        Ok(Table {
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

/// This function is helpful when doing Updates
/// It allows us to rewrite a specific row from the table.
pub fn rewrite_rows(table: &Table, mut rows: Vec<RowInfo>) -> Result<(), String> {
    // To reduce page updates, we sort the rows by page number.
    if rows.len() < 1 {
        return Ok(());
    }
    rows.sort();
    let mut pagenum = rows[0].pagenum;
    let mut page = read_page(pagenum, &table.path)?;
    for row in rows {
        if pagenum != row.pagenum {
            write_page(pagenum, &table.path, page.as_ref())?;
            pagenum = row.pagenum;
            load_page(row.pagenum, &table.path, page.as_mut())?;
        }
        write_row(&table.schema, &mut page, &row.row, row.rownum)?;
    }
    // Write the last page
    write_page(pagenum, &table.path, page.as_ref())?;
    Ok(())
}

/// This function is helpful when doing Inserts
/// It allows us to insert a row into the table, allocating space when needed
pub fn insert_rows(table: &mut Table, rows: Vec<Row>) -> Result<(), String> {
    let mut pagenum = 1;
    let mut page = read_page(pagenum, &table.path)?;
    for row in rows {
        while insert_row(&table.schema, page.as_mut(), &row)?.is_none() {
            write_page(pagenum, &table.path, page.as_ref())?;
            pagenum += 1;
            if pagenum > table.max_pages || load_page(pagenum, &table.path, page.as_mut()).is_err()
            {
                // Allocate a new page
                page = Box::new([0; 4096]);
                table.max_pages += 1;
            }
        }
    }
    // Write the last page
    write_page(pagenum, &table.path, page.as_mut())?;
    Ok(())
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
        let path = "test_readterator.db".to_string();
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
        let path = "test_replacerator.db".to_string();
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
        rewrite_rows(&table, rowinfos).unwrap();

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

    fn test_inserts() {
        let path = "test_inserterator.db".to_string();
        let mut table = create_table(&path);
        let row = vec![
            Value::I32(3),
            Value::String("Alexander Hamilton".to_string()),
            Value::I32(60),
        ];
        let rows: Vec<Vec<Value>> = repeat(row).take(69).collect();
        insert_rows(&mut table, rows).unwrap();

        let row = vec![
            Value::I32(4),
            Value::String("Aaron Burr".to_string()),
            Value::I32(40),
        ];
        let rows: Vec<Vec<Value>> = repeat(row).take(69).collect();
        insert_rows(&mut table, rows).unwrap();

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

    fn create_table(path: &String) -> Table {
        // Creates a file table
        create_file(path).unwrap();
        let schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let header = Header {
            num_pages: 3,
            schema: schema.clone(),
        };
        write_header(path, &header).unwrap();
        let row = vec![
            Value::I32(1),
            Value::String("John Constantine".to_string()),
            Value::I32(20),
        ];
        let mut page = [0u8; PAGE_SIZE];
        while insert_row(&schema, &mut page, &row).unwrap().is_some() {}
        write_page(1, path, &page).unwrap();

        let row = vec![
            Value::I32(2),
            Value::String("Adam Sandler".to_string()),
            Value::I32(40),
        ];
        let mut page = [0u8; PAGE_SIZE];
        while insert_row(&schema, &mut page, &row).unwrap().is_some() {}
        write_page(2, path, &page).unwrap();
        // Clean up by removing file
        Table::new(path.to_string()).unwrap()
    }

    fn clean_table(path: &String) {
        std::fs::remove_file(path).unwrap();
    }
}
