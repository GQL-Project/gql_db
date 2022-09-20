use super::{
    header::{read_header, schema_size, Schema},
    pageio::{check_bounds, read_page, Page},
    rowio::{read_row, Row},
};

struct Table {
    pub schema: Schema,
    pub page: Box<Page>,
    pub path: String,
    pub pagenum: u32,
    pub rownum: u16,
    pub maxpages: u32,
    pub size: usize,
}

impl Table {
    // Construct a new table.
    pub fn new(path: String) -> Result<Table, String> {
        let header = read_header(&path)?;
        let page = read_page(1, &path).map_err(|e| e.to_string())?;
        Ok(Table {
            size: schema_size(&header.schema),
            schema: header.schema,
            page,
            path,
            pagenum: 1,
            rownum: 0,
            maxpages: header.num_pages,
        })
    }

    fn get_offset(&self) -> usize {
        self.rownum as usize * self.size
    }
}

/// This allows for table scans (but read-only)
/// We can now perform iteration over the table like:
/// table = Table::new("test.db");
/// for row in table {
///    println!("{:?}", row);
/// }
impl Iterator for Table {
    type Item = Row;

    // This iterator should only return None when the table is exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        let mut row = None;
        // Keep reading rows until we find one that isn't empty, or we run out of rows.
        while row.is_none() {
            if self.pagenum > self.maxpages {
                return None;
            }
            if check_bounds(self.get_offset(), self.size).is_err() {
                self.page = read_page(self.pagenum + 1, &self.path).ok()?;
                self.pagenum += 1;
                self.rownum = 0;
            }
            row = read_row(&self.schema, &self.page, self.rownum);
            self.rownum += 1;
        }
        row
    }
}

#[cfg(test)]
mod tests {

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
        let path = "test_read_table.db".to_string();
        let table = create_table(&path);
        // Zip iterator with index
        for (i, row) in table.enumerate() {
            if i <= 68 {
                assert_eq!(row[0], Value::I32(1));
                assert_eq!(row[1], Value::String("John Constantine".to_string()));
                assert_eq!(row[2], Value::I32(20));
            } else {
                assert_eq!(row[0], Value::I32(2));
                assert_eq!(row[1], Value::String("Adam Sandler".to_string()));
                assert_eq!(row[2], Value::I32(40));
            }
        }
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
            num_pages: 2,
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
