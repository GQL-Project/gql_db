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
    // TODO: Write tests for the table iterator.
}
