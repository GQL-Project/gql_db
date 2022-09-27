use crate::util::row::RowInfo;

#[derive(Clone)]
pub struct UpdateDiff {
    table_name: String, // The name of the table that the rows were updated in
    row_size: usize,    // The size of each row.
    num_rows: usize,    // The number of rows that were affected.
    rows: Vec<RowInfo>, // The rows that were updated.
}

impl UpdateDiff {
    pub fn new(table_name: String, row_size: usize, num_rows: usize, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            row_size,
            num_rows,
            rows,
        }
    }
}