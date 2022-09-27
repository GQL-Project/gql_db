use crate::{util::row::*, fileio::header::*};

/***************************************************************************************************/
/*                                         Diff Structs                                            */
/***************************************************************************************************/

#[derive(Clone)]
pub struct UpdateDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub row_size: usize,    // The size of each row.
    pub num_rows: usize,    // The number of rows that were affected.
    pub rows: Vec<RowInfo>, // The rows that were updated.
}

#[derive(Clone)]
pub struct InsertDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub row_size: usize,    // The size of each row.
    pub num_rows: usize,    // The number of rows that were inserted.
    pub rows: Vec<RowInfo>, // The rows that were inserted.
}

#[derive(Clone)]
pub struct RemoveDiff {
    pub table_name: String,              // The name of the table that the rows were removed from
    pub row_size: usize,                 // The size of each row.
    pub num_rows: usize,                 // The number of rows that were removed.
    pub row_locations: Vec<RowLocation>, // The rows that were removed.
}

#[derive(Clone)]
pub struct TableCreateDiff {
    pub table_name: String, // The name of the table that was created.
    pub schema: Schema
}

#[derive(Clone)]
pub struct TableRemoveDiff {
    pub table_name: String, // The name of the table that was removed.
}


/***************************************************************************************************/
/*                                         Constructors                                            */
/***************************************************************************************************/

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

impl InsertDiff {
    pub fn new(table_name: String, row_size: usize, num_rows: usize, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            row_size,
            num_rows,
            rows,
        }
    }
}

impl RemoveDiff {
    pub fn new(table_name: String, row_size: usize, num_rows: usize, row_locations: Vec<RowLocation>) -> Self {
        Self {
            table_name,
            row_size,
            num_rows,
            row_locations,
        }
    }
}