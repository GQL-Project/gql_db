use crate::{fileio::header::*, util::row::*};

/***************************************************************************************************/
/*                                         Diff Structs                                            */
/***************************************************************************************************/

pub enum Diff {
    Update(UpdateDiff),
    Insert(InsertDiff),
    Remove(RemoveDiff),
    TableCreate(TableCreateDiff),
    TableRemove(TableRemoveDiff),
}

#[derive(Clone)]
pub struct UpdateDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub rows: Vec<RowInfo>, // The rows that were updated.
}

#[derive(Clone)]
pub struct InsertDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub rows: Vec<RowInfo>, // The rows that were inserted.
}

#[derive(Clone)]
pub struct RemoveDiff {
    pub table_name: String, // The name of the table that the rows were removed from
    pub row_locations: Vec<RowLocation>, // The rows that were removed.
}

#[derive(Clone)]
pub struct TableCreateDiff {
    pub table_name: String, // The name of the table that was created.
    pub schema: Schema,
}

#[derive(Clone)]
pub struct TableRemoveDiff {
    pub table_name: String, // The name of the table that was removed.
}

/***************************************************************************************************/
/*                                         Constructors                                            */
/***************************************************************************************************/

impl UpdateDiff {
    pub fn new(table_name: String, num_rows: usize, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            num_rows,
            rows,
        }
    }
}

impl InsertDiff {
    pub fn new(table_name: String, num_rows: usize, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            num_rows,
            rows,
        }
    }
}

impl RemoveDiff {
    pub fn new(
        table_name: String,
        num_rows: usize,
        row_locations: Vec<RowLocation>,
    ) -> Self {
        Self {
            table_name,
            num_rows,
            row_locations,
        }
    }
}
