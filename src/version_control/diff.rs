use crate::{fileio::header::*, util::row::*};

/***************************************************************************************************/
/*                                         Diff Structs                                            */
/***************************************************************************************************/

#[derive(Clone, Debug, PartialEq)]
pub enum Diff {
    Update(UpdateDiff),
    Insert(InsertDiff),
    Remove(RemoveDiff),
    TableCreate(TableCreateDiff),
    TableRemove(TableRemoveDiff),
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub schema: Schema,     // The schema of the table
    pub rows: Vec<RowInfo>, // The rows that were updated.
}

#[derive(Clone, Debug, PartialEq)]
pub struct InsertDiff {
    pub table_name: String, // The name of the table that the rows were updated in
    pub schema: Schema,     // The schema of the table
    pub rows: Vec<RowInfo>, // The rows that were inserted.
}

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveDiff {
    pub table_name: String, // The name of the table that the rows were removed from
    pub row_locations: Vec<RowLocation>, // The rows that were removed.
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableCreateDiff {
    pub table_name: String, // The name of the table that was created.
    pub schema: Schema,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableRemoveDiff {
    pub table_name: String, // The name of the table that was removed.
}

/***************************************************************************************************/
/*                                         Constructors                                            */
/***************************************************************************************************/

impl UpdateDiff {
    pub fn new(table_name: String, schema: Schema, rows: Vec<RowInfo>) -> Self {
        Self { table_name, schema, rows }
    }
}

impl InsertDiff {
    pub fn new(table_name: String, schema: Schema, rows: Vec<RowInfo>) -> Self {
        Self { table_name, schema, rows }
    }
}

impl RemoveDiff {
    pub fn new(table_name: String, row_locations: Vec<RowLocation>) -> Self {
        Self {
            table_name,
            row_locations,
        }
    }
}
