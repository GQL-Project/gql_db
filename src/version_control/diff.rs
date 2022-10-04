use crate::{
    fileio::{databaseio::*, header::*, tableio::*},
    util::row::*,
};

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
    pub schema: Schema,     // The schema of the table
    pub rows_removed: Vec<RowInfo>, // The rows that were removed
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableCreateDiff {
    pub table_name: String, // The name of the table that was created.
    pub schema: Schema,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableRemoveDiff {
    pub table_name: String, // The name of the table that was removed.
    pub schema: Schema,
    pub rows_removed: Vec<RowInfo>, // The rows that were removed from the table.
}

/***************************************************************************************************/
/*                                         Constructors                                            */
/***************************************************************************************************/

impl UpdateDiff {
    pub fn new(table_name: String, schema: Schema, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            schema,
            rows,
        }
    }
}

impl InsertDiff {
    pub fn new(table_name: String, schema: Schema, rows: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            schema,
            rows,
        }
    }
}

impl RemoveDiff {
    pub fn new(table_name: String, schema: Schema, rows_removed: Vec<RowInfo>) -> Self {
        Self {
            table_name,
            schema,
            rows_removed,
        }
    }
}

/// This method takes in a directory along with the diffs that are to be applied to it and applies them.
/// There are a couple assumptions:
/// 1. The table_dir exists and is where the table files are/will be stored.
/// 2. The diffs are in the order that the changes were made.
pub fn construct_tables_from_diffs(table_dir: &String, diffs: &Vec<Diff>) -> Result<(), String> {
    for diff in diffs {
        match diff {
            Diff::Update(update_diff) => {
                let table = Table::new(table_dir, &update_diff.table_name, None)?;
                table.rewrite_rows(update_diff.rows.clone())?;
            }
            Diff::Insert(insert_diff) => {
                let mut table = Table::new(table_dir, &insert_diff.table_name, None)?;
                // We write the rows instead of inserting because this allows us to dictate the row nums to insert to
                table.write_rows(insert_diff.rows.clone())?;
            }
            Diff::Remove(remove_diff) => {
                let table = Table::new(table_dir, &remove_diff.table_name, None)?;
                let mut row_locations_removed: Vec<RowLocation> = Vec::new();
                for row in remove_diff.rows_removed.clone() {
                    row_locations_removed.push(row.get_row_location());
                }
                table.remove_rows(row_locations_removed)?;
            }
            Diff::TableCreate(table_create_diff) => {
                create_table_in_dir(
                    &table_create_diff.table_name,
                    &table_create_diff.schema,
                    table_dir,
                )?;
            }
            Diff::TableRemove(table_remove_diff) => {
                delete_table_in_dir(&table_remove_diff.table_name, table_dir)?;
            }
        }
    }
    Ok(())
}

/// This method takes in a directory along with the diffs that are to be undone to the database.
/// There are a couple assumptions:
/// 1. The table_dir exists and is where the table files are/will be stored.
/// 2. The diffs are in the order that the changes were made.
pub fn revert_tables_from_diffs(table_dir: &String, diffs: &Vec<Diff>) -> Result<(), String> {
    //Reversing the list of diffs since we are undoing the changes made to the table
    let reversed_diffs = diffs.iter().rev();
    for diff in reversed_diffs {
        match diff {
            Diff::Update(update_diff) => {
                let table = Table::new(table_dir, &update_diff.table_name, None)?;
                table.rewrite_rows(update_diff.rows.clone())?;
            }
            // We remove rows instead of inserting as we're reverting the change
            Diff::Insert(insert_diff) => {
                let table = Table::new(table_dir, &insert_diff.table_name, None)?;
                let mut row_locations_removed: Vec<RowLocation> = Vec::new();
                for row in insert_diff.rows.clone() {
                    row_locations_removed.push(row.get_row_location());
                }
                table.remove_rows(row_locations_removed)?;
            }
            // Insert instead of remove as we're reverting the change
            Diff::Remove(remove_diff) => {
                let mut table = Table::new(table_dir, &remove_diff.table_name, None)?;
                table.write_rows(remove_diff.rows_removed.clone())?;
            }
            Diff::TableCreate(table_create_diff) => {
                delete_table_in_dir(&table_create_diff.table_name, table_dir)?;
            }
            Diff::TableRemove(table_remove_diff) => {
                create_table_in_dir(
                    &table_remove_diff.table_name,
                    &table_remove_diff.schema,
                    table_dir,
                )?;
                // We need to insert the rows back into the table
                let mut table = Table::new(table_dir, &table_remove_diff.table_name, None)?;
                table.write_rows(table_remove_diff.rows_removed.clone())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::dbtype::*;
    use file_diff::diff;

    #[test]
    fn test_create_table_diff() {
        let dir_to_create_in: String = "test_create_table".to_string();
        let dir_to_build_in: String = "test_create_table_diff".to_string();
        let table_name: String = "test_table".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let mut diffs: Vec<Diff> = Vec::new();

        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table_name, &schema, &dir_to_create_in).unwrap();
        let table1: Table = result.0;
        diffs.push(Diff::TableCreate(result.1.clone()));

        // Construct the table from the diffs and then read it in
        construct_tables_from_diffs(&dir_to_build_in, &diffs).unwrap();
        let table2: Table = Table::new(&dir_to_build_in, &table_name, None).unwrap();

        // Make sure table1 and table2 are the same and that they point to the right directories
        assert!(compare_tables(
            &table1,
            &table2,
            &dir_to_create_in,
            &dir_to_build_in
        ));

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_to_build_in).unwrap();
    }

    #[test]
    fn test_create_table_and_inserts_diff() {
        let dir_to_create_in: String = "test_create_table_and_inserts".to_string();
        let dir_to_build_in: String = "test_create_table_and_inserts_diff".to_string();
        let table_name: String = "test_table".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let mut diffs: Vec<Diff> = Vec::new();

        // Create Table
        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table_name, &schema, &dir_to_create_in).unwrap();
        let mut table1: Table = result.0;
        diffs.push(Diff::TableCreate(result.1.clone()));

        // Insert 2 sets of rows
        let rows1: Vec<Row> = vec![
            vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(2),
                Value::String("Jane".to_string()),
                Value::I32(21),
            ],
            vec![
                Value::I32(3),
                Value::String("Joe".to_string()),
                Value::I32(22),
            ],
        ];
        let rows2: Vec<Row> = vec![
            vec![
                Value::I32(4),
                Value::String("Bob".to_string()),
                Value::I32(30),
            ],
            vec![
                Value::I32(5),
                Value::String("Bill".to_string()),
                Value::I32(31),
            ],
            vec![
                Value::I32(6),
                Value::String("Bucky".to_string()),
                Value::I32(32),
            ],
        ];
        let insert_diff1: InsertDiff = table1.insert_rows(rows1).unwrap();
        diffs.push(Diff::Insert(insert_diff1.clone()));
        let insert_diff2: InsertDiff = table1.insert_rows(rows2).unwrap();
        diffs.push(Diff::Insert(insert_diff2.clone()));

        // Construct the table from the diffs and then read it in
        construct_tables_from_diffs(&dir_to_build_in, &diffs).unwrap();
        let table2 = Table::new(&dir_to_build_in, &table_name, None).unwrap();

        // Make sure table1 and table2 are the same and that they point to the right directories
        assert!(compare_tables(
            &table1,
            &table2,
            &dir_to_create_in,
            &dir_to_build_in
        ));

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_to_build_in).unwrap();
    }

    #[test]
    fn test_update_diff() {
        let dir_to_create_in: String = "test_update".to_string();
        let dir_to_build_in: String = "test_update_diff".to_string();
        let table_name: String = "test_table".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let mut diffs: Vec<Diff> = Vec::new();

        // Create Table
        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table_name, &schema, &dir_to_create_in).unwrap();
        let mut table1: Table = result.0;
        diffs.push(Diff::TableCreate(result.1.clone()));

        // Insert rows
        let rows1: Vec<Row> = vec![
            vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(2),
                Value::String("Jane".to_string()),
                Value::I32(21),
            ],
            vec![
                Value::I32(3),
                Value::String("Joe".to_string()),
                Value::I32(22),
            ],
        ];
        let insert_diff1: InsertDiff = table1.insert_rows(rows1).unwrap();
        diffs.push(Diff::Insert(insert_diff1.clone()));

        // Update 2 of the inserted rows
        let rows_to_change: Vec<RowInfo> = vec![
            RowInfo {
                rownum: insert_diff1.rows[0].rownum,
                pagenum: insert_diff1.rows[0].pagenum,
                row: vec![
                    Value::I32(1),
                    Value::String("John2".to_string()),
                    Value::I32(30),
                ],
            },
            RowInfo {
                rownum: insert_diff1.rows[2].rownum,
                pagenum: insert_diff1.rows[2].pagenum,
                row: vec![
                    Value::I32(100),
                    Value::String("Joe Schmoe".to_string()),
                    Value::I32(55),
                ],
            },
        ];
        let update_diff1: UpdateDiff = table1.rewrite_rows(rows_to_change).unwrap();
        diffs.push(Diff::Update(update_diff1.clone()));

        // Construct the table from the diffs and then read it in
        construct_tables_from_diffs(&dir_to_build_in, &diffs).unwrap();
        let table2 = Table::new(&dir_to_build_in, &table_name, None).unwrap();

        // Make sure table1 and table2 are the same and that they point to the right directories
        assert!(compare_tables(
            &table1,
            &table2,
            &dir_to_create_in,
            &dir_to_build_in
        ));

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_to_build_in).unwrap();
    }

    #[test]
    fn test_diffs_on_existing_tables() {
        let dir_to_create_in: String = "test_diffs_on_existing_tables".to_string();
        let dir_to_build_in: String = "test_diffs_on_existing_tables_diff".to_string();

        // Create 3 tables that will be act as the existing tables
        let table1_name: String = "test_table1".to_string();
        let table2_name: String = "test_table2".to_string();
        let table3_name: String = "test_table3".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let schema3: Schema = vec![
            ("id".to_string(), Column::I32),
            ("value".to_string(), Column::Double),
        ];
        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table1_name, &schema, &dir_to_create_in).unwrap();
        let mut table1: Table = result.0;
        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table2_name, &schema, &dir_to_create_in).unwrap();
        let mut table2: Table = result.0;
        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table3_name, &schema3, &dir_to_create_in).unwrap();
        let mut table3: Table = result.0;

        // Insert rows into table1
        let rows1: Vec<Row> = vec![
            vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(2),
                Value::String("Jane".to_string()),
                Value::I32(21),
            ],
            vec![
                Value::I32(3),
                Value::String("Joe".to_string()),
                Value::I32(22),
            ],
        ];
        table1.insert_rows(rows1).unwrap();

        // Insert rows into table2
        let rows2: Vec<Row> = vec![vec![
            Value::I32(45),
            Value::String("Bob".to_string()),
            Value::I32(240),
        ]];
        table2.insert_rows(rows2).unwrap();

        // Insert rows into table3
        let rows3: Vec<Row> = vec![
            vec![Value::I32(12), Value::Double(3.14)],
            vec![Value::I32(39), Value::Double(2.718)],
        ];
        table3.insert_rows(rows3).unwrap();

        // Copy the 3 tables to the build directory
        std::fs::create_dir(&dir_to_build_in).unwrap();
        std::fs::copy(
            &format!("{}/{}.db", dir_to_create_in, table1_name),
            &format!("{}/{}.db", dir_to_build_in, table1_name),
        )
        .unwrap();
        std::fs::copy(
            &format!("{}/{}.db", dir_to_create_in, table2_name),
            &format!("{}/{}.db", dir_to_build_in, table2_name),
        )
        .unwrap();
        std::fs::copy(
            &format!("{}/{}.db", dir_to_create_in, table3_name),
            &format!("{}/{}.db", dir_to_build_in, table3_name),
        )
        .unwrap();

        // Create a vector of diffs to apply to the tables in the create directory
        let mut diffs: Vec<Diff> = Vec::new();

        // Update 2 of the inserted rows in table1
        let rows_to_change: Vec<RowInfo> = vec![
            RowInfo {
                rownum: 0,
                pagenum: 0,
                row: vec![
                    Value::I32(1),
                    Value::String("John2".to_string()),
                    Value::I32(30),
                ],
            },
            RowInfo {
                rownum: 2,
                pagenum: 0,
                row: vec![
                    Value::I32(100),
                    Value::String("Joe Schmoe".to_string()),
                    Value::I32(55),
                ],
            },
        ];
        let update_diff1: UpdateDiff = table1.rewrite_rows(rows_to_change).unwrap();
        diffs.push(Diff::Update(update_diff1.clone()));

        // Insert another row in table2
        let rows_to_insert: Vec<Row> = vec![vec![
            Value::I32(46),
            Value::String("Bob2".to_string()),
            Value::I32(241),
        ]];
        let insert_diff1: InsertDiff = table2.insert_rows(rows_to_insert).unwrap();
        diffs.push(Diff::Insert(insert_diff1.clone()));

        // Now delete table2
        let delete_diff1: TableRemoveDiff =
            delete_table_in_dir(&table2.name, &dir_to_create_in).unwrap();
        diffs.push(Diff::TableRemove(delete_diff1.clone()));

        // Insert more rows into table3
        let rows_to_insert: Vec<Row> = vec![
            vec![Value::I32(13), Value::Double(3.141)],
            vec![Value::I32(59), Value::Double(1.23456)],
            vec![Value::I32(40), Value::Double(2.7182)],
        ];
        let insert_diff2: InsertDiff = table3.insert_rows(rows_to_insert).unwrap();
        diffs.push(Diff::Insert(insert_diff2.clone()));

        // Delete a row from table3
        let rows_to_delete: Vec<RowLocation> = vec![RowLocation {
            rownum: insert_diff2.rows[1].rownum,
            pagenum: insert_diff2.rows[0].pagenum,
        }];
        let delete_diff2: RemoveDiff = table3.remove_rows(rows_to_delete).unwrap();
        diffs.push(Diff::Remove(delete_diff2.clone()));

        // Construct the tables from the diffs and then read it in
        construct_tables_from_diffs(&dir_to_build_in, &diffs).unwrap();
        let table1_built = Table::new(&dir_to_build_in, &table1_name, None).unwrap();
        let table2_exists = match Table::new(&dir_to_build_in, &table2_name, None) {
            Ok(_) => true,
            Err(_) => false,
        };
        let table3_built = Table::new(&dir_to_build_in, &table3_name, None).unwrap();

        // Make sure that table1 and table1_built are the same and they point to the right directories
        assert!(compare_tables(
            &table1,
            &table1_built,
            &dir_to_create_in,
            &dir_to_build_in
        ));

        // Make sure that table2 does not exist in the build directory
        assert_eq!(table2_exists, false);

        // Make sure that table3 and table3_built are the same and they point to the right directories
        assert!(compare_tables(
            &table3,
            &table3_built,
            &dir_to_create_in,
            &dir_to_build_in
        ));

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_to_build_in).unwrap();
    }

    #[test]
    fn test_revert_diffs_table() {
        //Setting up test table and directory
        let dir_to_create_in: String = "test_revert_diffs_on_table".to_string();
        let dir_to_build_in: String = "test_revert_diffs_on_table_diff".to_string();
        let dir_to_compare_to: String = "test_revert_compare".to_string();

        // If the directory to the dir_to_build_in does not exist, create it
        let dir = dir_to_build_in.clone() + &"/test".to_string();
        let path_obj = std::path::Path::new(&dir);
        let path_to_build_in_dir = path_obj.parent().unwrap();
        std::fs::create_dir_all(path_to_build_in_dir).unwrap();

        // If the directory to the dir_to_compare_to does not exist, create it
        let dir = dir_to_compare_to.clone() + &"/test".to_string();
        let path_obj = std::path::Path::new(&dir);
        let path_to_compare_to_dir = path_obj.parent().unwrap();
        std::fs::create_dir_all(path_to_compare_to_dir).unwrap();

        let table_name: String = "test_table".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let result: (Table, TableCreateDiff) =
            create_table_in_dir(&table_name, &schema, &dir_to_create_in).unwrap();
        let table_create_diff: TableCreateDiff = result.1;
        let mut table1: Table = result.0;
        let rows = vec![
            vec![
                Value::I32(1),
                Value::String("Dick".to_string()),
                Value::I32(28),
            ],
            vec![
                Value::I32(2),
                Value::String("Jason".to_string()),
                Value::I32(25),
            ],
            vec![
                Value::I32(3),
                Value::String("Tim".to_string()),
                Value::I32(21),
            ],
        ];
        let insert_diff = table1.insert_rows(rows).unwrap();
        let rows2 = vec![vec![
            Value::I32(4),
            Value::String("Damian".to_string()),
            Value::I32(16),
        ]];
        let insert_diff2 = table1.insert_rows(rows2).unwrap();
        std::fs::copy(
            table1.path.clone(),
            format!(
                "{}{}{}.db",
                dir_to_build_in,
                std::path::MAIN_SEPARATOR,
                &table1.name
            ),
        )
        .unwrap();
        let remove_diff = table1
            .remove_rows(vec![RowLocation {
                pagenum: insert_diff.clone().rows[1].pagenum,
                rownum: insert_diff.clone().rows[1].rownum,
            }])
            .unwrap();
        revert_tables_from_diffs(&dir_to_build_in, &vec![Diff::Remove(remove_diff)]).unwrap();
        // Assert that the table is the same as it was before the remove diff
        let table2 = Table::new(&dir_to_build_in, &table_name, None).unwrap();
        let mut diffs: Vec<Diff> = Vec::new();
        diffs.push(Diff::TableCreate(table_create_diff));
        diffs.push(Diff::Insert(insert_diff));
        diffs.push(Diff::Insert(insert_diff2));
        construct_tables_from_diffs(&dir_to_compare_to, &diffs).unwrap();
        let table3 = Table::new(&dir_to_compare_to, &table_name, None).unwrap();
        assert!(compare_tables(
            &table2,
            &table3,
            &dir_to_build_in,
            &dir_to_compare_to
        ));

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_to_build_in).unwrap();
        std::fs::remove_dir_all(&dir_to_compare_to).unwrap();
    }

    #[test]
    fn test_reverting_all_diffs() {
        //Setting up test table and directory
        let dir_to_create_in: String = "test_dir_create_tables".to_string();
        let dir_revert_delete: String = "test_dir_revert_delete".to_string();
        let dir_revert_insert: String = "test_dir_revert_insert".to_string();

        // If the directory to the dir_to_build_in does not exist, create it
        let dir = dir_revert_delete.clone() + &"/test".to_string();
        let path_obj = std::path::Path::new(&dir);
        let path_to_build_in_dir = path_obj.parent().unwrap();
        std::fs::create_dir_all(path_to_build_in_dir).unwrap();

        // If the directory to the dir_to_compare_to does not exist, create it
        let dir = dir_revert_insert.clone() + &"/test".to_string();
        let path_obj = std::path::Path::new(&dir);
        let path_to_compare_to_dir = path_obj.parent().unwrap();
        std::fs::create_dir_all(path_to_compare_to_dir).unwrap();

        // Define schema and names for two tables
        let table1_name: String = "test_table1".to_string();
        let table2_name: String = "test_table2".to_string();
        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("weapon".to_string(), Column::String(50)),
        ];

        // Create the two tables
        let table1_create_results: (Table, TableCreateDiff) =
            create_table_in_dir(&table1_name, &schema1, &dir_to_create_in).unwrap();
        let table1_create_diff: TableCreateDiff = table1_create_results.1;
        let mut table1: Table = table1_create_results.0;
        let table2_create_results: (Table, TableCreateDiff) =
            create_table_in_dir(&table2_name, &schema2, &dir_to_create_in).unwrap();
        let table2_create_diff: TableCreateDiff = table2_create_results.1;
        let mut table2: Table = table2_create_results.0;

        // Insert rows into table1
        let rows1 = vec![
            vec![
                Value::I32(1),
                Value::String("Geralt".to_string()),
                Value::I32(50),
            ],
            vec![
                Value::I32(2),
                Value::String("Ciri".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(3),
                Value::String("Yennefer".to_string()),
                Value::I32(40),
            ],
        ];
        let insert_diff_table1: InsertDiff = table1.insert_rows(rows1).unwrap();

        // Copy the tables into dir_revert_insert
        std::fs::copy(
            table1.path.clone(),
            format!(
                "{}{}{}.db",
                dir_revert_insert,
                std::path::MAIN_SEPARATOR,
                &table1.name
            ),
        )
        .unwrap();
        std::fs::copy(
            table2.path.clone(),
            format!(
                "{}{}{}.db",
                dir_revert_insert,
                std::path::MAIN_SEPARATOR,
                &table2.name
            ),
        )
        .unwrap();

        // Insert rows into table2
        let rows2 = vec![
            vec![
                Value::I32(1),
                Value::String("Geralt".to_string()),
                Value::String("Sword".to_string()),
            ],
            vec![
                Value::I32(2),
                Value::String("Ciri".to_string()),
                Value::String("Sword".to_string()),
            ],
            vec![
                Value::I32(3),
                Value::String("Yennefer".to_string()),
                Value::String("Magic".to_string()),
            ],
        ];
        let insert_diff_table2: InsertDiff = table2.insert_rows(rows2).unwrap();

        // Update a row in table2
        let update_diff_table2: UpdateDiff = table2
            .rewrite_rows(vec![RowInfo {
                row: vec![
                    Value::I32(2),
                    Value::String("Ciri".to_string()),
                    Value::String("Sword/Magic".to_string()),
                ],
                pagenum: insert_diff_table2.clone().rows[1].pagenum,
                rownum: insert_diff_table2.clone().rows[1].rownum,
            }])
            .unwrap();

        // Delete a row in table1
        let delete_diff_table1: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: insert_diff_table1.clone().rows[2].pagenum,
                rownum: insert_diff_table1.clone().rows[2].rownum,
            }])
            .unwrap();

        // Copy the tables into dir_revert_delete
        std::fs::copy(
            table1.path.clone(),
            format!(
                "{}{}{}.db",
                dir_revert_delete,
                std::path::MAIN_SEPARATOR,
                &table1.name
            ),
        )
        .unwrap();
        std::fs::copy(
            table2.path.clone(),
            format!(
                "{}{}{}.db",
                dir_revert_delete,
                std::path::MAIN_SEPARATOR,
                &table2.name
            ),
        )
        .unwrap();

        // Remove both tables now
        let remove_table1_diff: TableRemoveDiff =
            delete_table_in_dir(&table1_name, &dir_to_create_in).unwrap();
        let remove_table2_diff: TableRemoveDiff =
            delete_table_in_dir(&table2_name, &dir_to_create_in).unwrap();

        let remove_table_diffs: Vec<Diff> = vec![
            Diff::TableRemove(remove_table1_diff),
            Diff::TableRemove(remove_table2_diff),
        ];

        // Revert the remove tables in the dir_to_create_in directory
        revert_tables_from_diffs(&dir_to_create_in, &remove_table_diffs).unwrap();

        // Make sure the reverted tables are the same as the original tables
        let table1: Table = Table::new(&dir_to_create_in, &table1_name, None).unwrap();
        let table1_reverted: Table = Table::new(&dir_revert_delete, &table1_name, None).unwrap();
        assert!(compare_tables(
            &table1,
            &table1_reverted,
            &dir_to_create_in,
            &dir_revert_delete
        ));
        let table2: Table = Table::new(&dir_to_create_in, &table2_name, None).unwrap();
        let table2_reverted: Table = Table::new(&dir_revert_delete, &table2_name, None).unwrap();
        assert!(compare_tables(
            &table2,
            &table2_reverted,
            &dir_to_create_in,
            &dir_revert_delete
        ));

        // Revert the insert rows in the dir_to_create_in directory
        let insert_diffs: Vec<Diff> = vec![
            Diff::Insert(insert_diff_table2),
            Diff::Update(update_diff_table2),
            Diff::Remove(delete_diff_table1),
        ];
        revert_tables_from_diffs(&dir_to_create_in, &insert_diffs).unwrap();

        // Make sure the reverted tables are the same as the original tables
        let table1: Table = Table::new(&dir_to_create_in, &table1_name, None).unwrap();
        let table1_reverted: Table = Table::new(&dir_revert_insert, &table1_name, None).unwrap();
        assert!(compare_tables(
            &table1,
            &table1_reverted,
            &dir_to_create_in,
            &dir_revert_insert
        ));
        let table2: Table = Table::new(&dir_to_create_in, &table2_name, None).unwrap();
        let table2_reverted: Table = Table::new(&dir_revert_insert, &table2_name, None).unwrap();
        assert!(compare_tables(
            &table2,
            &table2_reverted,
            &dir_to_create_in,
            &dir_revert_insert
        ));

        // Revert the create tables in the dir_to_create_in directory
        let create_table_diffs: Vec<Diff> = vec![
            Diff::TableCreate(table1_create_diff),
            Diff::TableCreate(table2_create_diff),
            Diff::Insert(insert_diff_table1),
        ];
        revert_tables_from_diffs(&dir_to_create_in, &create_table_diffs).unwrap();

        // Make sure that the dir_to_create_in directory is empty
        assert!(std::fs::read_dir(&dir_to_create_in)
            .unwrap()
            .next()
            .is_none());

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
        std::fs::remove_dir_all(&dir_revert_delete).unwrap();
        std::fs::remove_dir_all(&dir_revert_insert).unwrap();
    }

    #[test]
    fn test_reverting_every_diffs() {
        //Setting up test table and directory
        let dir_to_create_in: String = "test_dir_reverting_every_diffs".to_string();

        // Define schema and names for two tables
        let table1_name: String = "test_table1".to_string();
        let table2_name: String = "test_table2".to_string();
        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("weapon".to_string(), Column::String(50)),
        ];

        // Create the two tables
        let table1_create_results: (Table, TableCreateDiff) =
            create_table_in_dir(&table1_name, &schema1, &dir_to_create_in).unwrap();
        let table1_create_diff: TableCreateDiff = table1_create_results.1;
        let mut table1: Table = table1_create_results.0;
        let table2_create_results: (Table, TableCreateDiff) =
            create_table_in_dir(&table2_name, &schema2, &dir_to_create_in).unwrap();
        let table2_create_diff: TableCreateDiff = table2_create_results.1;
        let mut table2: Table = table2_create_results.0;

        // Insert rows into table1
        let rows1 = vec![
            vec![
                Value::I32(1),
                Value::String("Geralt".to_string()),
                Value::I32(50),
            ],
            vec![
                Value::I32(2),
                Value::String("Ciri".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(3),
                Value::String("Yennefer".to_string()),
                Value::I32(40),
            ],
        ];
        let insert_diff_table1: InsertDiff = table1.insert_rows(rows1).unwrap();

        // Insert rows into table2
        let rows2 = vec![
            vec![
                Value::I32(1),
                Value::String("Geralt".to_string()),
                Value::String("Sword".to_string()),
            ],
            vec![
                Value::I32(2),
                Value::String("Ciri".to_string()),
                Value::String("Sword".to_string()),
            ],
            vec![
                Value::I32(3),
                Value::String("Yennefer".to_string()),
                Value::String("Magic".to_string()),
            ],
        ];
        let insert_diff_table2: InsertDiff = table2.insert_rows(rows2).unwrap();

        // Update a row in table2
        let update_diff_table2: UpdateDiff = table2
            .rewrite_rows(vec![RowInfo {
                row: vec![
                    Value::I32(2),
                    Value::String("Ciri".to_string()),
                    Value::String("Sword/Magic".to_string()),
                ],
                pagenum: insert_diff_table2.clone().rows[1].pagenum,
                rownum: insert_diff_table2.clone().rows[1].rownum,
            }])
            .unwrap();

        // Delete a row in table1
        let delete_diff_table1: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: insert_diff_table1.clone().rows[2].pagenum,
                rownum: insert_diff_table1.clone().rows[2].rownum,
            }])
            .unwrap();

        // Remove both tables now
        let remove_table1_diff: TableRemoveDiff =
            delete_table_in_dir(&table1_name, &dir_to_create_in).unwrap();
        let remove_table2_diff: TableRemoveDiff =
            delete_table_in_dir(&table2_name, &dir_to_create_in).unwrap();

        // Create a vector of all the diffs
        let diffs: Vec<Diff> = vec![
            Diff::TableCreate(table1_create_diff),
            Diff::TableCreate(table2_create_diff),
            Diff::Insert(insert_diff_table1),
            Diff::Insert(insert_diff_table2),
            Diff::Update(update_diff_table2),
            Diff::Remove(delete_diff_table1),
            Diff::TableRemove(remove_table1_diff),
            Diff::TableRemove(remove_table2_diff),
        ];

        // Revert all the diffs
        revert_tables_from_diffs(&dir_to_create_in, &diffs).unwrap();

        // Make sure that the dir_to_create_in directory is empty
        assert!(std::fs::read_dir(&dir_to_create_in)
            .unwrap()
            .next()
            .is_none());

        // Clean up directories
        std::fs::remove_dir_all(&dir_to_create_in).unwrap();
    }

    /// Compares two tables to make sure that they are identical, but in separate directories
    fn compare_tables(
        table1: &Table,
        table2: &Table,
        table1dir: &String,
        table2dir: &String,
    ) -> bool {
        if table1dir == table2dir {
            return false;
        }

        // Make sure that table3 and table3_built are the same and they point to the right directories
        if std::path::Path::new(&table1.path)
            != std::path::Path::new(&format!("{}/{}.db", table1dir, table1.name))
        {
            return false;
        }

        if std::path::Path::new(&table2.path)
            != std::path::Path::new(&format!("{}/{}.db", table2dir, table1.name))
        {
            return false;
        }

        if !diff(&table1.path, &table2.path) {
            return false;
        }
        true
    }
}
