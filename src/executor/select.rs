use std::fmt::format;
use crate::{fileio::{*, header::Header, tableio::Table}, util::dbtype::Value};

/// This method implements the SQL Select statement. It takes in the columns and tables
/// and outputs a table.
pub fn select(columns: &[String], tables: &[String], database: &String) -> Result<String, String> {
    if tables.len() == 0 || columns.len() == 0 {
        return Err("Malformed SELECT Command".to_string());
    }

    /*
    // The names of all the columns we are going to select from the tables.
    // The names are stored as <table_name>.<column_name>
    let mut column_names: Vec<String> = Vec::new();

    // We are getting all the columns from the tables
    if columns.get(0).unwrap() == &"*".to_string() {
        for table in tables {
            let db_path = format!("{}/{}.db", database, table);
            let header = header::read_header(&db_path)?;
            for (colname, _) in header.schema {
                let full_colname = format!("{}.{}", table, colname);
                column_names.push(full_colname);
            }
        }
    }
    // We are only getting the columns specified by the columns parameter
    else {
        for colname in columns {
            column_names.push(colname.to_string());
        }
    }

    // Now all column names we want are stored in column_names
    */

    // Whether the select statement used '*' to select columns or not
    let is_star_cols: bool = columns.get(0).unwrap().eq(&"*".to_string());

    let mut selected_rows: Vec<Vec<Value>> = Vec::new();
    
    // We only have to select columns from one table
    if tables.len() == 1 {
        // Read in the table
        let table_name: &String = tables.get(0).unwrap();
        let table_path: String = format!("{}/{}.db", database, table_name);
        let table: Table = Table::new(table_path)?;

        // We need to take all the columns
        if is_star_cols {
            for row in table {
                println!("{:?}", row);
                selected_rows.push(row.clone());
            }
        }
        // We need to take a subset of columns
        else {
            // Get the names of columns we want to select
            let table_column_names: Vec<String> = table.schema.iter()
                .map(|x| x.0.clone())
                .collect::<Vec<String>>();

            for row in table {
                let selected_row: Vec<Value> = Vec::new();
                for (i, row_cell) in row.iter().enumerate() {
                    
                }
                println!("{:?}", selected_row);
                selected_rows.push(selected_row.clone());
            }
        }
    }
    // We have to select columns from multiple tables
    else {

    }
    

    Ok("Done".to_string())
}