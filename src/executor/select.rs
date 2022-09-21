use std::fmt::format;
use std::collections::HashMap;
use itertools::Itertools;
use crate::{fileio::{*, header::Header, tableio::Table, databaseio::*}, util::dbtype::Value};

/// This method implements the SQL Select statement. It takes in the column and table names where table_names
/// is an array of tuples where the first element is the table name and the second element is the alias.
pub fn select(column_names: &[String], table_names: &[(String, String)], database: Database) -> Result<String, String> {
    if table_names.len() == 0 || column_names.len() == 0 {
        return Err("Malformed SELECT Command".to_string());
    }

    // Whether the select statement used '*' to select columns or not
    let is_star_cols: bool = column_names.get(0).unwrap().eq(&"*".to_string());

    let mut selected_rows: Vec<Vec<Value>> = Vec::new();
    
    // We only have to select columns from one table
    if table_names.len() == 1 {
        // Read in the table
        let table_name: &String = &table_names.get(0).unwrap().0;
        let table_path: String = format!("{}/{}.db", database.path, table_name);
        let table: Table = Table::new(table_path)?;

        // We need to take all the columns
        if is_star_cols {
            for row in table {
                selected_rows.push(row.clone());
            }
        }
        // We need to take a subset of columns
        else {
            // Get the names of all the columns in the table
            let table_column_names: Vec<String> = table.schema.iter()
                .map(|x| x.0.clone())
                .collect::<Vec<String>>();

            // Get the indices of the columns we want to select
            let mut table_column_indices: Vec<usize> = Vec::new();
            for desired_column in column_names {
                let index = table_column_names.iter().position(|x| x.eq(desired_column));
                if index.is_none() {
                    return Err(format!("Column {} does not exist in table {}", desired_column, table_name));
                }
                table_column_indices.push(index.unwrap());
            }

            // Iterate through all the rows in the table and select the columns we want from each row
            for row in table {
                let mut selected_cells: Vec<Value> = Vec::new();
                for (i, row_cell) in row.iter().enumerate() {
                    if table_column_indices.contains(&i) {
                        selected_cells.push(row_cell.clone());
                    }
                }

                // Append the selected_cells row to our result
                selected_rows.push(selected_cells.clone());
            }
        }
    }
    // We have to select columns from multiple tables
    else {
        // Read in the tables into a vector of tuples where they are represented as (table, alias)
        let mut tables: Vec<(Table, String)> = Vec::new();
        for (table_name, alias) in table_names {
            let table_path: String = format!("{}/{}.db", database.path, table_name);
            tables.push((Table::new(table_path)?, alias.clone()));
        }

        // Create an iterator of table iterators using the cartesion product of the tables :)
        let table_iterator = tables.iter()
            .map(|x| x.0.clone()).multi_cartesian_product();

        // We need to take all the columns
        if is_star_cols {
            // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
            for table_rows in table_iterator {
                // Accumulate all the cells across the vector of rows into a single vector
                let mut selected_cells: Vec<Value> = Vec::new();
                table_rows.iter().for_each(|x| selected_cells.extend(x.clone()));

                // Append the selected_cells row to our result
                selected_rows.push(selected_cells.clone());
            }
        }
        // We need to take a subset of columns
        else {
            // Get the names of all the columns in the tables along with their aliases in
            // the format <alias>.<column_name> and store them in a vector.
            let table_column_names: Vec<String> = tables.iter()
                .map(|x| x.0.schema.iter()
                    .map(|y| format!("{}.{}", x.1, y.0))
                    .collect::<Vec<String>>())
                .flatten()
                .collect::<Vec<String>>();

            // Get the indices of the columns we want to select
            let mut table_column_indices: Vec<usize> = Vec::new();
            for desired_column in column_names {
                let index = table_column_names.iter().position(|x| x.eq(desired_column));
                if index.is_none() {
                    println!("Failed");
                    return Err(format!("Column {} does not exist in any of the tables", desired_column));
                }
                table_column_indices.push(index.unwrap());
            }

            // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
            for table_rows in table_iterator {
                // Flatten the entire output row, but it includes all columns from all tables
                let output_row: Vec<Value> = table_rows.into_iter().flatten().collect();

                // Iterate through the output row and only select the columns we want
                let mut selected_cells: Vec<Value> = Vec::new();
                for (i, row_cell) in output_row.iter().enumerate() {
                    if table_column_indices.contains(&i) {
                        selected_cells.push(row_cell.clone());
                    }
                }

                // Append the selected_cells row to our result
                selected_rows.push(selected_cells.clone());
            }
        }
    }

    println!("Selected Rows: {:?}", selected_rows);
    Ok("Done".to_string())
}