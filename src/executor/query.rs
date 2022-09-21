use crate::util::dbtype::{Column, Value};
use crate::fileio::{databaseio::*, tableio::*, header::*, rowio::*, pageio::*};
use itertools::Itertools;

/// A parse function, that starts with a string and returns either a table for query commands
/// or a string for 
pub fn execute(ast: &String, update: bool) -> Result<String, String> {
    if ast.len() == 0 {
        return Err("Empty AST".to_string());
    }

    Ok("0".to_string())
}


/// This method implements the SQL Select statement. It takes in the column and table names where table_names
/// is an array of tuples where the first element is the table name and the second element is the alias.
/// It returns a tuple containing the schema and the rows of the resulting table.
pub fn select(column_names: &[String], table_names: &[(String, String)], database: Database) -> Result<(Schema, Vec<Vec<Value>>), String> {
    if table_names.len() == 0 || column_names.len() == 0 {
        return Err("Malformed SELECT Command".to_string());
    }

    // Whether the select statement used '*' to select columns or not
    let is_star_cols: bool = column_names.get(0).unwrap().eq(&"*".to_string());

    // The rows and schema that are to be returned from the select statement
    let mut selected_rows: Vec<Vec<Value>> = Vec::new();
    let mut selected_schema: Schema = Vec::new();
    
    // Read in the tables into a vector of tuples where they are represented as (table, alias)
    let mut tables: Vec<(Table, String)> = Vec::new();
    for (table_name, alias) in table_names {
        let table_path: String = format!("{}/{}.db", database.path, table_name);
        tables.push((Table::new(table_path)?, alias.clone()));
    }

    // Create an iterator of table iterators using the cartesion product of the tables :)
    let table_iterator = tables.iter()
        .map(|x| x.0.clone()).multi_cartesian_product();

    // Get the names of all the columns in the tables along with their aliases in
    // the format <alias>.<column_name> and store them in a vector of tuples
    // alongside their column types.
    // This is where the fun begins... ;)
    let table_column_names: Vec<(String, Column)> = tables.iter()
        .map(|x| x.0.schema.iter()
            .map(|y| (format!("{}.{}", x.1, y.0), y.1.clone()))
            .collect::<Vec<(String, Column)>>())
        .flatten()
        .collect::<Vec<(String, Column)>>();

    // We need to take all the columns
    if is_star_cols {
        // Add all columns to the selected schema
        for (name, col_type) in table_column_names {
            selected_schema.push((name, col_type));
        }

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
        // Get the indices of the columns we want to select
        let mut table_column_indices: Vec<usize> = Vec::new();
        for desired_column in column_names {
            let index = table_column_names.iter().position(|x| x.0.eq(desired_column));
            // Check that index is valid
            match index {
                Some(x) => {
                    table_column_indices.push(x);
                    selected_schema.push(table_column_names.get(x).unwrap().clone());
                },
                None => return Err(format!("Column {} does not exist in any of the tables", desired_column))
            }
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

    Ok((selected_schema, selected_rows))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::dbtype::{Column, Value};

    #[test]
    fn test_select_single_table_star() {
        // This tests 'SELECT * FROM select_test_db.test_table1'
        let columns = ["*".to_string()];
        let tables = [("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]
    
        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();
    
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let table1: Table = create_table("test_table1".to_string(), schema.clone(), new_db.clone()).unwrap();
        let table2: Table = create_table("test_table2".to_string(), schema.clone(), new_db.clone()).unwrap();
        
        let row1 = vec![
            Value::I32(1),
            Value::String("Iron Man".to_string()),
            Value::I32(40),
        ];
        let mut page = [0u8; PAGE_SIZE];
        insert_row(&schema, &mut page, &row1).unwrap();
        let row2 = vec![
            Value::I32(2),
            Value::String("Spiderman".to_string()),
            Value::I32(20),
        ];
        insert_row(&schema, &mut page, &row2).unwrap();
        write_page(1, &table1.path, &page).unwrap();
        
        println!("{:?}", select(&columns.to_owned(), &tables, new_db.clone()).unwrap());
        let result = select(&columns.to_owned(), &tables, new_db.clone()).unwrap();

        assert_eq!(result.0[0], ("T.id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("T.name".to_string(), Column::String(50)));
        assert_eq!(result.0[2], ("T.age".to_string(), Column::I32));

        for (i, row) in result.1.iter().enumerate() {
            if i == 0 {
                assert_eq!(row[0], Value::I32(1));
                assert_eq!(row[1], Value::String("Iron Man".to_string()));
                assert_eq!(row[2], Value::I32(40));
            } else if i == 1 {
                assert_eq!(row[0], Value::I32(2));
                assert_eq!(row[1], Value::String("Spiderman".to_string()));
                assert_eq!(row[2], Value::I32(20));
            }
        }

    }
}