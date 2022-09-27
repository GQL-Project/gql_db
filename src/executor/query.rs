use crate::util::dbtype::{Column, Value};
use crate::fileio::{databaseio::*, tableio::*, header::*};
use itertools::Itertools;

/// A parse function, that starts with a string and returns either a table for query commands
/// or a string for 
pub fn execute(ast: &String, _update: bool) -> Result<String, String> {
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
        database.get_table_path(table_name.to_string())?;
        let table_dir: String = database.get_current_branch_path();
        tables.push((Table::new(&table_dir, &table_name, None)?, alias.clone()));
    }

    // Create an iterator of table iterators using the cartesion product of the tables :)
    let table_iterator = tables.iter()
        .map(|x| x.0.clone()).multi_cartesian_product();

    // Get the names of all the columns in the tables along with their aliases in
    // the format <alias>.<column_name> and store them in a vector of tuples
    // alongside their column types and new column name when output.
    // It will be a vector of tuples where each tuple is of the form:
    // (<table_alias>.<column_name>, <column_type>, <output_column_name>)
    // This is where the fun begins... ;)
    let table_column_names: Vec<(String, Column, String)> = tables.iter()
        .map(|x: &(Table, String)| x.0.schema.iter()
            .map(|y: &(String, Column)| (format!("{}.{}", x.1, y.0.clone()), 
                                         y.1.clone(), 
                                         y.0.clone()))
            .collect::<Vec<(String, Column, String)>>())
        .flatten()
        .collect::<Vec<(String, Column, String)>>();

    // We need to take all the columns
    if is_star_cols {
        // Add all columns to the selected schema
        for (_, col_type, output_col_name) in table_column_names {
            selected_schema.push((output_col_name, col_type));
        }

        // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
        for table_rows in table_iterator {
            // Accumulate all the cells across the vector of rows into a single vector
            let mut selected_cells: Vec<Value> = Vec::new();
            table_rows.iter().for_each(|x| selected_cells.extend(x.row.clone()));

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
                    let (_, col_type, output_col_name) = table_column_names.get(x).unwrap();
                    selected_schema.push((output_col_name.clone(), col_type.clone()));
                },
                None => return Err(format!("Column {} does not exist in any of the tables", desired_column))
            }
        }

        // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
        for table_rows in table_iterator {
            // Flatten the entire output row, but it includes all columns from all tables
            let mut output_row: Vec<Value> = Vec::new();
            for row_info in table_rows {
                output_row.extend(row_info.row.clone());
            }

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
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_select_single_table_star() {
        // This tests 
        // SELECT * FROM select_test_db.test_table1
        let columns = ["*".to_string()];
        let tables = [("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]
    
        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();
    
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let mut table1: Table = create_table(&"test_table1".to_string(), &schema.clone(), &new_db.clone()).unwrap().0;
        
        let row1 = vec![
            Value::I32(1),
            Value::String("Iron Man".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Spiderman".to_string()),
            Value::I32(20),
        ];
        insert_rows(&mut table1, [row1, row2].to_vec()).unwrap();
        
        let result = select(&columns.to_owned(), &tables, new_db.clone()).unwrap();

        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));
        assert_eq!(result.0[2], ("age".to_string(), Column::I32));

        // Assert that 2 rows were returned
        assert_eq!(result.1.iter().len(), 2);

        // Assert that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(1));
        assert_eq!(result.1[0][1], Value::String("Iron Man".to_string()));
        assert_eq!(result.1[0][2], Value::I32(40));

        // Assert that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(2));
        assert_eq!(result.1[1][1], Value::String("Spiderman".to_string()));
        assert_eq!(result.1[1][2], Value::I32(20));

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_select_multi_table_star() {
        // This tests:
        // SELECT * FROM select_test_db.test_table1, select_test_db.test_table2
        let columns = ["*".to_string()];
        let tables = [("test_table1".to_string(), "T1".to_string()),
                                             ("test_table2".to_string(), "T2".to_string())]; // [(table_name, alias)]
    
        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();
    
        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("country".to_string(), Column::String(50)),
        ];
        let mut table1: Table = create_table(&"test_table1".to_string(), &schema1.clone(), &new_db.clone()).unwrap().0;
        let mut table2: Table = create_table(&"test_table2".to_string(), &schema2.clone(), &new_db.clone()).unwrap().0;
        
        // Write rows to first table
        let row1 = vec![
            Value::I32(1),
            Value::String("Robert Downey Jr.".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Tom Holland".to_string()),
            Value::I32(20),
        ];
        insert_rows(&mut table1, [row1, row2].to_vec()).unwrap();

        // Write rows to second table
        let row1 = vec![
            Value::I32(1),
            Value::String("United States".to_string()),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Britain".to_string()),
        ];
        insert_rows(&mut table2, [row1, row2].to_vec()).unwrap();
        
        // Run the SELECT query
        let result = select(&columns.to_owned(),
                                                             &tables, 
                                                             new_db.clone()).unwrap();

        // Check that the schema is correct
        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));
        assert_eq!(result.0[2], ("age".to_string(), Column::I32));
        assert_eq!(result.0[3], ("id".to_string(), Column::I32));
        assert_eq!(result.0[4], ("country".to_string(), Column::String(50)));

        // Check that we returned 4 rows
        assert_eq!(result.1.iter().len(), 4);

        // Check that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(1));
        assert_eq!(result.1[0][1], Value::String("Robert Downey Jr.".to_string()));
        assert_eq!(result.1[0][2], Value::I32(40));
        assert_eq!(result.1[0][3], Value::I32(1));
        assert_eq!(result.1[0][4], Value::String("United States".to_string()));

        // Check that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(1));
        assert_eq!(result.1[1][1], Value::String("Robert Downey Jr.".to_string()));
        assert_eq!(result.1[1][2], Value::I32(40));
        assert_eq!(result.1[1][3], Value::I32(2));
        assert_eq!(result.1[1][4], Value::String("Britain".to_string()));

        // Check that the third row is correct
        assert_eq!(result.1[2][0], Value::I32(2));
        assert_eq!(result.1[2][1], Value::String("Tom Holland".to_string()));
        assert_eq!(result.1[2][2], Value::I32(20));
        assert_eq!(result.1[2][3], Value::I32(1));
        assert_eq!(result.1[2][4], Value::String("United States".to_string()));

        // Check that the fourth row is correct
        assert_eq!(result.1[3][0], Value::I32(2));
        assert_eq!(result.1[3][1], Value::String("Tom Holland".to_string()));
        assert_eq!(result.1[3][2], Value::I32(20));
        assert_eq!(result.1[3][3], Value::I32(2));
        assert_eq!(result.1[3][4], Value::String("Britain".to_string()));

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_select_single_table_specific_columns() {
        // This tests 
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = ["T.id".to_string(), "T.name".to_string()];
        let tables = [("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]
    
        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();
    
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let mut table1: Table = create_table(&"test_table1".to_string(), &schema.clone(), &new_db.clone()).unwrap().0;
        
        let row1 = vec![
            Value::I32(1),
            Value::String("Iron Man".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Spiderman".to_string()),
            Value::I32(20),
        ];
        let row3 = vec![
            Value::I32(3),
            Value::String("Doctor Strange".to_string()),
            Value::I32(35),
        ];
        insert_rows(&mut table1, [row1, row2, row3].to_vec()).unwrap();
        
        // Run the SELECT query
        let result = select(&columns.to_owned(), &tables, new_db.clone()).unwrap();

        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));

        // Assert that 3 rows were returned
        assert_eq!(result.1.iter().len(), 3);

        // Assert that each row only has 2 columns
        for row in result.1.clone() {
            assert_eq!(row.len(), 2);
        }

        // Assert that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(1));
        assert_eq!(result.1[0][1], Value::String("Iron Man".to_string()));

        // Assert that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(2));
        assert_eq!(result.1[1][1], Value::String("Spiderman".to_string()));

        // Assert that the third row is correct
        assert_eq!(result.1[2][0], Value::I32(3));
        assert_eq!(result.1[2][1], Value::String("Doctor Strange".to_string()));

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_select_multiple_tables_specific_columns() {
        // This tests 
        // SELECT T1.id, T2.country FROM select_test_db.test_table1 T1, select_test_db.test_table2 T2;
        let columns = ["T1.id".to_string(), "T2.country".to_string()];
        let tables = [("test_table1".to_string(), "T1".to_string()),
                                             ("test_table2".to_string(), "T2".to_string())]; // [(table_name, alias)]
    
        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();
    
        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let mut table1: Table = create_table(&"test_table1".to_string(), &schema1.clone(), &new_db.clone()).unwrap().0;
        
        let row1 = vec![
            Value::I32(1),
            Value::String("Robert Downey Jr.".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Tom Holland".to_string()),
            Value::I32(20),
        ];
        insert_rows(&mut table1, [row1, row2].to_vec()).unwrap();
        
        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("country".to_string(), Column::String(50)),
        ];
        let mut table2: Table = create_table(&"test_table2".to_string(), &schema2.clone(), &new_db.clone()).unwrap().0;
        
        let row1 = vec![
            Value::I32(5),
            Value::String("United States".to_string()),
        ];
        let row2 = vec![
            Value::I32(6),
            Value::String("Britain".to_string()),
        ];
        insert_rows(&mut table2, [row1, row2].to_vec()).unwrap();
        
        // Run the SELECT query
        let result = select(&columns.to_owned(), &tables, new_db.clone()).unwrap();

        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("country".to_string(), Column::String(50)));

        // Assert that 4 rows were returned
        assert_eq!(result.1.iter().len(), 4);

        // Assert that each row only has 2 columns
        for row in result.1.clone() {
            assert_eq!(row.len(), 2);
        }

        // Assert that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(1));
        assert_eq!(result.1[0][1], Value::String("United States".to_string()));

        // Assert that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(1));
        assert_eq!(result.1[1][1], Value::String("Britain".to_string()));

        // Assert that the third row is correct
        assert_eq!(result.1[2][0], Value::I32(2));
        assert_eq!(result.1[2][1], Value::String("United States".to_string()));

        // Assert that the fourth row is correct
        assert_eq!(result.1[3][0], Value::I32(2));
        assert_eq!(result.1[3][1], Value::String("Britain".to_string()));

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_column_select() {
        // This tests
        // SELECT id, name, age, invalid_column FROM select_test_db.test_table1;

        let columns = ["id".to_string(), "name".to_string(), "age".to_string(), "invalid_column".to_string()];
        let tables = [("test_table1".to_string(), "".to_string())]; // [(table_name, alias)]

        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let mut table1: Table = create_table(&"test_table1".to_string(), &schema1.clone(), &new_db.clone()).unwrap().0;

        let row1 = vec![
            Value::I32(1),
            Value::String("Robert Downey Jr.".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Tom Holland".to_string()),
            Value::I32(20),
        ];
        insert_rows(&mut table1, [row1, row2].to_vec()).unwrap();

        // Run the SELECT query
        let result = select(&columns.to_owned(), &tables, new_db.clone());

        // Verify that SELECT failed
        assert!(result.is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_table_select() {
        // This tests
        // SELECT id, name, age FROM select_test_db.test_table1, select_test_db.test_table2;

        let columns = ["id".to_string(), "name".to_string(), "age".to_string()];
        let tables = [("test_table1".to_string(), "".to_string()),
                                             ("test_table2".to_string(), "".to_string())]; // [(table_name, alias)]

        let mut new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let mut table1: Table = create_table(&"test_table1".to_string(), &schema1.clone(), &new_db.clone()).unwrap().0;

        let row1 = vec![
            Value::I32(1),
            Value::String("Robert Downey Jr.".to_string()),
            Value::I32(40),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Tom Holland".to_string()),
            Value::I32(20),
        ];
        insert_rows(&mut table1, [row1, row2].to_vec()).unwrap();

        // Run the SELECT query
        let result = select(&columns.to_owned(), &tables, new_db.clone());

        // Verify that SELECT failed
        assert!(result.is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }
}