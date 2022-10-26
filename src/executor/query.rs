use std::collections::HashMap;

use super::predicate::{resolve_predicate, solve_predicate, SolvePredicate};
use crate::fileio::{
    databaseio::*,
    header::*,
    tableio::{self, *},
};
use crate::user::userdata::*;
use crate::util::dbtype::Column;
use crate::util::row::Row;
use crate::version_control::diff::*;
use itertools::Itertools;
use sqlparser::ast::{Expr, SetExpr, Statement};

/// A parse function, that starts with a string and returns either a table for query commands
/// or a string for
pub fn execute_query(
    ast: &Vec<Statement>,
    user: &mut User,
    command: &String,
) -> Result<(Schema, Vec<Row>), String> {
    if ast.len() == 0 {
        return Err("Empty AST".to_string());
    }
    for a in ast.iter() {
        match a {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => {
                    let mut column_names = Vec::new();
                    for c in s.projection.iter() {
                        column_names.push(c.to_string());
                    }
                    let mut table_names = Vec::new();
                    for t in s.from.iter() {
                        let table_name = t.to_string();
                        let table_name: Vec<&str> = table_name.split(" ").collect();
                        if table_name.len() == 3 {
                            table_names
                                .push((table_name[0].to_string(), table_name[2].to_string()));
                        } else {
                            table_names.push((table_name[0].to_string(), "".to_string()));
                        }
                    }
                    user.append_command(&command);
                    let pred: Option<SolvePredicate> = match &s.selection {
                        Some(pred) => Some(where_clause(
                            pred,
                            table_names.clone(),
                            get_db_instance()?,
                            user,
                        )?),
                        None => None,
                    };
                    return select(column_names, pred, table_names, get_db_instance()?, user);
                }
                _ => print!("Not a select\n"),
            },
            _ => print!("Not a query\n"),
        };
    }
    Err("No query found".to_string())
}

pub fn execute_update(
    ast: &Vec<Statement>,
    user: &mut User,
    command: &String,
) -> Result<String, String> {
    if ast.len() == 0 {
        return Err("Empty AST".to_string());
    }
    let mut results: Vec<String> = Vec::new();
    // Commands: create, insert, select
    for a in ast.iter() {
        match a {
            Statement::Update {
                table,
                assignments,
                from,
                selection
            } => {
                println!("table: {:?}", table);
                println!("assignments: {:?}", assignments);
                println!("from: {:?}", from);
                println!("selection: {:?}", selection);
                /* TODO: 1. Get Table name //
                         2. Parse assignments into Vector
                 */
                let mut final_table = String::from("test"); // What is the best way to do this?
                //let mut all_data = Vec::new();

                match table.relation.clone() {
                    sqlparser::ast::TableFactor::Table{
                        name: table_name,
                        alias: alias,
                        args: args,
                        with_hints: with_hints
                    } => {
                        // Now you have the table
                        final_table = table_name.to_string();
                    },
                    _ => {
                        // Not a table inside the TableFactor enum
                    }
                }
                //println!("table: {:?}", table_name);
                println!("Table: {:?}", final_table);
                


            }
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.0[0].value.to_string();
                let mut schema = Schema::new();

                for c in columns.iter() {
                    schema.push((c.name.value.clone(), Column::from_col_def(c)?));
                }
                let _result = create_table(&table_name, &schema, get_db_instance()?, user)?;
                results.push(format!("Table created: {}", table_name));
            }
            Statement::Insert {
                table_name,
                columns: _,
                source,
                ..
            } => {
                let table_name = table_name.0[0].value.to_string();
                let mut all_data = Vec::new();
                match *source.body.clone() {
                    SetExpr::Values(values) => {
                        let values_list = values.0;
                        for row in values_list {
                            let mut data = Vec::new();
                            for k in row {
                                match k {
                                    Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                                        data.push(s);
                                    }
                                    Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => {
                                        data.push(s);
                                    }
                                    Expr::Value(sqlparser::ast::Value::Number(s, _)) => {
                                        data.push(s);
                                    }
                                    Expr::Value(sqlparser::ast::Value::Boolean(s)) => {
                                        data.push(s.to_string());
                                    }
                                    Expr::Value(sqlparser::ast::Value::Null) => {
                                        data.push("".to_string());
                                    }
                                    _ => println!("Unexpected Value"),
                                }
                            }
                            all_data.push(data);
                            println!("data: {:?}", all_data);
                        }
                    }
                    _ => {
                        return Err("Expected a Values statement".to_string());
                    }
                }
                results.push(insert(all_data, table_name, get_db_instance()?, user)?.0);
            }
            _ => {
                return Err(format!("Not a valid command: {0}", a));
            }
        }
    }
    if results.len() == 0 {
        Err("No command found".to_string())
    } else {
        user.append_command(command);
        Ok(results.join("\n"))
    }
}

/// Creates a new table within the given database named <table_name><TABLE_FILE_EXTENSION>
/// with the given schema.
/// It appends the diff to the user passed in
pub fn create_table(
    table_name: &String,
    schema: &Schema,
    database: &Database,
    user: &mut User,
) -> Result<(Table, TableCreateDiff), String> {
    let table_dir: String = database.get_current_working_branch_path(&user);

    // Create a table file and return it
    let results = tableio::create_table(table_name, schema, &table_dir)?;
    user.append_diff(&Diff::TableCreate(results.1.clone()));
    Ok(results)
}

/// Drops a table from the given database.
/// It appends the diff to the user passed in
pub fn drop_table(
    table_name: &String,
    database: &Database,
    user: &mut User,
) -> Result<TableRemoveDiff, String> {
    let table_dir: String = database.get_current_working_branch_path(user);

    // Delete the table file and return it
    let results = delete_table_in_dir(table_name, &table_dir)?;
    user.append_diff(&Diff::TableRemove(results.clone()));
    Ok(results)
}

/// This method implements the SQL Select statement. It takes in the column and table names where table_names
/// is an array of tuples where the first element is the table name and the second element is the alias.
/// It returns a tuple containing the schema and the rows of the resulting table.
pub fn select(
    column_names: Vec<String>,
    pred: Option<SolvePredicate>,
    table_names: Vec<(String, String)>,
    database: &Database,
    user: &User, // If a user is present, query that user's branch. Otherwise, query main branch
) -> Result<(Schema, Vec<Row>), String> {
    if table_names.len() == 0 || column_names.len() == 0 {
        return Err("Malformed SELECT Command".to_string());
    }

    // Whether the select statement used '*' to select columns or not
    let is_star_cols: bool = column_names.contains(&"*".to_string());

    // The rows and schema that are to be returned from the select statement
    let mut selected_rows: Vec<Row> = Vec::new();
    let mut selected_schema: Schema = Vec::new();

    let tables = load_aliased_tables(database, user, table_names)?;

    // This is where the fun begins... ;)
    let table_column_names = gen_column_aliases(&tables);

    // Create an iterator of table iterators using the cartesion product of the tables :)
    let table_iterator = tables
        .into_iter()
        .map(|(table, _)| table)
        .multi_cartesian_product();

    // We need to take all the columns
    if is_star_cols {
        // Add all columns to the selected schema
        for (_, col_type, output_col_name) in table_column_names {
            selected_schema.push((output_col_name, col_type));
        }

        // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
        for table_rows in table_iterator {
            // Accumulate all the cells across the vector of rows into a single vector
            let mut selected_cells: Row = Vec::new();
            table_rows
                .into_iter()
                .for_each(|x| selected_cells.extend(x.row));
            // Add the accumulated cells to the selected rows
            if resolve_predicate(&pred, &selected_cells)? {
                selected_rows.push(selected_cells);
            }
        }
    }
    // We need to take a subset of columns
    else {
        // Pass through columns with no aliases used to provide an alias if unambiguous
        let column_names = resolve_colnames(column_names, &table_column_names)?;

        // Get the indices of the columns we want to select
        let mut table_column_indices: Vec<usize> = Vec::new();
        for desired_column in column_names {
            let index = table_column_names
                .iter()
                .position(|(name, _, _)| name.eq(&desired_column));
            // Check that index is valid
            match index {
                Some(x) => {
                    table_column_indices.push(x);
                    let (_, col_type, output_col_name) = table_column_names.get(x).unwrap();
                    selected_schema.push((output_col_name.clone(), col_type.clone()));
                }
                None => {
                    return Err(format!(
                        "Column {} does not exist in any of the tables",
                        desired_column
                    ))
                }
            }
        }

        // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
        for table_rows in table_iterator {
            // Flatten the entire output row, but it includes all columns from all tables
            let mut output_row: Row = Vec::new();
            for row_info in table_rows {
                output_row.extend(row_info.row);
            }
            if resolve_predicate(&pred, &output_row)? {
                // Iterate through the output row and only select the columns we want
                let selected_cells: Row = output_row
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, row_cell)| {
                        if table_column_indices.contains(&i) {
                            Some(row_cell)
                        } else {
                            None
                        }
                    })
                    .collect();

                // Append the selected_cells row to our result
                selected_rows.push(selected_cells);
            }
        }
    }
    Ok((selected_schema, selected_rows))
}

/// This method implements the SQL update statement
/// 
/* 
pub fn update(
    values: Vec<Vec<String>>,
    table_name: String,
    database: &Database,
    user: &mut User,
) -> Result<(String, InsertDiff), String> {


    //user.append_diff(&Diff::Insert(diff.clone()));
    //Ok((format!("{} rows were successfully inserted.", len), diff))
}
*/

/// This method implements the SQL Insert statement. It takes in the table name and the values to be inserted
/// into the table. It returns a string containing the number of rows inserted.
/// If the table does not exist, it returns an error.
/// If the number of values to be inserted does not match the number of columns in the table, it returns an error.
/// If the values to be inserted are not of the correct type, it returns an error.
/// It appends the diff to the user passed in
pub fn insert(
    values: Vec<Vec<String>>,
    table_name: String,
    database: &Database,
    user: &mut User,
) -> Result<(String, InsertDiff), String> {
    database.get_table_path(&table_name, user)?;
    let table_dir: String = database.get_current_working_branch_path(user);
    let mut table = Table::new(&table_dir, &table_name, None)?;
    // Ensure that the number of values to be inserted matches the number of columns in the table
    let values = values
        .iter()
        .map(|x| {
            if x.len() != table.schema.len() {
                Err(format!(
                    "Number of values ({}) to be inserted does not match the number of columns in the table ({})"
                , x.len(), table.schema.len()))
            } else {
                Ok(x
                    .iter()
                    .zip(table.schema.iter())
                    .map(|(str, (_, col))| {
                        col.parse(str).map_err(|e| format!("Error parsing value: {}", e))
                    })
                    .collect::<Result<Row, String>>()?)
            }
        })
        .collect::<Result<Vec<Row>, _>>().map_err(|x| x.to_string())?;
    values.iter().try_for_each(|vec| {
        if vec.len() != table.schema.len() {
            return Err(format!(
                "Error: Values Inserted did not match Schema {}",
                table_name
            ));
        }
        vec.iter()
            .zip(table.schema.iter())
            .try_for_each(|(val, (_, col_type))| {
                if !col_type.match_value(&val) {
                    return Err(format!(
                        "Error: Value {} is not of type {}",
                        val.to_string(),
                        col_type.to_string()
                    ));
                }
                Ok(())
            })?;
        Ok(())
    })?;
    // Actually insert the values into the table
    let len: usize = values.len();
    let diff: InsertDiff = table.insert_rows(values)?;
    user.append_diff(&Diff::Insert(diff.clone()));
    Ok((format!("{} rows were successfully inserted.", len), diff))
}

// This method implements the SQL Where clause. It takes in an expression, and generates
// a function that takes in a row and returns a boolean. The function returns an error if
// the expression is invalid.
pub fn where_clause(
    pred: &Expr,
    table_names: Vec<(String, String)>,
    database: &Database,
    user: &User,
) -> Result<SolvePredicate, String> {
    let tables = load_aliased_tables(database, user, table_names)?;
    let col_names = gen_column_aliases(&tables);
    let index_refs = col_names
        .iter()
        .enumerate()
        .map(|(i, (name, _, _))| (name.clone(), i))
        .collect::<HashMap<String, usize>>();
    solve_predicate(pred, &col_names, &index_refs)
}

// Generating tables with aliases from a list of table names,
// and creating new aliases where necessary
fn load_aliased_tables(
    database: &Database,
    user: &User,
    table_names: Vec<(String, String)>,
) -> Result<Vec<(Table, String)>, String> {
    let table_dir: String = database.get_current_working_branch_path(user);
    let tables: Vec<(Table, String)> = table_names
        .iter()
        .map(|(table_name, alias)| {
            let table = Table::new(&table_dir, &table_name, None)?;
            if alias.is_empty() {
                // If no alias is provided, use the table name as the alias
                let alias = table_name.clone();
                Ok((table, alias))
            } else {
                Ok((table, alias.to_string()))
            }
        })
        .collect::<Result<Vec<(Table, String)>, String>>()?;
    Ok(tables)
}

// Get the names of all the columns in the tables along with their aliases in
// the format <alias>.<column_name> and store them in a vector of tuples
// alongside their column types and new column name when output.
// It will be a vector of tuples where each tuple is of the form:
// (<table_alias>.<column_name>, <column_type>, <output_column_name>)
fn gen_column_aliases(tables: &Vec<(Table, String)>) -> Vec<(String, Column, String)> {
    tables
        .iter()
        .map(|(table, alias): &(Table, String)| {
            table
                .schema
                .iter()
                .map(|(name, coltype)| {
                    (
                        format!("{}.{}", alias, name.clone()),
                        coltype.clone(),
                        name.clone(),
                    )
                })
                .collect::<Vec<(String, Column, String)>>()
        })
        .flatten()
        .collect::<Vec<(String, Column, String)>>()
}

// Unambiguates column names by adding the table alias if there is only one table with that column name
fn resolve_colnames(
    column_names: Vec<String>,
    table_column_names: &Vec<(String, Column, String)>,
) -> Result<Vec<String>, String> {
    let column_names = column_names
        .into_iter()
        .map(|x| resolve_reference(x, table_column_names))
        .collect::<Result<Vec<String>, String>>()?;
    Ok(column_names)
}

// Given a column name, it figures out which table it belongs to and returns the
// unambiguous column name
pub fn resolve_reference(
    column_name: String,
    table_column_names: &Vec<(String, Column, String)>,
) -> Result<String, String> {
    if column_name.contains(".") {
        // We know this works, as the parser does not allow for '.' in column names
        Ok(column_name)
    } else {
        let matches: Vec<&String> = table_column_names
            .iter()
            .filter_map(|(col_name, _, name)| {
                if name == &column_name {
                    Some(col_name)
                } else {
                    None
                }
            })
            .collect();
        if matches.len() == 1 {
            Ok(matches[0].clone())
        } else if matches.len() != 0 {
            Err(format!("Column name {} is ambiguous.", column_name))
        } else {
            Err(format!("Column name {} does not exist.", column_name))
        }
    }
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
        let columns = vec!["*".to_string()];
        let tables = vec![("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();

        insert(
            [
                vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
                vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            ]
            .to_vec(),
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

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
        let columns = vec!["*".to_string()];
        let tables = vec![
            ("test_table1".to_string(), "T1".to_string()),
            ("test_table2".to_string(), "T2".to_string()),
        ]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("country".to_string(), Column::String(50)),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema1, &new_db, &mut user).unwrap();
        create_table(&"test_table2".to_string(), &schema2, &new_db, &mut user).unwrap();

        // Write rows to first table
        insert(
            [
                // Rewritten with all as strings
                vec![
                    "1".to_string(),
                    "Robert Downey Jr.".to_string(),
                    "40".to_string(),
                ],
                vec!["2".to_string(), "Tom Holland".to_string(), "20".to_string()],
            ]
            .to_vec(),
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Write rows to second table
        insert(
            [
                vec!["1".to_string(), "United States".to_string()],
                vec!["2".to_string(), "Britain".to_string()],
            ]
            .to_vec(),
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

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
        assert_eq!(
            result.1[0][1],
            Value::String("Robert Downey Jr.".to_string())
        );
        assert_eq!(result.1[0][2], Value::I32(40));
        assert_eq!(result.1[0][3], Value::I32(1));
        assert_eq!(result.1[0][4], Value::String("United States".to_string()));

        // Check that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(1));
        assert_eq!(
            result.1[1][1],
            Value::String("Robert Downey Jr.".to_string())
        );
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
        let columns = vec!["T.id".to_string(), "name".to_string()];
        let tables = vec![("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        insert(
            vec![
                vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
                vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
                vec![
                    "3".to_string(),
                    "Doctor Strange".to_string(),
                    "35".to_string(),
                ],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

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
    fn test_unaliased_select() {
        // This tests
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = vec!["id".to_string(), "name".to_string()];
        let tables = vec![("test_table1".to_string(), "".to_string())]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        insert(
            vec![
                vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
                vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
                vec![
                    "3".to_string(),
                    "Doctor Strange".to_string(),
                    "35".to_string(),
                ],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

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
        // SELECT T1.id, country FROM select_test_db.test_table1 T1, select_test_db.test_table2;
        let columns = vec!["T1.id".to_string(), "country".to_string()];
        let tables = vec![
            ("test_table1".to_string(), "T1".to_string()),
            ("test_table2".to_string(), "".to_string()),
        ]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema1, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec![
                    "1".to_string(),
                    "Robert Downey Jr".to_string(),
                    "40".to_string(),
                ],
                vec!["2".to_string(), "Tom Holland".to_string(), "20".to_string()],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("country".to_string(), Column::String(50)),
        ];

        create_table(&"test_table2".to_string(), &schema2, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec!["5".to_string(), "United States".to_string()],
                vec!["6".to_string(), "Britain".to_string()],
            ],
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

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
    fn test_ambigous_select() {
        // This tests
        // SELECT id, country FROM select_test_db.test_table1, select_test_db.test_table2;
        let columns = vec!["id".to_string(), "country".to_string()];
        let tables = vec![
            ("test_table1".to_string(), "".to_string()),
            ("test_table2".to_string(), "".to_string()),
        ]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema1, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec![
                    "1".to_string(),
                    "Robert Downey Jr".to_string(),
                    "40".to_string(),
                ],
                vec!["2".to_string(), "Tom Holland".to_string(), "20".to_string()],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        let schema2: Schema = vec![
            ("id".to_string(), Column::I32),
            ("country".to_string(), Column::String(50)),
        ];

        create_table(&"test_table2".to_string(), &schema2, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec!["5".to_string(), "United States".to_string()],
                vec!["6".to_string(), "Britain".to_string()],
            ],
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let _ = select(columns.to_owned(), None, tables, &new_db, &user).unwrap_err();
        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_column_select() {
        // This tests
        // SELECT id, name, age, invalid_column FROM select_test_db.test_table1;

        let columns = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "invalid_column".to_string(),
        ];
        let tables = vec![("test_table1".to_string(), "".to_string())]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema1, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec![
                    "1".to_string(),
                    "Robert Downey Jr".to_string(),
                    "40".to_string(),
                ],
                vec!["2".to_string(), "Tom Holland".to_string(), "20".to_string()],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user);

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

        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let tables = vec![
            ("test_table1".to_string(), "".to_string()),
            ("test_table2".to_string(), "".to_string()),
        ]; // [(table_name, alias)]

        let new_db: Database = Database::new("select_test_db".to_string()).unwrap();

        let schema1: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema1, &new_db, &mut user).unwrap();

        insert(
            vec![
                vec![
                    "1".to_string(),
                    "Robert Downey Jr".to_string(),
                    "40".to_string(),
                ],
                vec!["2".to_string(), "Tom Holland".to_string(), "20".to_string()],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user);

        // Verify that SELECT failed
        assert!(result.is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_insert_columns() {
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = vec!["T.id".to_string(), "T.name".to_string()];
        let tables = vec![("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]

        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let rows = vec![
            vec![
                Value::I32(1),
                Value::String("Iron Man".to_string()),
                Value::I32(40),
            ],
            vec![
                Value::I32(2),
                Value::String("Spiderman".to_string()),
                Value::I32(20),
            ],
            vec![
                Value::I32(3),
                Value::String("Doctor Strange".to_string()),
                Value::I32(35),
            ],
            vec![
                Value::I32(4),
                Value::String("Captain America".to_string()),
                Value::I32(100),
            ],
            vec![
                Value::I32(5),
                Value::String("Thor".to_string()),
                Value::I32(1000),
            ],
        ];
        let newrows = vec![
            vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
            vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "35".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "100".to_string(),
            ],
            vec!["5".to_string(), "Thor".to_string(), "1000".to_string()],
        ];
        let (_, diff) = insert(newrows, "test_table1".to_string(), &new_db, &mut user).unwrap();

        // Verify that the insert was successful by looking at the diff first
        assert_eq!(diff.rows.len(), 5);
        assert_eq!(diff.schema, schema);
        assert_eq!(diff.table_name, "test_table1".to_string());
        assert_eq!(diff.rows[0].row, rows[0]);
        assert_eq!(diff.rows[1].row, rows[1]);
        assert_eq!(diff.rows[2].row, rows[2]);
        assert_eq!(diff.rows[3].row, rows[3]);
        assert_eq!(diff.rows[4].row, rows[4]);

        // Run the SELECT query and ensure that the result is correct
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));

        // Assert that 3 rows were returned
        assert_eq!(result.1.iter().len(), 5);

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

        // Assert that the fourth row is correct
        assert_eq!(result.1[3][0], Value::I32(4));
        assert_eq!(result.1[3][1], Value::String("Captain America".to_string()));

        // Assert that the fifth row is correct
        assert_eq!(result.1[4][0], Value::I32(5));
        assert_eq!(result.1[4][1], Value::String("Thor".to_string()));

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_insert() {
        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();

        let newrows = vec![
            vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
            vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            vec!["3".to_string(), "35".to_string()],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "100".to_string(),
            ],
        ];

        assert!(insert(newrows, "test_table1".to_string(), &new_db, &mut user).is_err());
        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    // Ensures that insert can cast values to the correct type if possible
    fn test_insert_casts() {
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = vec!["T.id".to_string(), "T.name".to_string()];
        let tables = vec![("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::Double),
        ];

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let rows = vec![
            vec![
                Value::I32(100), // Can only insert I32
                Value::String("Iron Man".to_string()),
                Value::Double(3.456),
            ],
            vec![
                Value::I32(2),
                Value::String("Spiderman".to_string()),
                Value::Double(3.43456),
            ],
            vec![
                Value::I32(3),
                Value::String("Doctor Strange".to_string()),
                Value::Double(322.456),
            ],
            vec![
                Value::I32(4),
                Value::String("Captain America".to_string()),
                Value::Double(12.456),
            ],
        ];
        let new_rows = vec![
            vec![
                "100".to_string(), // Can only insert I32
                "Iron Man".to_string(),
                "3.456".to_string(),
            ],
            vec![
                "2".to_string(),
                "Spiderman".to_string(),
                "3.43456".to_string(),
            ],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "322.456".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "12.456".to_string(),
            ],
        ];

        let (_, diff) = insert(new_rows, "test_table1".to_string(), &new_db, &mut user).unwrap();

        // Verify that the insert was successful by looking at the diff first
        assert_eq!(diff.rows.len(), 4);
        assert_eq!(diff.schema, schema);
        assert_eq!(diff.table_name, "test_table1".to_string());
        assert_eq!(diff.rows[0].row, rows[0]);
        assert_eq!(diff.rows[1].row, rows[1]);
        assert_eq!(diff.rows[2].row, rows[2]);
        assert_eq!(diff.rows[3].row, rows[3]);

        // Run the SELECT query and ensure that the result is correct
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

        assert_eq!(result.0[0], ("id".to_string(), Column::I32));
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));

        // Assert that 3 rows were returned
        assert_eq!(result.1.iter().len(), 4);

        // Assert that each row only has 2 columns
        for row in result.1.clone() {
            assert_eq!(row.len(), 2);
        }

        // Assert that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(100)); // Casted from I64
        assert_eq!(result.1[0][1], Value::String("Iron Man".to_string()));

        // Assert that the second row is correct
        assert_eq!(result.1[1][0], Value::I32(2));
        assert_eq!(result.1[1][1], Value::String("Spiderman".to_string()));

        // Assert that the third row is correct
        assert_eq!(result.1[2][0], Value::I32(3));
        assert_eq!(result.1[2][1], Value::String("Doctor Strange".to_string()));

        // Assert that the fourth row is correct
        assert_eq!(result.1[3][0], Value::I32(4));
        assert_eq!(result.1[3][1], Value::String("Captain America".to_string()));
        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    // Ensures that insert exits if a value cannot be casted
    fn test_insert_invalid_casts() {
        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::Double),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let rows = vec![
            vec![
                "100".to_string(), // Can only insert I32
                "Iron Man".to_string(),
                "Robert Downey".to_string(),
            ],
            vec![
                "2".to_string(),
                "Spiderman".to_string(),
                "3.43456".to_string(),
            ],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "322.456".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "12.456".to_string(),
            ],
        ];

        assert!(insert(rows, "test_table1".to_string(), &new_db, &mut user).is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    // Ensures that insert can cast values to the correct type if possible
    fn test_insert_nulls() {
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = vec!["T.id".to_string(), "T.name".to_string()];
        let tables = vec![("test_table1".to_string(), "T".to_string())]; // [(table_name, alias)]

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::Nullable(Box::new(Column::I32))),
            ("name".to_string(), Column::String(50)),
            (
                "age".to_string(),
                Column::Nullable(Box::new(Column::Double)),
            ),
        ];

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let rows = vec![
            vec![
                Value::I32(100), // Can only insert I32
                Value::String("Iron Man".to_string()),
                Value::Double(3.456),
            ],
            vec![
                Value::Null,
                Value::String("Spiderman".to_string()),
                Value::Double(3.43456),
            ],
            vec![
                Value::I32(3),
                Value::String("Doctor Strange".to_string()),
                Value::Null,
            ],
            vec![
                Value::Null,
                Value::String("Captain America".to_string()),
                Value::Null,
            ],
        ];
        let new_rows = vec![
            vec![
                "100".to_string(), // Can only insert I32
                "Iron Man".to_string(),
                "3.456".to_string(),
            ],
            vec![
                "".to_string(),
                "Spiderman".to_string(),
                "3.43456".to_string(),
            ],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "".to_string(),
            ],
            vec![
                "".to_string(),
                "Captain America".to_string(),
                "".to_string(),
            ],
        ];

        let (_, diff) = insert(new_rows, "test_table1".to_string(), &new_db, &mut user).unwrap();

        // Verify that the insert was successful by looking at the diff first
        assert_eq!(diff.rows.len(), 4);
        assert_eq!(diff.schema, schema);
        assert_eq!(diff.table_name, "test_table1".to_string());
        assert_eq!(diff.rows[0].row, rows[0]);
        assert_eq!(diff.rows[1].row, rows[1]);
        assert_eq!(diff.rows[2].row, rows[2]);
        assert_eq!(diff.rows[3].row, rows[3]);

        // Run the SELECT query and ensure that the result is correct
        let user: User = User::new("test_user".to_string());
        let result = select(columns.to_owned(), None, tables, &new_db, &user).unwrap();

        assert_eq!(
            result.0[0],
            ("id".to_string(), Column::Nullable(Box::new(Column::I32)))
        );
        assert_eq!(result.0[1], ("name".to_string(), Column::String(50)));

        // Assert that 3 rows were returned
        assert_eq!(result.1.iter().len(), 4);

        // Assert that each row only has 2 columns
        for row in result.1.clone() {
            assert_eq!(row.len(), 2);
        }

        // Assert that the first row is correct
        assert_eq!(result.1[0][0], Value::I32(100)); // Casted from I64
        assert_eq!(result.1[0][1], Value::String("Iron Man".to_string()));

        // Assert that the second row is correct
        assert_eq!(result.1[1][0], Value::Null);
        assert_eq!(result.1[1][1], Value::String("Spiderman".to_string()));

        // Assert that the third row is correct
        assert_eq!(result.1[2][0], Value::I32(3));
        assert_eq!(result.1[2][1], Value::String("Doctor Strange".to_string()));

        // Assert that the fourth row is correct
        assert_eq!(result.1[3][0], Value::Null);
        assert_eq!(result.1[3][1], Value::String("Captain America".to_string()));
        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    // Ensures that insert exits if a value is not nullable and is inserted as null
    fn test_insert_invalid_nulls() {
        let new_db: Database = Database::new("insert_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            (
                "age".to_string(),
                Column::Nullable(Box::new(Column::Double)),
            ),
        ];

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let rows = vec![
            vec![
                "".to_string(), // Nulled
                "Iron Man".to_string(),
                "Robert Downey".to_string(),
            ],
            vec!["2".to_string(), "Spiderman".to_string(), "".to_string()],
            vec!["3".to_string(), "".to_string(), "322.456".to_string()],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "".to_string(),
            ],
        ];

        assert!(insert(rows, "test_table1".to_string(), &new_db, &mut user).is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }
}
