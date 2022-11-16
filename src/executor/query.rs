use std::collections::HashMap;

use super::aggregate::resolve_aggregates;
use super::predicate::{
    resolve_comparison, resolve_predicate, resolve_pure_value, resolve_reference, resolve_value,
    solve_comparison, solve_predicate, solve_value, ComparisonSolver, PredicateSolver, ValueSolver,
};
use crate::user::userdata::*;
use crate::util::dbtype::Column;
use crate::util::row::{Row, RowInfo};
use crate::version_control::diff::*;
use crate::{
    btree::{btree::*, indexes::*},
    fileio::{
        databaseio::*,
        header::*,
        tableio::{self, *},
    },
    util::row::RowLocation,
};

use crate::util::dbtype::Value;
use itertools::Itertools;
use sqlparser::ast::{
    Expr, Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator, Statement, AlterTableOperation,
};

pub type Tables = Vec<(Table, String)>;
pub type ColumnAliases = Vec<(String, Column, String)>;
pub type IndexRefs = HashMap<String, usize>;

/// A parse function, that starts with a string and returns either a table for query commands
/// or a string for
pub fn execute_query(
    ast: &Vec<Statement>,
    user: &mut User,
    _command: &String,
) -> Result<(Vec<String>, Vec<Row>), String> {
    if ast.len() == 0 {
        return Err("Empty AST".to_string());
    }
    for a in ast.iter() {
        match a {
            Statement::Query(q) => {
                return func(&q.body, user, q);
            }
            _ => print!("Not a query\n"),
        };
    }
    Err("No query found".to_string())
}

fn func(
    setExpr: &SetExpr,
    user: &mut User,
    query: &Query,
) -> Result<(Vec<String>, Vec<Row>), String> {
    match &setExpr {
        SetExpr::Select(s) => parse_select(&s, user, Some(query)),
        SetExpr::SetOperation {
            op,
            all: _,
            left,
            right,
        } => {
            let (left_cols, left_rows) = func(&left, user, query)?;
            let (right_cols, right_rows) = func(&right, user, query)?;
            if left_cols.len() != right_cols.len() {
                return Err("Incompatible types in set operation".to_string());
            }

            let row = set_operations(op, left_rows.clone(), right_rows.clone())?;
            if left_rows.is_empty() && !right_rows.is_empty() {
                return Ok((right_cols, row));
            }
            Ok((left_cols, row))
        }
        _ => Err("Not a select\n".to_string()),
    }
}

fn parse_select(
    s: &Select,
    user: &mut User,
    query: Option<&Query>,
) -> Result<(Vec<String>, Vec<Row>), String> {
    let mut columns = Vec::new();
    for c in s.projection.iter() {
        columns.push(c.clone());
    }
    let mut table_names = Vec::new();
    for t in s.from.iter() {
        let table_name = t.to_string();
        let table_name: Vec<&str> = table_name.split(" ").collect();
        if table_name.len() == 3 {
            table_names.push((table_name[0].to_string(), table_name[2].to_string()));
        } else {
            table_names.push((table_name[0].to_string(), "".to_string()));
        }
    }
    let mut index_id: Option<IndexID> = None;
    let mut expr: Option<Expr> = None;
    let pred: Option<PredicateSolver> = match &s.selection {
        Some(pred) => {
            let tables: Tables = load_aliased_tables(get_db_instance()?, user, &table_names)?;
            let column_aliases: ColumnAliases = gen_column_aliases(&tables);
            let index_refs: IndexRefs = get_index_refs(&column_aliases);
            index_id = Some(get_index_id_from_expr(pred, &column_aliases, &index_refs)?);
            expr = Some(pred.clone());
            Some(where_clause(pred, &table_names, get_db_instance()?, user)?)
        }
        None => None,
    };

    // Results
    let mut res_columns: Vec<String> = Vec::new();
    let mut res_rows: Vec<Row> = Vec::new();

    // Check if we are using an index
    let mut using_index: bool = false;
    if index_id.is_some() && expr.is_some() {
        let index_id: IndexID = index_id.unwrap();
        let tables: Tables = load_aliased_tables(get_db_instance()?, user, &table_names)?;
        // For now, lets only support using indexing on a single table.
        if tables.len() == 1 {
            if let Some(idx_val) = tables[0].0.indexes.get(&index_id) {
                let btree_pagenum: u32 = idx_val.0;
                let index_name: String = idx_val.1.clone();
                let table: Table = tables[0].0.clone();
                let index_key_type: IndexKeyType = index_id
                    .iter()
                    .map(|x| table.schema[*x as usize].1.clone())
                    .collect();

                let btree: BTree = BTree::load_btree_from_root_page(
                    &tables[0].0,
                    btree_pagenum,
                    index_id,
                    index_key_type,
                    index_name,
                )?;

                res_rows = btree
                    .get_rows_matching_expr(&expr.unwrap())?
                    .iter()
                    .map(|x| x.row.clone())
                    .collect();

                // Get the column names
                let table_aliases = gen_column_aliases(&tables);
                resolve_columns(columns.clone(), &mut res_columns, &tables, &table_aliases)?;
                using_index = true;
            }
        }
    }

    if !using_index {
        (res_columns, res_rows) = select(
            columns.clone(),
            pred,
            s.group_by.clone(),
            query.map_or(vec![], |q| q.order_by.clone()),
            &table_names,
            get_db_instance()?,
            user,
        )?;
    }

    // Limit and Offset
    if let Some(query) = query {
        let limit: Option<usize> = match &query.limit {
            Some(l) => match resolve_pure_value(l)? {
                Value::I32(i) => Some(i as usize),
                Value::I64(i) => Some(i as usize),
                _ => None,
            },
            None => None,
        };
        let offset: usize = match &query.offset {
            Some(l) => match resolve_pure_value(&l.value)? {
                Value::I32(i) => i as usize,
                Value::I64(i) => i as usize,
                _ => 0,
            },
            None => 0,
        };
        if offset >= res_rows.len() {
            return Ok((res_columns, Vec::new()));
        }
        if let Some(l) = limit {
            res_rows = res_rows[offset..(offset + l).min(res_rows.len())].to_vec();
        }
    }
    Ok((res_columns, res_rows))
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
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique: _,
                if_not_exists: _,
            } => {
                let table_dir: String = get_db_instance()?.get_current_working_branch_path(user);
                let table_name: String = table_name.to_string();
                let index_name: String = name.0[0].value.clone();

                let column_names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();

                println!(
                    "Creating index {} on table {} with columns {:?}",
                    index_name, table_name, column_names
                );

                let (_, idx_new_diff): (_, IndexCreateDiff) = BTree::create_btree_index(
                    &table_dir,
                    &table_name,
                    None,
                    column_names,
                    index_name,
                )?;

                user.append_diff(&Diff::IndexCreate(idx_new_diff));
                results.push("Successfully created index".to_string());
            }
            Statement::Update {
                table,
                assignments,
                from: _,
                selection,
            } => {
                let final_table; // What is the best way to do this?
                let mut all_data: Vec<(String, Expr)> = Vec::new();
                let final_alias;

                match table.relation.clone() {
                    sqlparser::ast::TableFactor::Table {
                        name: table_name,
                        alias,
                        args: _,
                        with_hints: _,
                    } => {
                        // Now you have the table
                        final_alias = match alias {
                            Some(x) => x.to_string(),
                            None => "".to_string(),
                        };
                        final_table = table_name.to_string();
                    }
                    _ => {
                        // Not a table inside the TableFactor enum
                        return Err("Error parsing".to_string());
                    }
                }

                let table_names = vec![(final_table.clone(), final_alias.clone())];

                // Iterate through and build vector of assignments to pass to update
                for assignment in assignments {
                    let column_name;
                    let insert_value = assignment.value.clone();
                    column_name = assignment.id[0].value.clone();

                    all_data.push((column_name, insert_value));
                }

                // Now we have the table name and the assignments
                let pred: Option<PredicateSolver> = match selection {
                    Some(pred) => Some(where_clause(pred, &table_names, get_db_instance()?, user)?),
                    None => None,
                };

                results.push(
                    update(
                        all_data,
                        final_table,
                        final_alias,
                        pred,
                        get_db_instance()?,
                        user,
                    )?
                    .0,
                );
            }
            Statement::Delete {
                table_name,
                using: _,
                selection,
            } => {
                let final_table; // What is the best way to do this?
                let final_alias;
                match table_name.clone() {
                    sqlparser::ast::TableFactor::Table {
                        name: table_name,
                        alias,
                        args: _,
                        with_hints: _,
                    } => {
                        // Now you have the table
                        final_alias = match alias {
                            Some(x) => x.to_string(),
                            None => "".to_string(),
                        };
                        final_table = table_name.to_string();
                    }
                    _ => {
                        // Not a table inside the TableFactor enum
                        return Err("Error parsing".to_string());
                    }
                }

                let table_names = vec![(final_table.clone(), final_alias)];

                let pred: Option<PredicateSolver> = match selection {
                    Some(pred) => Some(where_clause(pred, &table_names, get_db_instance()?, user)?),
                    None => None,
                };
                results.push(delete(final_table, pred, get_db_instance()?, user)?.0);
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade: _,
                purge: _,
            } => {
                match object_type.clone() {
                    sqlparser::ast::ObjectType::Table => {
                        if names.len() != 1 {
                            return Err("Can only drop one table at a time".to_string());
                        }

                        let table_name: String = names[0].to_string();

                        // If the table doesn't exist on this branch, return an error
                        if (!if_exists)
                            && (!get_db_instance()?.get_tables(user)?.contains(&table_name))
                        {
                            return Err(format!("Table {} does not exist", table_name));
                        }

                        let result: TableRemoveDiff =
                            drop_table(&table_name, get_db_instance()?, user)?;
                        results.push(format!("Table dropped: {}", result.table_name));
                    }
                    sqlparser::ast::ObjectType::Index => {
                        if names.len() != 1 {
                            return Err("Can only drop one index at a time".to_string());
                        }

                        let idents: Vec<Ident> = names[0].0.clone();
                        if idents.len() != 2 {
                            return Err("Must specify one index and table to drop {table_name}.{index_name}".to_string());
                        }

                        let table_name: &String = &idents[0].value;
                        let index_name: &String = &idents[1].value;

                        let table_dir: String =
                            get_db_instance()?.get_current_working_branch_path(user);

                        let idx_rem_diff: IndexRemoveDiff =
                            BTree::drop_btree_index(&table_dir, table_name, None, index_name)?;

                        user.append_diff(&Diff::IndexRemove(idx_rem_diff));
                        results.push(format!("Index dropped: {}", index_name));
                    }
                    _ => {
                        return Err("Can only drop tables and indexes".to_string());
                    }
                }
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
                // Keeping all_data as a vector of rows allows us to also easily integrate select later on
                let mut all_data: Vec<Row> = Vec::new();
                match *source.body.clone() {
                    SetExpr::Values(values) => {
                        let values_list = values.0;
                        for row in values_list {
                            let mut data = Vec::new();
                            for k in row {
                                data.push(
                                    // We don't need any additional information to solve this, hence the empty vectors and maps
                                    // Here, we effectively convert the Expr's into our Value types
                                    resolve_pure_value(&k)?,
                                );
                            }
                            all_data.push(data);
                        }
                    }
                    SetExpr::Select(v) => {
                        let (_, rows) = parse_select(&v, user, None)?;
                        all_data = rows;
                    }
                    _ => {
                        return Err("Expected a Values statement".to_string());
                    }
                }
                results.push(insert(all_data, table_name, get_db_instance()?, user)?.0);
            }
            Statement::AlterTable {
                name,
                operation
            } => {
                let table_name = name.0[0].value.to_string();
                println!("Alter table name: {:?}", table_name);
                // println!("Operation: {:?}", operation);
                match operation {
                    AlterTableOperation::AddColumn { column_def } => {
                        let column_name = column_def.name.value.to_string();
                        let column = Column::from_col_def(&column_def)?;
                        println!("Column name: {:?}", column_name);
                        println!("Column: {:?}", column);
                        // let result = alter_table_add_column(&table_name, &column_name, &column, get_db_instance()?, user)?;
                        // results.push(format!("Column added: {}", result.column_name));
                        results.push(format!("Schema added {}({:?}) to Table {}", column_name, column, table_name));
                    }
                    AlterTableOperation::DropColumn { column_name, if_exists, cascade } => {
                        let column_name = column_name.to_string();
                        // let result = alter_table_drop_column(&table_name, &column_name, get_db_instance()?, user)?;
                        // results.push(format!("Column dropped: {}", result.column_name));
                        results.push(format!("Schema {} deleted in Table {}", column_name, table_name));
                    }
                    _ => {
                        return Err("Can only add or drop columns".to_string());
                    }
                }
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
    columns: Vec<SelectItem>,
    where_pred: Option<PredicateSolver>,
    group_by: Vec<Expr>,        // Empty if no group by
    order_by: Vec<OrderByExpr>, // Empty if no order by
    table_names: &Vec<(String, String)>,
    database: &Database,
    user: &User, // If a user is present, query that user's branch. Otherwise, query main branch
) -> Result<(Vec<String>, Vec<Row>), String> {
    if table_names.len() == 0 || columns.len() == 0 {
        return Err("Malformed SELECT Command".to_string());
    }

    // The schema that would be returned from the select statement
    let mut column_names: Vec<String> = Vec::new();

    let tables: Tables = load_aliased_tables(database, user, &table_names)?;

    // This is where the fun begins... ;)
    let table_aliases = gen_column_aliases(&tables);

    // Create an iterator of table iterators using the cartesion product of the tables :)
    let table_iterator = tables
        .iter()
        .map(|(table, _)| table)
        .cloned()
        .multi_cartesian_product();

    let index_refs = get_index_refs(&table_aliases);

    // Pass through columns with no aliases used to provide an alias if unambiguous
    let column_exprs = resolve_columns(columns, &mut column_names, &tables, &table_aliases)?;

    // Instead of directly adding rows to a Vector, we add them to a HashMap from the group_by columns to the rows in that group
    let mut grouped_rows: HashMap<Row, Vec<(Row, Row)>> = HashMap::new();

    let column_solver = solve_row(&column_exprs, &table_aliases, &index_refs)?;
    let group_solver = solve_row(&group_by, &table_aliases, &index_refs)?;
    let order_solver: ComparisonSolver = solve_comparison(&order_by, &table_aliases, &index_refs)?;

    // The table_iterator returns a vector of rows where each row is a vector of cells on each iteration
    for table_rows in table_iterator {
        // Flatten the entire output row, but it includes all columns from all tables
        let mut output_row: Row = Vec::new();
        for row_info in table_rows {
            output_row.extend(row_info.row);
        }
        if resolve_predicate(&where_pred, &output_row)? {
            // Iterate through the output row and apply the column functions to each row
            let selected_cells = resolve_row(&column_solver, &output_row)?;
            let group_row = resolve_row(&group_solver, &output_row)?;
            // Append the selected_cells row to our result
            grouped_rows
                .entry(group_row)
                .or_insert_with(Vec::new)
                .push((selected_cells, output_row));
        }
    }

    // Also, individually sort the rows in each group by the order_by columns
    // Solve aggregate functions and create the selected rows that are now ready to be returned
    let selected_rows: Vec<Row> = grouped_rows
        .into_values()
        .map(|mut rows| {
            rows.sort_unstable_by(|(_, row1), (_, row2)| {
                resolve_comparison(&order_solver, row1, row2)
            });
            resolve_aggregates(rows, &column_exprs, &table_aliases, &index_refs)
        })
        .flatten_ok()
        .collect::<Result<Vec<Row>, String>>()?;

    Ok((column_names, selected_rows))
}

fn solve_row(
    group_by: &Vec<Expr>,
    table_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<Vec<ValueSolver>, String> {
    let group_solver = group_by
        .iter()
        .map(|item| solve_value(item, &table_aliases, &index_refs))
        .collect::<Result<Vec<ValueSolver>, String>>()?;
    Ok(group_solver)
}

fn resolve_row(column_funcs: &Vec<ValueSolver>, output_row: &Row) -> Result<Row, String> {
    let selected_cells: Row = column_funcs
        .iter()
        .map(|f| resolve_value(f, &output_row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(selected_cells)
}

/// This method implements the SQL update statement
pub fn update(
    values: Vec<(String, Expr)>,
    table_name: String,
    alias: String,
    selection: Option<PredicateSolver>,
    database: &Database,
    user: &mut User,
) -> Result<(String, UpdateDiff), String> {
    database.get_table_path(&table_name, user)?;
    let table = Table::from_user(user, database, &table_name, None)?;
    let mut selected_rows: Vec<RowInfo> = Vec::new();
    let tables = load_aliased_tables(database, user, &vec![(table_name.clone(), alias)])?;
    let column_aliases = gen_column_aliases(&tables);
    let index_refs = get_index_refs(&column_aliases);

    let values = values
        .into_iter()
        .map(|(name, expr)| Ok((name, solve_value(&expr, &column_aliases, &index_refs)?)))
        .collect::<Result<Vec<(String, ValueSolver)>, String>>()?;

    for row_info in table.clone() {
        if resolve_predicate(&selection, &row_info.row)? {
            // Append the selected_cells row to our result
            let mut row_info = row_info.clone();
            for (name, value) in values.iter() {
                let value = resolve_value(&value, &row_info.row)?;
                let column_name = resolve_reference(name.clone(), &column_aliases)?;
                let index = *index_refs.get(&column_name).ok_or(format!(
                    "Column name {} not found in table {}",
                    column_name, &table_name
                ))?;
                row_info.row[index] = value;
            }
            selected_rows.push(row_info);
        }
    }

    let len: usize = selected_rows.len();
    let diff: UpdateDiff = table.rewrite_rows(selected_rows)?;
    user.append_diff(&Diff::Update(diff.clone()));
    Ok((format!("{} rows were successfully updated.", len), diff))
}

pub fn delete(
    table_name: String,
    selection: Option<PredicateSolver>,
    database: &Database,
    user: &mut User,
) -> Result<(String, RemoveDiff), String> {
    let table = Table::from_user(user, database, &table_name, None)?;
    let mut selected_rows: Vec<RowLocation> = Vec::new();

    for row_info in table.clone() {
        if resolve_predicate(&selection, &row_info.row)? {
            // Append the selected_cells row to our result
            selected_rows.push(row_info.get_row_location());
        }
    }

    let len: usize = selected_rows.len();
    let diff: RemoveDiff = table.remove_rows(selected_rows)?;
    user.append_diff(&Diff::Remove(diff.clone()));

    Ok((format!("{} rows were deleted.", len), diff))
}

/// This method implements the SQL Insert statement. It takes in the table name and the values to be inserted
/// into the table. It returns a string containing the number of rows inserted.
/// If the table does not exist, it returns an error.
/// If the number of values to be inserted does not match the number of columns in the table, it returns an error.
/// If the values to be inserted are not of the correct type, it returns an error.
/// It appends the diff to the user passed in
pub fn insert(
    values: Vec<Row>,
    table_name: String,
    database: &Database,
    user: &mut User,
) -> Result<(String, InsertDiff), String> {
    database.get_table_path(&table_name, user)?;
    let mut table = Table::from_user(user, database, &table_name, None)?;
    // Ensure that the number of values to be inserted matches the number of columns in the table
    let values = values
        .into_iter()
        .map(|x| {
            if x.len() != table.schema.len() {
                Err(format!(
                    "Number of values ({}) to be inserted does not match the number of columns in the table ({})"
                , x.len(), table.schema.len()))
            } else {
                Ok(x
                    .into_iter()
                    .zip(table.schema.iter())
                    .map(|(val, (_, col))| {
                        col.coerce_type(val).map_err(|e| format!("Error parsing value: {}", e))
                    })
                    .collect::<Result<Row, String>>()?)
            }
        })
        .collect::<Result<Vec<Row>, _>>().map_err(|x| x.to_string())?;
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
    table_names: &Vec<(String, String)>,
    database: &Database,
    user: &User,
) -> Result<PredicateSolver, String> {
    let tables = load_aliased_tables(database, user, &table_names)?;
    let column_aliases = gen_column_aliases(&tables);
    let index_refs = get_index_refs(&column_aliases);
    solve_predicate(pred, &column_aliases, &index_refs)
}

// Generating tables with aliases from a list of table names,
// and creating new aliases where necessary
fn load_aliased_tables(
    database: &Database,
    user: &User,
    table_names: &Vec<(String, String)>,
) -> Result<Tables, String> {
    let tables: Vec<(Table, String)> = table_names
        .iter()
        .map(|(table_name, alias)| {
            let table = Table::from_user(user, database, table_name, None)?;
            if alias.is_empty() {
                // If no alias is provided, use the table name as the alias
                let alias = table_name.clone();
                Ok((table, alias))
            } else {
                Ok((table, alias.to_string()))
            }
        })
        .collect::<Result<Tables, String>>()?;
    Ok(tables)
}

// Get the names of all the columns in the tables along with their aliases in
// the format <alias>.<column_name> and store them in a vector of tuples
// alongside their column types and new column name when output.
// It will be a vector of tuples where each tuple is of the form:
// (<table_alias>.<column_name>, <column_type>, <output_column_name>)
pub fn gen_column_aliases(tables: &Tables) -> ColumnAliases {
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
                .collect::<ColumnAliases>()
        })
        .flatten()
        .collect::<ColumnAliases>()
}

// Get the names of all the columns in the tables along with their aliases in
// the format <alias>.<column_name> and store them in a vector of tuples
// alongside their column types and new column name when output.
// It will be a vector of tuples where each tuple is of the form:
// (<table_alias>.<column_name>, <column_type>, <output_column_name>)
pub fn gen_column_aliases_from_schema(tables: &Vec<(Schema, String)>) -> ColumnAliases {
    tables
        .iter()
        .map(|(schema, alias): &(Schema, String)| {
            schema
                .iter()
                .map(|(name, coltype)| {
                    (
                        format!("{}.{}", alias, name.clone()),
                        coltype.clone(),
                        name.clone(),
                    )
                })
                .collect::<ColumnAliases>()
        })
        .flatten()
        .collect::<ColumnAliases>()
}

/// Hashmap from column names to index in the row
pub fn get_index_refs(column_aliases: &ColumnAliases) -> IndexRefs {
    column_aliases
        .iter()
        .enumerate()
        .map(|(i, (name, _, _))| (name.clone(), i))
        .collect::<IndexRefs>()
}

/// Given a set of Columns, this creates a vector to reference these columns and apply relevant operations
fn resolve_columns(
    columns: Vec<SelectItem>,
    column_names: &mut Vec<String>,
    tables: &Tables,
    column_aliases: &ColumnAliases,
) -> Result<Vec<Expr>, String> {
    columns
        .into_iter()
        .map(|item| resolve_selects(item, column_names, &tables, column_aliases))
        .flatten_ok()
        .collect::<Result<Vec<Expr>, String>>()
}

/// Given a specific SelectItem, this will resolve the column name and create a function to resolve the value
fn resolve_selects(
    item: SelectItem,
    column_names: &mut Vec<String>,
    tables: &Tables,
    column_aliases: &ColumnAliases,
) -> Result<Vec<Expr>, String> {
    Ok(match item {
        SelectItem::ExprWithAlias { expr, alias: _ } => {
            column_names.push(expr.to_string());
            vec![expr]
        }
        SelectItem::UnnamedExpr(expr) => {
            column_names.push(expr.to_string());
            vec![expr]
        }
        // Pick out all the columns
        SelectItem::Wildcard => {
            let names: Vec<Expr> = column_aliases
                .iter()
                .map(|(x, _, _)| to_ident(x.clone()))
                .collect();
            column_names.append(
                column_aliases
                    .iter()
                    .map(|(_, _, z)| z.clone())
                    .collect::<Vec<String>>()
                    .as_mut(),
            );
            names
        }
        // Pick out all the columns from tha table, aliased
        SelectItem::QualifiedWildcard(idents) => {
            let name = idents
                .0
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(".");
            let index = tables
                .iter()
                .position(|(t, alias)| t.name == name || alias == &name);
            if let Some(index) = index {
                let (table, alias) = tables.get(index).unwrap();
                table
                    .schema
                    .iter()
                    .map(|(colname, _)| {
                        column_names.push(colname.clone());
                        to_ident(format!("{}.{}", alias, colname))
                    })
                    .collect()
            } else {
                return Err(format!("Table {} not found.", name));
            }
        }
    })
}

pub fn to_ident(s: String) -> Expr {
    Expr::Identifier(Ident {
        value: s.to_string(),
        quote_style: None,
    })
}

pub fn set_operations(
    op: &SetOperator,
    left_rows: Vec<Vec<Value>>,
    right_rows: Vec<Vec<Value>>,
) -> Result<Vec<Row>, String> {
    // checking if the columns match
    if left_rows.is_empty() || right_rows.is_empty() {
        match op {
            SetOperator::Union => {
                if left_rows.is_empty() {
                    return Ok(right_rows);
                }
                return Ok(left_rows);
            }
            SetOperator::Except => {
                return Ok(left_rows);
            }
            SetOperator::Intersect => {
                return Ok(vec![]);
            }
        }
    }

    for left in left_rows[0].iter() {
        // checking if the columns match
        // I32 and I64 counts as same type
        if !right_rows[0].iter().any(|x| {
            x.get_coltype() == left.get_coltype()
                || (x.get_coltype().to_string() == "I32" && left.get_coltype().to_string() == "I64")
                || (x.get_coltype().to_string() == "I64" && left.get_coltype().to_string() == "I32")
        }) {
            return Err("Incompatible types in set operation".to_string());
        }
    }

    // convert all int to i64
    let mut new_right: Vec<Vec<Value>> = Vec::new();
    let mut new_left: Vec<Vec<Value>> = Vec::new();

    for row in right_rows.clone() {
        let temp_right = row
            .into_iter()
            .map(|val| Column::I64.coerce_type_numbers_only(val.clone()))
            .collect::<Result<Row, String>>()?;
        new_right.push(temp_right);
    }

    for row in left_rows.clone() {
        let temp_left = row
            .into_iter()
            .map(|val| Column::I64.coerce_type_numbers_only(val.clone()))
            .collect::<Result<Row, String>>()?;
        new_left.push(temp_left);
    }

    match op {
        SetOperator::Union => {
            for row in new_right {
                if !new_left.contains(&row) {
                    new_left.push(row);
                }
            }
            Ok(new_left)
        }
        SetOperator::Except => {
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for row in new_left {
                if !new_right.contains(&row) {
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        SetOperator::Intersect => {
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for row in new_right {
                if new_left.contains(&row) {
                    rows.push(row);
                }
            }
            Ok(rows)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        parser::parser::parse,
        util::{
            bench::create_demo_db,
            dbtype::{Column, Value},
        },
    };
    use serial_test::serial;

    pub fn to_selectitems(names: Vec<String>) -> Vec<SelectItem> {
        names
            .into_iter()
            .map(|name| SelectItem::UnnamedExpr(to_ident(name)))
            .collect()
    }

    #[test]
    #[serial]
    fn test_select_single_table_star() {
        // This tests
        // SELECT * FROM select_test_db.test_table1
        let columns = vec![SelectItem::Wildcard];
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
            [
                vec![
                    Value::I64(1),
                    Value::String("Iron Man".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Spiderman".to_string()),
                    Value::I64(20),
                ],
            ]
            .to_vec(),
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "id".to_string());
        assert_eq!(result.0[1], "name".to_string());
        assert_eq!(result.0[2], "age".to_string());

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
        let columns = vec![SelectItem::Wildcard];
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
                vec![
                    Value::I64(1),
                    Value::String("Robert Downey Jr.".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Tom Holland".to_string()),
                    Value::I64(20),
                ],
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
                vec![Value::I64(1), Value::String("United States".to_string())],
                vec![Value::I64(2), Value::String("Britain".to_string())],
            ]
            .to_vec(),
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        // Check that the schema is correct
        assert_eq!(result.0[0], "id".to_string());
        assert_eq!(result.0[1], "name".to_string());
        assert_eq!(result.0[2], "age".to_string());
        assert_eq!(result.0[3], "id".to_string());
        assert_eq!(result.0[4], "country".to_string());

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
        let columns = to_selectitems(vec!["T.id".to_string(), "name".to_string()]);
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
                vec![
                    Value::I64(1),
                    Value::String("Iron Man".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Spiderman".to_string()),
                    Value::I64(20),
                ],
                vec![
                    Value::I64(3),
                    Value::String("Doctor Strange".to_string()),
                    Value::I64(35),
                ],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "T.id".to_string());
        assert_eq!(result.0[1], "name".to_string());

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
        let columns = to_selectitems(vec!["id".to_string(), "name".to_string()]);
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
                vec![
                    Value::I64(1),
                    Value::String("Iron Man".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Spiderman".to_string()),
                    Value::I64(20),
                ],
                vec![
                    Value::I64(3),
                    Value::String("Doctor Strange".to_string()),
                    Value::I64(35),
                ],
            ],
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "id".to_string());
        assert_eq!(result.0[1], "name".to_string());

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
        let columns = to_selectitems(vec!["T1.id".to_string(), "country".to_string()]);
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
            [
                vec![
                    Value::I64(1),
                    Value::String("Robert Downey Jr".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Tom Holland".to_string()),
                    Value::I64(20),
                ],
            ]
            .to_vec(),
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
            [
                vec![Value::I64(1), Value::String("United States".to_string())],
                vec![Value::I64(2), Value::String("Britain".to_string())],
            ]
            .to_vec(),
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "T1.id".to_string());
        assert_eq!(result.0[1], "country".to_string());

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
        let columns = to_selectitems(vec!["id".to_string(), "country".to_string()]);
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
            [
                vec![
                    Value::I64(1),
                    Value::String("Robert Downey Jr.".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Tom Holland".to_string()),
                    Value::I64(20),
                ],
            ]
            .to_vec(),
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
            [
                vec![Value::I64(5), Value::String("United States".to_string())],
                vec![Value::I64(6), Value::String("Britain".to_string())],
            ]
            .to_vec(),
            "test_table2".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let _ = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap_err();
        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_column_select() {
        // This tests
        // SELECT id, name, age, invalid_column FROM select_test_db.test_table1;

        let columns = to_selectitems(vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "invalid_column".to_string(),
        ]);
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
            [
                vec![
                    Value::I64(1),
                    Value::String("Robert Downey Jr.".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Tom Holland".to_string()),
                    Value::I64(20),
                ],
            ]
            .to_vec(),
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        );

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

        let columns = to_selectitems(vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
        ]);
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
            [
                vec![
                    Value::I64(1),
                    Value::String("Robert Downey Jr.".to_string()),
                    Value::I64(40),
                ],
                vec![
                    Value::I64(2),
                    Value::String("Tom Holland".to_string()),
                    Value::I64(20),
                ],
            ]
            .to_vec(),
            "test_table1".to_string(),
            &new_db,
            &mut user,
        )
        .unwrap();

        // Run the SELECT query
        let user: User = User::new("test_user".to_string());
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        );

        // Verify that SELECT failed
        assert!(result.is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    fn test_insert_columns() {
        // SELECT T.id, T.name FROM select_test_db.test_table1 T;
        let columns = to_selectitems(vec!["T.id".to_string(), "T.name".to_string()]);
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
        let (_, diff) =
            insert(rows.clone(), "test_table1".to_string(), &new_db, &mut user).unwrap();

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
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "T.id".to_string());
        assert_eq!(result.0[1], "T.name".to_string());

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
            vec![
                Value::I64(1),
                Value::String("Iron Man".to_string()),
                Value::I64(40),
            ],
            vec![
                Value::I64(2),
                Value::String("Spiderman".to_string()),
                Value::I64(20),
            ],
            vec![Value::I64(3), Value::I64(35)],
            vec![
                Value::I64(4),
                Value::String("Captain America".to_string()),
                Value::I64(35),
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
        let columns = to_selectitems(vec!["T.id".to_string(), "T.name".to_string()]);
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

        let (_, diff) =
            insert(rows.clone(), "test_table1".to_string(), &new_db, &mut user).unwrap();

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
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "T.id".to_string());
        assert_eq!(result.0[1], "T.name".to_string());

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
                Value::I32(100), // Can only insert I32
                Value::String("Iron Man".to_string()),
                Value::String("Robert Downey".to_string()),
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
                Value::I64(4),
                Value::String("Captain America".to_string()),
                Value::Float(12.456),
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
        let columns = to_selectitems(vec!["id".to_string(), "name".to_string()]);
        let tables = vec![("test_table1".to_string(), "".to_string())]; // [(table_name, alias)]

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
                Value::I32(100),
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
        let result = select(
            columns.to_owned(),
            None,
            vec![],
            vec![],
            &tables,
            &new_db,
            &user,
        )
        .unwrap();

        assert_eq!(result.0[0], "id".to_string());
        assert_eq!(result.0[1], "name".to_string());

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
                Value::Null, // Nulled
                Value::String("Iron Man".to_string()),
                Value::String("Robert Downey".to_string()),
            ],
            vec![
                Value::I64(2),
                Value::String("Spiderman".to_string()),
                Value::String("".to_string()),
            ],
            vec![Value::I64(3), Value::Null, Value::Float(322.456)],
            vec![
                Value::I64(4),
                Value::String("Captain America".to_string()),
                Value::Null,
            ],
        ];

        assert!(insert(rows, "test_table1".to_string(), &new_db, &mut user).is_err());

        // Delete the test database
        new_db.delete_database().unwrap();
    }

    #[test]
    #[serial]
    // Test update row with one specific row
    fn test_update_single_row() {
        let mut user = create_demo_db("personal_info");
        let _res = execute_update(
            &parse("UPDATE personal_info SET age = 50 WHERE id = 1", false).unwrap(),
            &mut user,
            &"".to_string(),
        );
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        for row in results {
            if let Value::I64(x) = row[3] {
                if let Value::I32(y) = row[0] {
                    if y == 1 {
                        assert!(x == 50);
                    }
                }
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    // Test update row with multiple rows
    fn test_update_multiple_row() {
        let mut user = create_demo_db("personal_info");
        let _res = execute_update(
            &parse("UPDATE personal_info SET age = 55", false).unwrap(),
            &mut user,
            &"".to_string(),
        );
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        for row in results {
            if let Value::I64(x) = row[3] {
                assert!(x == 55);
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    // Test update row with multiple rows using more complex logic
    fn test_update_row_predicate() {
        let mut user = create_demo_db("personal_info");
        let _res = execute_update(
            &parse("UPDATE personal_info SET age = 55 WHERE id < 10", false).unwrap(),
            &mut user,
            &"".to_string(),
        );
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        for row in results {
            if let Value::I64(x) = row[3] {
                if let Value::I32(y) = row[0] {
                    if y < 10 {
                        assert!(x == 55);
                    }
                }
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }
    #[test]
    #[serial]
    // Test delete a single row from the database
    fn test_delete_single_row() {
        let mut user = create_demo_db("personal_info");
        let _res = execute_update(
            &parse("DELETE FROM personal_info WHERE id = 27", false).unwrap(),
            &mut user,
            &"".to_string(),
        );
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        assert!(results.len() == 24);
        for row in results {
            if let Value::I32(x) = row[0] {
                assert!(x != 27);
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }
    #[test]
    #[serial]
    // Test deleting multiple rows on the database
    fn test_delete_multiple_rows() {
        let mut user = create_demo_db("personal_info");
        let _res = execute_update(
            &parse("DELETE FROM personal_info WHERE id < 25", false).unwrap(),
            &mut user,
            &"".to_string(),
        );
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        assert!(results.len() == 16);
        delete_db_instance().unwrap();
    }
    #[test]
    #[serial]
    // Test order by command ASC
    fn test_order_by_asc() {
        let mut user = create_demo_db("personal_info");
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info ORDER BY id ASC", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        let mut temp = 0;
        for row in results {
            if let Value::I32(x) = row[0] {
                assert!(x >= temp);
                temp = x;
            } else {
                panic!("Invalid value type");
            }
        }
    }
    #[test]
    #[serial]
    // Test order by command DESC
    fn test_order_by_desc() {
        let mut user = create_demo_db("personal_info");
        let (_, results) = execute_query(
            &parse("SELECT * from personal_info ORDER BY id DESC", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        let mut temp = 100;
        for row in results {
            if let Value::I32(x) = row[0] {
                assert!(x <= temp);
                temp = x;
            } else {
                panic!("Invalid value type");
            }
        }
    }

    #[test]
    #[serial]
    // Test set operation with incompatible types
    fn test_set_operations_incompatible_types() {
        let mut user = create_demo_db("set_op_test");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let results = execute_query(
            &parse(
                "SELECT * from personal_info UNION SELECT * from locations",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap_err();
        println!("THIS IS PRINTING {:?}", results);
        assert!(results == "Incompatible types in set operation");
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_set_operations_union() {
        let mut user = create_demo_db("set_op_test");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("first_name".to_string(), Column::String(50)),
            ("last_name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I64),
            ("height".to_string(), Column::Float),
            ("date_inserted".to_string(), Column::Timestamp),
        ];
        create_table(
            &"test_table".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();

        let results = execute_query(
            &parse(
                "SELECT * from personal_info UNION SELECT * from test_table",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let results2 = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results == results2);
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_set_operations_except() {
        let mut user = create_demo_db("set_op_test");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("first_name".to_string(), Column::String(50)),
            ("last_name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I64),
            ("height".to_string(), Column::Float),
            ("date_inserted".to_string(), Column::Timestamp),
        ];
        create_table(
            &"test_table".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();

        let results = execute_query(
            &parse(
                "SELECT * from personal_info except SELECT * from test_table",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let results2 = execute_query(
            &parse("SELECT * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results == results2);
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_set_operations_intersect() {
        let mut user = create_demo_db("set_op_test");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("first_name".to_string(), Column::String(50)),
            ("last_name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I64),
            ("height".to_string(), Column::Float),
            ("date_inserted".to_string(), Column::Timestamp),
        ];
        create_table(
            &"test_table".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();

        let results = execute_query(
            &parse(
                "SELECT * from personal_info intersect SELECT * from test_table",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let results2 = execute_query(
            &parse("SELECT * from test_table", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results == results2);
        delete_db_instance().unwrap();
    }
}
