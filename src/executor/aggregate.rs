use std::collections::HashMap;

use crate::util::dbtype::{Column, Value};
use crate::util::row::Row;

use sqlparser::ast::{BinaryOperator, Expr, FunctionArgExpr, UnaryOperator};

use super::predicate::{resolve_reference, solve_value, JointValues};
use super::query::ColumnAliases;
use super::query::IndexRefs;

/// This function takes a list of rows and the column selection, and performs aggregate function application on the rows.
pub fn resolve_aggregates(
    rows: Vec<(Row, Row)>,
    selections: &Vec<Expr>,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<Vec<Row>, String> {
    // Filter down to functions only and their indices
    let functions = selections
        .iter()
        .enumerate()
        .filter(|(_, expr)| contains_aggregate(expr).map_or(false, |x| x))
        .collect::<Vec<(usize, &Expr)>>();

    let (value_rows, original_rows): (Vec<Row>, Vec<Row>) = rows.into_iter().unzip();
    if functions.is_empty() {
        return Ok(value_rows);
    }
    if value_rows.is_empty() {
        return Ok(vec![]);
    }

    // Take the first row, and solve the functions for it for the entire group
    let mut row = value_rows[0].clone();
    for (i, expr) in functions {
        row[i] = solve_aggregate(&original_rows, expr, column_aliases, index_refs)?;
    }
    Ok(vec![row])
}

/// Versions of the Solvers that just return the Value directly
pub fn solve_agg_predicate(
    rows: &Vec<Row>,
    pred: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<bool, String> {
    match pred {
        Expr::Identifier(_) => {
            let value = solve_aggregate(rows, pred, column_aliases, index_refs)?;
            match value {
                Value::Bool(x) => Ok(x),
                _ => Err(format!("Cannot compare value {:?} to bool", value)),
            }
        }
        Expr::IsFalse(pred) => Ok(!solve_agg_predicate(
            rows,
            pred,
            column_aliases,
            index_refs,
        )?),
        Expr::IsNotFalse(pred) => solve_agg_predicate(rows, pred, column_aliases, index_refs),
        Expr::IsTrue(pred) => solve_agg_predicate(rows, pred, column_aliases, index_refs),
        Expr::IsNotTrue(pred) => Ok(!solve_agg_predicate(
            rows,
            pred,
            column_aliases,
            index_refs,
        )?),
        Expr::IsNull(pred) => {
            let value = solve_aggregate(rows, pred, column_aliases, index_refs)?;
            match value {
                Value::Null(_) => Ok(true),
                _ => Ok(false),
            }
        }
        Expr::IsNotNull(pred) => {
            let value = solve_aggregate(rows, pred, column_aliases, index_refs)?;
            match value {
                Value::Null(_) => Ok(false),
                _ => Ok(true),
            }
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let left =
                    JointValues::DBValue(solve_aggregate(rows, left, column_aliases, index_refs)?);
                let right =
                    JointValues::DBValue(solve_aggregate(rows, right, column_aliases, index_refs)?);
                match op {
                    BinaryOperator::Gt => Ok(left > right),
                    BinaryOperator::Lt => Ok(left < right),
                    BinaryOperator::GtEq => Ok(left >= right),
                    BinaryOperator::LtEq => Ok(left <= right),
                    BinaryOperator::Eq => Ok(left == right),
                    BinaryOperator::NotEq => Ok(left != right),
                    _ => Err(format!("Invalid comparison operator {:?}", op)),
                }
            }
            BinaryOperator::And => {
                let left = solve_agg_predicate(rows, left, column_aliases, index_refs)?;
                let right = solve_agg_predicate(rows, right, column_aliases, index_refs)?;
                Ok(left && right)
            }
            BinaryOperator::Or => {
                let left = solve_agg_predicate(rows, left, column_aliases, index_refs)?;
                let right = solve_agg_predicate(rows, right, column_aliases, index_refs)?;
                Ok(left || right)
            }
            _ => Err(format!("Unsupported binary operator for Predicate: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let left = solve_agg_predicate(rows, expr, column_aliases, index_refs)?;
                Ok(!left)
            }
            _ => Err(format!("Unsupported unary operator for Predicate: {}", op)),
        },
        Expr::Nested(pred) => solve_agg_predicate(rows, pred, column_aliases, index_refs),
        _ => Err(format!("Invalid Predicate Clause: {}", pred)),
    }
}

pub fn solve_aggregate(
    rows: &Vec<Row>,
    expr: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<Value, String> {
    let row = &rows[0];
    match expr {
        Expr::Identifier(x) => {
            let x = resolve_reference(x.value.to_string(), column_aliases)?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;
            Ok(row[index].clone())
        }
        Expr::CompoundIdentifier(list) => {
            let x = resolve_reference(
                list.iter()
                    .map(|x| x.value.to_string())
                    .collect::<Vec<String>>()
                    .join("."),
                column_aliases,
            )?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;
            Ok(row[index].clone())
        }
        Expr::Nested(x) => solve_aggregate(rows, x, column_aliases, index_refs),
        Expr::Value(x) => JointValues::SQLValue(x.clone()).unpack(),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left =
                    JointValues::DBValue(solve_aggregate(rows, left, column_aliases, index_refs)?);
                let right =
                    JointValues::DBValue(solve_aggregate(rows, right, column_aliases, index_refs)?);
                match op {
                    BinaryOperator::Plus => left.add(&right),
                    BinaryOperator::Minus => left.subtract(&right),
                    BinaryOperator::Multiply => left.multiply(&right),
                    BinaryOperator::Divide => left.divide(&right),
                    BinaryOperator::Modulo => left.modulo(&right),
                    _ => Err(format!("Invalid binary operator {:?}", op)),
                }?
                .unpack()
            }
            BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let binary = solve_agg_predicate(rows, expr, column_aliases, index_refs)?;
                Ok(Value::Bool(binary))
            }
            _ => Err(format!("Invalid Binary Operator for Value: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => {
                let val =
                    JointValues::DBValue(solve_aggregate(rows, expr, column_aliases, index_refs)?);
                val.unpack()
            }
            UnaryOperator::Minus => {
                let val =
                    JointValues::DBValue(solve_aggregate(rows, expr, column_aliases, index_refs)?);
                JointValues::DBValue(Value::I32(0)).subtract(&val)?.unpack()
            }
            UnaryOperator::Not => {
                let binary = solve_agg_predicate(rows, expr, column_aliases, index_refs)?;
                Ok(Value::Bool(binary))
            }
            _ => Err(format!("Invalid Unary Operator for Value: {}", op)),
        },
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            let args = &func.args;
            if args.len() != 1 {
                return Err(format!(
                    "Invalid number of arguments for {}: {}",
                    name,
                    args.len()
                ));
            }

            match name.as_str() {
                "count" => {
                    match &args[0] {
                        sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                            // Count number of non-null values in the column
                            FunctionArgExpr::Expr(expr) => {
                                aggregate_count(rows, &Some(expr), column_aliases, index_refs)
                            }
                            _ => aggregate_count(rows, &None, column_aliases, index_refs),
                        },
                        _ => Err(format!("Unsupported arguments {}", args[0])),
                    }
                }
                "sum" => match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                        FunctionArgExpr::Expr(expr) => {
                            aggregate_sum(rows, expr, column_aliases, index_refs)
                        }
                        _ => Err(format!("Unsupported arguments {}", args[0])),
                    },
                    _ => Err(format!("Unsupported arguments {}", args[0])),
                },
                "avg" => match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                        FunctionArgExpr::Expr(expr) => {
                            let sum = JointValues::DBValue(aggregate_sum(
                                rows,
                                expr,
                                column_aliases,
                                index_refs,
                            )?);
                            let count = JointValues::DBValue(aggregate_count(
                                rows,
                                &Some(expr),
                                column_aliases,
                                index_refs,
                            )?);
                            sum.divide(&count)?.unpack()
                        }
                        _ => Err(format!("Unsupported arguments {}", args[0])),
                    },
                    _ => Err(format!("Unsupported arguments {}", args[0])),
                },
                "min" => match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                        FunctionArgExpr::Expr(expr) => {
                            aggregate_min(expr, column_aliases, index_refs, rows)
                        }
                        _ => Err(format!("Unsupported arguments {}", args[0])),
                    },
                    _ => Err(format!("Unsupported arguments {}", args[0])),
                },
                "max" => match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                        FunctionArgExpr::Expr(expr) => {
                            aggregate_max(expr, column_aliases, index_refs, rows)
                        }
                        _ => Err(format!("Unsupported arguments {}", args[0])),
                    },
                    _ => Err(format!("Unsupported arguments {}", args[0])),
                },
                _ => Err(format!("Unsupported aggregate function: {}", name)),
            }
        }
        _ => Err(format!("Unexpected Value Clause: {}", expr)),
    }
}

fn aggregate_count(
    rows: &Vec<Row>,
    expr: &Option<&Expr>,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<Value, String> {
    match expr {
        Some(expr) => {
            let solver = solve_value(expr, column_aliases, index_refs)?;
            let mut count = 0;
            for row in rows {
                let val = solver(row)?;
                if val.is_null() {
                    count += 1;
                }
            }
            Ok(Value::I32(count as i32))
        }
        None => Ok(Value::I32(rows.len() as i32)),
    }
}

fn aggregate_sum(
    rows: &Vec<Row>,
    expr: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<Value, String> {
    let solver = solve_value(expr, column_aliases, index_refs)?;
    let mut sum: Option<JointValues> = None;
    for row in rows {
        let val = solver(row)?;
        sum = match sum {
            Some(sum) => Some(sum.add(&val)?),
            None => Some(val),
        };
    }
    match sum {
        Some(v) => v.unpack(),
        None => Ok(Value::Null(Column::I32)),
    }
}

fn aggregate_min(
    expr: &Expr,
    column_aliases: &Vec<(String, crate::util::dbtype::Column, String)>,
    index_refs: &HashMap<String, usize>,
    rows: &Vec<Vec<Value>>,
) -> Result<Value, String> {
    let solver = solve_value(expr, column_aliases, index_refs)?;
    let mut min: Option<JointValues> = None;
    for row in rows {
        let val = solver(row)?;
        min = match min {
            Some(min) => {
                if min < val {
                    Some(min)
                } else {
                    Some(val)
                }
            }
            None => Some(val),
        };
    }
    match min {
        Some(v) => v.unpack(),
        None => Ok(Value::Null(Column::I32)),
    }
}

fn aggregate_max(
    expr: &Expr,
    column_aliases: &Vec<(String, crate::util::dbtype::Column, String)>,
    index_refs: &HashMap<String, usize>,
    rows: &Vec<Vec<Value>>,
) -> Result<Value, String> {
    let solver = solve_value(expr, column_aliases, index_refs)?;
    let mut max: Option<JointValues> = None;
    for row in rows {
        let val = solver(row)?;
        max = match max {
            Some(max) => {
                if max > val {
                    Some(max)
                } else {
                    Some(val)
                }
            }
            None => Some(val),
        };
    }
    match max {
        Some(v) => v.unpack(),
        None => Ok(Value::Null(Column::I32)),
    }
}

/// Takes an expression, and returns true if there is a Function expression nested somewhere inside it.
pub fn contains_aggregate(expr: &Expr) -> Result<bool, String> {
    match expr {
        Expr::Identifier(_) => Ok(false),
        Expr::CompoundIdentifier(_) => Ok(false),
        Expr::Nested(x) => contains_aggregate(x),
        Expr::Value(_) => Ok(false),
        Expr::BinaryOp { left, op: _, right } => {
            Ok(contains_aggregate(left)? || contains_aggregate(right)?)
        }
        Expr::UnaryOp { op: _, expr } => contains_aggregate(expr),
        Expr::Function(_) => Ok(true),
        _ => Err(format!("Unexpected Clause: {}", expr)),
    }
}

// Where (predicate) tests go here
#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        executor::query::{execute_query, execute_update},
        fileio::databaseio::{delete_db_instance, get_db_instance},
        parser::parser::parse,
        util::{
            bench::create_huge_bench_db,
            dbtype::{Column, Value},
        },
    };

    impl Value {
        pub fn force_int(&self) -> i32 {
            match self {
                Value::I32(x) => *x,
                Value::I64(x) => *x as i32,
                _ => panic!("Expected an integer"),
            }
        }
    }

    #[test]
    #[serial]
    fn test_group_by_simple() {
        let mut user = create_huge_bench_db(312, true);

        let (_, results) = execute_query(
            &parse("select *, count(*), sum(id2) from huge_table group by id2", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        // Assuming the table has id2 between 0 and 51 always
        assert!(results.len() == 52);

        
        let mut count = 0;
        for row in results {
            // Ensure total count is 300, and each count is either 5 or 6 (300 / 52)
            let val = row[4].force_int();
            assert!(val == 6);
            count += val;
            // Ensure each sum is count * id2
            let val = row[4].force_int();
            let sum = row[5].force_int();
            assert!(sum == val * row[1].force_int());
        }
        assert!(count == 312);
        
    }

    #[test]
    #[serial]
    fn test_non_group_aggregate() {
        let mut user = create_huge_bench_db(300, true);

        let (_, results) = execute_query(
            &parse("select count(*) from huge_table", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results.len() == 1);
        assert!(results[0][0].force_int() == 300);

        let (_, results) = execute_query(
            &parse("select sum(id1) from huge_table", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results.len() == 1);
        assert!(results[0][0].force_int() == (300 * 299) / 2);
    }
}
