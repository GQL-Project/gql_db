use std::cmp::Ordering;
use std::collections::HashMap;

use crate::util::dbtype::Column;
use crate::util::dbtype::Value;
use crate::util::row::Row;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use super::query::resolve_reference;

/// Basically, these are pointers to functions that can take a row and return a bool
/// We could also encounter an invalid operation, like 1 + 'a' or 'a' + 'b'
/// They have to be done this way, as the actual function itself is not known until runtime
/// The SolvePredicate Function Type takes a row and returns a bool, used to filter rows
pub type SolvePredicate = Box<dyn Fn(&Row) -> Result<bool, String>>;
/// The SolveValue Function Type takes a row and returns a Value, which is used by SolvePredicate
type SolveValue = Box<dyn Fn(&Row) -> Result<JointValues, String>>;

// We could encounter cases with two different types of values, so we need to be able to handle both
#[derive(Debug)]
enum JointValues {
    DBValue(Value),
    SQLValue(SqlValue),
}

/// Given a predicate and a row, return a bool or an error
pub fn resolve_predicate(pred: &Option<SolvePredicate>, row: &Row) -> Result<bool, String> {
    match pred {
        Some(pred) => pred(row),
        None => Ok(true),
    }
}

// Currently, this is implemented recursively, see if we can do it iteratively
pub fn solve_predicate(
    pred: &Expr,
    column_names: &Vec<(String, Column, String)>,
    index_refs: &HashMap<String, usize>,
) -> Result<SolvePredicate, String> {
    match pred {
        Expr::Identifier(_) => {
            let solve_value = solve_value(pred, column_names, index_refs)?;
            Ok(Box::new(move |row| {
                let value = solve_value(row)?;
                match value {
                    JointValues::DBValue(Value::Bool(x)) => Ok(x),
                    JointValues::SQLValue(SqlValue::Boolean(x)) => Ok(x),
                    _ => Err(format!("Cannot compare value {:?} to bool", value)),
                }
            }))
        }
        Expr::IsFalse(pred) => {
            let pred = solve_predicate(pred, column_names, index_refs)?;
            Ok(Box::new(move |row| Ok(!pred(row)?)))
        }
        Expr::IsNotFalse(pred) => solve_predicate(pred, column_names, index_refs),
        Expr::IsTrue(pred) => solve_predicate(pred, column_names, index_refs),
        Expr::IsNotTrue(pred) => {
            let pred = solve_predicate(pred, column_names, index_refs)?;
            Ok(Box::new(move |row| Ok(!pred(row)?)))
        }
        Expr::IsNull(pred) => {
            let pred = solve_value(pred, column_names, index_refs)?;
            Ok(Box::new(move |row| match pred(row)? {
                JointValues::DBValue(Value::Null) => Ok(true),
                JointValues::SQLValue(SqlValue::Null) => Ok(true),
                _ => Ok(false),
            }))
        }
        Expr::IsNotNull(pred) => {
            let pred = solve_value(pred, column_names, index_refs)?;
            Ok(Box::new(move |row| match pred(row)? {
                JointValues::DBValue(Value::Null) => Ok(false),
                JointValues::SQLValue(SqlValue::Null) => Ok(false),
                _ => Ok(true),
            }))
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Gt => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.gt(&right))
                }))
            }
            BinaryOperator::Lt => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.lt(&right))
                }))
            }
            BinaryOperator::GtEq => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.ge(&right))
                }))
            }
            BinaryOperator::LtEq => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.le(&right))
                }))
            }
            BinaryOperator::Eq => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.eq(&right))
                }))
            }
            BinaryOperator::NotEq => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.ne(&right))
                }))
            }
            BinaryOperator::And => {
                let left = solve_predicate(left, column_names, index_refs)?;
                let right = solve_predicate(right, column_names, index_refs)?;
                Ok(Box::new(move |row| Ok(left(row)? && right(row)?)))
            }
            BinaryOperator::Or => {
                let left = solve_predicate(left, column_names, index_refs)?;
                let right = solve_predicate(right, column_names, index_refs)?;
                Ok(Box::new(move |row| Ok(left(row)? || right(row)?)))
            }
            _ => Err(format!("Unsupported binary operator for Predicate: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let expr = solve_predicate(expr, column_names, index_refs)?;
                Ok(Box::new(move |row| Ok(!expr(row)?)))
            }
            _ => Err(format!("Unsupported unary operator for Predicate: {}", op)),
        },
        Expr::Nested(pred) => solve_predicate(pred, column_names, index_refs),
        _ => Err(format!("Invalid Predicate Clause: {}", pred)),
    }
}

fn solve_value(
    pred: &Expr,
    column_names: &Vec<(String, Column, String)>,
    index_refs: &HashMap<String, usize>,
) -> Result<SolveValue, String> {
    match pred {
        Expr::Identifier(x) => {
            let x = resolve_reference(x.value.to_string(), column_names)?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;
            // Force the closure to take `index` ownership
            // Then, create a closure that takes in a row and returns the value at the index
            Ok(Box::new(move |row: &Row| {
                Ok(JointValues::DBValue(row[index].clone()))
            }))
        }
        Expr::CompoundIdentifier(list) => {
            // Join all the identifiers in the list with a dot
            let x = resolve_reference(
                list.iter()
                    .map(|x| x.value.to_string())
                    .collect::<Vec<String>>()
                    .join("."),
                column_names,
            )?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;
            Ok(Box::new(move |row: &Row| {
                Ok(JointValues::DBValue(row[index].clone()))
            }))
        }
        Expr::Nested(x) => solve_value(x, column_names, index_refs),
        Expr::Value(x) => {
            let val = x.clone();
            Ok(Box::new(move |_| Ok(JointValues::SQLValue(val.clone()))))
        }
        Expr::BinaryOp {
            left: _,
            op,
            right: _,
        } => match op {
            BinaryOperator::Plus => todo!(),
            BinaryOperator::Minus => todo!(),
            BinaryOperator::Multiply => todo!(),
            BinaryOperator::Divide => todo!(),
            BinaryOperator::Modulo => todo!(),
            _ => Err(format!("Invalid Binary Operator for Value: {}", op)),
        },
        Expr::UnaryOp { op: _, expr: _ } => todo!(),
        _ => Err(format!("Unexpected Value Clause: {}", pred)),
    }
}

impl PartialEq for JointValues {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DBValue(l0), Self::DBValue(r0)) => l0 == r0,
            (Self::SQLValue(l0), Self::SQLValue(r0)) => l0 == r0,
            (Self::DBValue(l0), Self::SQLValue(r0)) => {
                if let Ok(r0) = l0.get_coltype().from_sql_value(r0) {
                    l0 == &r0
                } else {
                    false
                }
            }
            (Self::SQLValue(r0), Self::DBValue(l0)) => {
                if let Ok(r0) = l0.get_coltype().from_sql_value(r0) {
                    l0 == &r0
                } else {
                    false
                }
            }
        }
    }
}

impl PartialOrd for JointValues {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::DBValue(l0), Self::DBValue(r0)) => l0.partial_cmp(r0),
            (Self::SQLValue(l0), Self::SQLValue(r0)) => {
                Value::from_sql_value(l0).partial_cmp(&Value::from_sql_value(r0))
            }
            (Self::DBValue(l0), Self::SQLValue(r0)) => {
                if let Ok(r0) = l0.get_coltype().from_sql_value(r0) {
                    l0.partial_cmp(&r0)
                } else {
                    None
                }
            }
            (Self::SQLValue(r0), Self::DBValue(l0)) => {
                if let Ok(r0) = l0.get_coltype().from_sql_value(r0) {
                    l0.partial_cmp(&r0)
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {}
