use std::cmp::Ordering;
use std::collections::HashMap;

use crate::util::dbtype::Value;
use crate::util::row::Row;
use prost_types::Timestamp;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};
use sqlparser::ast::{OrderByExpr, Value as SqlValue};

use super::aggregate::contains_aggregate;
use super::query::ColumnAliases;
use super::query::IndexRefs;

/// Basically, these are pointers to functions that can take a row and return a bool
/// We could also encounter an invalid operation, like 1 + 'a' or 'a' + 'b'
/// They have to be done this way, as the actual function itself is not known until runtime
/// The SolvePredicate Function Type takes a row and returns a bool, used to filter rows
pub type PredicateSolver = Box<dyn Fn(&Row) -> Result<bool, String>>;
/// The SolveValue Function Type takes a row and returns a Value, which is used by SolvePredicate
/// It's also used to resolve the value of a column in a row, such as in `select id + 5 from table`
/// Think of both of these functions as a 'solver' given a row, it will reduce the row to a value,
/// as defined by the expression in the query.
pub type ValueSolver = Box<dyn Fn(&Row) -> Result<JointValues, String>>;

/// A comparator between two Rows, used to sort rows in a ORDER BY clause
/// The comparator is a function that takes two rows and returns an Ordering
/// Ordering is an enum that can be Less, Equal, or Greater
pub type ComparisonSolver = Box<dyn Fn(&Row, &Row) -> Result<Ordering, String>>;

// We could encounter cases with two different types of values, so we need to be able to handle both
#[derive(Debug)]
pub enum JointValues {
    DBValue(Value),
    SQLValue(SqlValue),
}

/// Given a predicate and a row, return a bool or an error
pub fn resolve_predicate(pred: &Option<PredicateSolver>, row: &Row) -> Result<bool, String> {
    match pred {
        Some(pred) => pred(row),
        None => Ok(true), // If there's no predicate, then it's always true
    }
}

/// Converts the type into a 'Proper' Value, which is used by the database
pub fn resolve_value(solver: &ValueSolver, row: &Row) -> Result<Value, String> {
    solver(row)?.unpack()
}

/// Given a ComparisonSolver and two rows, return an Ordering or an error
pub fn resolve_comparison(comp: &ComparisonSolver, row1: &Row, row2: &Row) -> Ordering {
    match comp(row1, row2) {
        Ok(o) => o,
        Err(_) => Ordering::Less, // If there's an error, then we can't compare, so we just say they're less
    }
}

// Resolve a pure value without a row, such as in `select 5 + 5`
pub fn resolve_pure_value(expr: &Expr) -> Result<Value, String> {
    Ok(resolve_value(
        &solve_value(&expr, &vec![], &HashMap::new())?,
        &vec![],
    )?)
}

/// We know a lot of information already about the expression, so we can 'reduce' it
/// into just a function that takes a row and outputs true or false. This way, we don't
/// have to re-parse the function every time, and we have a direct function to call
/// when we need to filter rows.
/// Currently, this is implemented recursively, see if we can do it iteratively
pub fn solve_predicate(
    pred: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<PredicateSolver, String> {
    match pred {
        Expr::Identifier(_) => {
            let solve_value = solve_value(pred, column_aliases, index_refs)?;
            Ok(Box::new(move |row| {
                // Figure out the whether the value of the column cell is a boolean or not.
                let value = solve_value(row)?;
                match value {
                    JointValues::DBValue(Value::Bool(x)) => Ok(x),
                    JointValues::SQLValue(SqlValue::Boolean(x)) => Ok(x),
                    _ => Err(format!("Cannot compare value {:?} to bool", value)),
                }
            }))
        }
        Expr::IsFalse(pred) => {
            let pred = solve_predicate(pred, column_aliases, index_refs)?;
            Ok(Box::new(move |row| Ok(!pred(row)?)))
        }
        Expr::IsNotFalse(pred) => solve_predicate(pred, column_aliases, index_refs),
        Expr::IsTrue(pred) => solve_predicate(pred, column_aliases, index_refs),
        Expr::IsNotTrue(pred) => {
            let pred = solve_predicate(pred, column_aliases, index_refs)?;
            Ok(Box::new(move |row| Ok(!pred(row)?)))
        }
        Expr::IsNull(pred) => {
            let pred = solve_value(pred, column_aliases, index_refs)?;
            Ok(Box::new(move |row| match pred(row)? {
                JointValues::DBValue(Value::Null) => Ok(true),
                JointValues::SQLValue(SqlValue::Null) => Ok(true),
                _ => Ok(false),
            }))
        }
        Expr::IsNotNull(pred) => {
            let pred = solve_value(pred, column_aliases, index_refs)?;
            Ok(Box::new(move |row| match pred(row)? {
                JointValues::DBValue(Value::Null) => Ok(false),
                JointValues::SQLValue(SqlValue::Null) => Ok(false),
                _ => Ok(true),
            }))
        }
        Expr::BinaryOp { left, op, right } => match op {
            // Resolve values from the two sides of the expression, and then perform
            // the comparison on the two values
            BinaryOperator::Gt => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.gt(&right))
                }))
            }
            BinaryOperator::Lt => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.lt(&right))
                }))
            }
            BinaryOperator::GtEq => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.ge(&right))
                }))
            }
            BinaryOperator::LtEq => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.le(&right))
                }))
            }
            BinaryOperator::Eq => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.eq(&right))
                }))
            }
            BinaryOperator::NotEq => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    Ok(left.ne(&right))
                }))
            }
            // Create functions for the LHS and RHS of the 'and' operation, and then
            // combine them into a single function that returns true if both functions return true
            // Note how this would also indirectly handle short-circuiting
            BinaryOperator::And => {
                let left = solve_predicate(left, column_aliases, index_refs)?;
                let right = solve_predicate(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| Ok(left(row)? && right(row)?)))
            }
            BinaryOperator::Or => {
                let left = solve_predicate(left, column_aliases, index_refs)?;
                let right = solve_predicate(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| Ok(left(row)? || right(row)?)))
            }
            _ => Err(format!("Unsupported binary operator for Predicate: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let expr = solve_predicate(expr, column_aliases, index_refs)?;
                Ok(Box::new(move |row| Ok(!expr(row)?)))
            }
            _ => Err(format!("Unsupported unary operator for Predicate: {}", op)),
        },
        Expr::Nested(pred) => solve_predicate(pred, column_aliases, index_refs),
        _ => Err(format!("Invalid Predicate Clause: {}", pred)),
    }
}

/// Similar to solve_predicate, this is another function that takes a Row and reduces it to the
/// value described by the expression. In the most simple case, if we have an Expression just
/// referencing a column name, we just take a row and then apply the index on that row.
/// The main difference between this and solve_predicate is that we can return a Value, instead of
/// a boolean.
pub fn solve_value(
    expr: &Expr,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<ValueSolver, String> {
    if contains_aggregate(expr)? {
        // In this case, we need to just let it pass through, as we only want to evaluate the function when we need to
        // (i.e. when we're evaluating the groups)
        return Ok(Box::new(move |_| Ok(JointValues::DBValue(Value::Null))));
    }
    match expr {
        // This would mean that we're referencing a column name, so we just need to figure out the
        // index of that column name in the row, and then return a function that references this index
        // in the provided row.
        Expr::Identifier(x) => {
            let x = resolve_reference(x.value.to_string(), column_aliases)?;
            let index = *index_refs
                .get(&x)
                .ok_or(format!("Column {} does not exist in the table", x))?;
            // Force the closure to take `index` ownership (the index value is copied into the function below)
            // Then, create a closure that takes in a row and returns the value at the index
            Ok(Box::new(move |row: &Row| {
                Ok(JointValues::DBValue(row[index].clone()))
            }))
        }
        Expr::CompoundIdentifier(list) => {
            // Join all the identifiers in the list with a dot, perform the same step as above
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
            Ok(Box::new(move |row: &Row| {
                Ok(JointValues::DBValue(row[index].clone()))
            }))
        }
        Expr::Nested(x) => solve_value(x, column_aliases, index_refs),
        Expr::Value(x) => {
            // Create a copy of the value
            let val = x.clone();
            // Move a reference of this value into the closure, so that we can reference
            // it when we wish to respond with a Value.
            Ok(Box::new(move |_| Ok(JointValues::SQLValue(val.clone()))))
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.add(&right)
                }))
            }
            BinaryOperator::Minus => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.subtract(&right)
                }))
            }
            BinaryOperator::Multiply => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.multiply(&right)
                }))
            }
            BinaryOperator::Divide => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.divide(&right)
                }))
            }
            BinaryOperator::Modulo => {
                let left = solve_value(left, column_aliases, index_refs)?;
                let right = solve_value(right, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.modulo(&right)
                }))
            }
            BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let binary = solve_predicate(expr, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let pred = binary(row)?;
                    Ok(JointValues::DBValue(Value::Bool(pred)))
                }))
            }
            _ => Err(format!("Invalid Binary Operator for Value: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => {
                let expr = solve_value(expr, column_aliases, index_refs)?;
                Ok(Box::new(move |row| expr(row)))
            }
            UnaryOperator::Minus => {
                let expr = solve_value(expr, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let val = expr(row)?;
                    JointValues::DBValue(Value::I32(0)).subtract(&val)
                }))
            }
            UnaryOperator::Not => {
                // Solve the inner value, expecting it's return type to be a boolean, and negate it.
                let binary = solve_predicate(expr, column_aliases, index_refs)?;
                Ok(Box::new(move |row| {
                    let pred = binary(row)?;
                    Ok(JointValues::DBValue(Value::Bool(!pred)))
                }))
            }
            _ => Err(format!("Invalid Unary Operator for Value: {}", op)),
        },
        _ => Err(format!("Unexpected Value Clause: {}", expr)),
    }
}

/// Creates a comparator between two rows, given a series of Expr's to use as the comparison
/// between the two rows.
pub fn solve_comparison(
    order_bys: &Vec<OrderByExpr>,
    column_aliases: &ColumnAliases,
    index_refs: &IndexRefs,
) -> Result<ComparisonSolver, String> {
    let comparators = order_bys
        .iter()
        .map(|order_by| {
            let asc = match order_by.asc {
                Some(asc) => asc,
                None => true, // Default to ascending
            };
            let expr = &order_by.expr;
            let solver = solve_value(expr, column_aliases, index_refs)?;
            let result: ComparisonSolver = Box::new(move |a: &Row, b: &Row| {
                let a = solver(a)?;
                let b = solver(b)?;
                let ordering = a
                    .partial_cmp(&b)
                    .ok_or(format!("Cannot compare {:?} and {:?}", a, b))?;
                if asc {
                    Ok(ordering)
                } else {
                    Ok(ordering.reverse())
                }
            });
            Ok(result)
        })
        .collect::<Result<Vec<ComparisonSolver>, String>>()?;

    Ok(Box::new(move |a: &Row, b: &Row| {
        for comparator in &comparators {
            let order = comparator(a, b)?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(Ordering::Equal)
    }))
}

// Given a column name, it figures out which table it belongs to and returns the
// unambiguous column name. For example, if we have a table called "users" with
// a column called "id", this would return "users.id". If "users" has an alias
// already, like 'U', it would return "U.id".
pub fn resolve_reference(
    column_name: String,
    column_aliases: &ColumnAliases,
) -> Result<String, String> {
    if column_name.contains(".") {
        // We know this works, as the parser does not allow for '.' in column names
        Ok(column_name)
    } else {
        let matches: Vec<&String> = column_aliases
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

/// When applying some function to two values, we need to know how to treat the
/// two values.
type ApplyInt = fn(i64, i64) -> Result<i64, String>;
type ApplyFloat = fn(f64, f64) -> Result<f64, String>;
type ApplyString = fn(&String, &String) -> Result<String, String>;

impl JointValues {
    pub fn unpack(&self) -> Result<Value, String> {
        match self {
            JointValues::DBValue(v) => Ok(v.clone()),
            JointValues::SQLValue(v) => Value::from_sql_value(&v),
        }
    }

    pub fn add(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x + y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x + y);
        let apply_string = |x: &String, y: &String| Ok::<String, String>(x.to_string() + y);
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot add {:?} and {:?}", self, other))
    }

    pub fn subtract(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x - y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x - y);
        let apply_string = |x: &String, y: &String| Ok::<String, String>(x.replace(y, ""));
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot subtract {:?} and {:?}", self, other))
    }

    pub fn multiply(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x * y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x * y);
        let apply_string = |x: &String, y: &String| {
            let mut result = String::new();
            for _ in 0..y
                .parse::<i64>()
                .map_err(|_| "Cannot multiply string by non-integer")?
            {
                result.push_str(x);
            }
            Ok::<String, String>(result)
        };
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot multiply {:?} and {:?}", self, other))
    }

    pub fn divide(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x / y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x / y);
        let apply_string = |_: &String, _: &String| {
            Err::<String, String>("Cannot divide string by string".to_string())
        };
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot divide {:?} and {:?}", self, other))
    }

    pub fn modulo(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x % y);
        let apply_float =
            |_: f64, _: f64| Err::<f64, String>("Cannot modulus float by float".to_string());
        let apply_string = |_: &String, _: &String| {
            Err::<String, String>("Cannot modulus string by string".to_string())
        };
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot modulus {:?} and {:?}", self, other))
    }

    /// This function applies a function to two values of similar types, casting when necessary.
    /// This takes in three functions, telling us how to treat integers, floats and strings.
    fn apply(
        &self,
        other: &Self,
        int_func: ApplyInt,
        float_func: ApplyFloat,
        string_func: ApplyString,
    ) -> Result<JointValues, String> {
        Ok(match (self, other) {
            (Self::DBValue(l0), Self::DBValue(r0)) => Self::DBValue(match (l0, r0) {
                (Value::I32(l), Value::I32(r)) => {
                    Value::I32(int_func(*l as i64, *r as i64)? as i32)
                }
                (Value::Float(l), Value::Float(r)) => {
                    Value::Float(float_func(*l as f64, *r as f64)? as f32)
                }
                (Value::I64(l), Value::I64(r)) => Value::I64(int_func(*l, *r)?),
                (Value::Double(l), Value::Double(r)) => Value::Double(float_func(*l, *r)?),

                (Value::I32(l), Value::Float(r)) => {
                    Value::Float(float_func(*l as f64, *r as f64)? as f32)
                }
                (Value::I32(l), Value::I64(r)) => Value::I64(int_func(*l as i64, *r)?),
                (Value::I32(l), Value::Double(r)) => Value::Double(float_func(*l as f64, *r)?),

                (Value::Float(l), Value::I32(r)) => {
                    Value::Float(float_func(*l as f64, *r as f64)? as f32)
                }
                (Value::Float(l), Value::I64(r)) => {
                    Value::Double(float_func(*l as f64, *r as f64)?)
                }
                (Value::Float(l), Value::Double(r)) => {
                    Value::Double(float_func(*l as f64, *r as f64)?)
                }

                (Value::I64(l), Value::I32(r)) => Value::I64(int_func(*l, *r as i64)?),
                (Value::I64(l), Value::Float(r)) => {
                    Value::Double(float_func(*l as f64, *r as f64)?)
                }
                (Value::I64(l), Value::Double(r)) => {
                    Value::Double(float_func(*l as f64, *r as f64)?)
                }

                (Value::Double(l), Value::I32(r)) => Value::Double(float_func(*l, *r as f64)?),
                (Value::Double(l), Value::I64(r)) => Value::Double(float_func(*l, *r as f64)?),
                (Value::Double(l), Value::Float(r)) => Value::Double(float_func(*l, *r as f64)?),

                (Value::Timestamp(l), Value::Timestamp(r)) => Value::Timestamp(Timestamp {
                    seconds: int_func(l.seconds, r.seconds)?,
                    nanos: int_func(l.nanos as i64, r.nanos as i64)? as i32,
                }),
                (Value::Timestamp(l), Value::I32(r)) => Value::Timestamp(Timestamp {
                    seconds: int_func(l.seconds, *r as i64)?,
                    nanos: l.nanos,
                }),
                (Value::Timestamp(l), Value::I64(r)) => Value::Timestamp(Timestamp {
                    seconds: int_func(l.seconds, *r)?,
                    nanos: l.nanos,
                }),

                (Value::String(l), Value::String(r)) => Value::String(string_func(l, r)?),
                (Value::String(l), Value::I32(r)) => Value::String(string_func(l, &r.to_string())?),
                (Value::String(l), Value::I64(r)) => Value::String(string_func(l, &r.to_string())?),
                (Value::String(l), Value::Float(r)) => {
                    Value::String(string_func(l, &r.to_string())?)
                }
                (Value::String(l), Value::Double(r)) => {
                    Value::String(string_func(l, &r.to_string())?)
                }
                _ => Err(format!("Cannot apply {:?} and {:?} together", l0, r0))?,
            }),
            // Convert these into DBValues and then apply the function
            (Self::DBValue(x), Self::SQLValue(y)) => self.apply(
                &Self::DBValue(x.get_coltype().from_sql_value(y)?),
                int_func,
                float_func,
                string_func,
            )?,
            (Self::SQLValue(x), Self::DBValue(y)) => Self::DBValue(
                y.get_coltype().from_sql_value(x)?,
            )
            .apply(other, int_func, float_func, string_func)?,
            (Self::SQLValue(x), Self::SQLValue(y)) => {
                let x = Value::from_sql_value(x)?;
                let y = x.get_coltype().from_sql_value(y)?;
                Self::DBValue(x).apply(&Self::DBValue(y), int_func, float_func, string_func)?
            }
        })
    }
}

impl PartialEq for JointValues {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
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

// Where (predicate) tests go here
#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        executor::query::{execute_query, execute_update},
        fileio::databaseio::{delete_db_instance, get_db_instance},
        parser::parser::parse,
        util::{bench::create_demo_db, dbtype::Value},
    };

    #[test]
    #[serial]
    fn test_comparator_predicates() {
        let mut user = create_demo_db("comp_predicates");
        // It's very tricky / weird to test the predicates, so we're just running SQL commands
        // and checking if the results are correct
        let (_, results) = execute_query(
            &parse("select * from personal_info where id < 20", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I32(x) = row[0] {
                assert!(x < 20);
            } else {
                panic!("Invalid value type");
            }
        }

        let (_, results) = execute_query(
            &parse("select * from personal_info where id >= 19", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I32(x) = row[0] {
                assert!(x >= 19);
            } else {
                panic!("Invalid value type");
            }
        }

        let (_, results) = execute_query(
            &parse("select * from personal_info where first_name <= 'D'", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::String(x) = &row[1] {
                assert!(x.as_str() <= "D");
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_equality_predicates() {
        let mut user = create_demo_db("equals_predicates");
        // It's very tricky / weird to test the predicates, so we're just running SQL commands
        // and checking if the results are correct
        let (_, results) = execute_query(
            &parse(
                "select first_name, age, height from personal_info where height is null",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            assert!(row[2] == Value::Null);
        }

        let (_, results) = execute_query(
            &parse(
                "select * from personal_info where age = 32 and height is not null",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I64(x) = row[3] {
                assert!(x == 32);
                assert!(row[4] != Value::Null);
            } else {
                panic!("Invalid value type");
            }
        }

        let (_, results) = execute_query(
            &parse(
                "select * from personal_info where age = 32 and height < 30",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I64(x) = row[3] {
                if let Value::Float(y) = row[4] {
                    assert!(x == 32);
                    assert!(y < 30.0);
                } else {
                    panic!("Invalid value type");
                }
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_nested_predicates() {
        let mut user = create_demo_db("nested_predicates");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        // It's very tricky / weird to test the predicates, so we're just running SQL commands
        // and checking if the results are correct
        let (_, results) = execute_query(
            &parse(
                "select * from personal_info P, locations L where P.id < L.id and (P.age > L.id or (is_open and height is NULL)) and age < 32;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            println!("Row: {:?}", row);
            if let Value::I32(x) = row[0] {
                if let Value::I32(y) = row[6] {
                    assert!(x < y);
                    if let Value::I64(z) = row[3] {
                        assert!(
                            z > y.into() || (row[8] == Value::Bool(true) && row[4] == Value::Null)
                        );
                        assert!(z < 32);
                    } else {
                        panic!("Invalid value type");
                    }
                } else {
                    panic!("Invalid value type");
                }
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_join_predicate() {
        let mut user = create_demo_db("join_predicate");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        // It's very tricky / weird to test the predicates, so we're just running SQL commands
        // and checking if the results are correct
        let (_, results) = execute_query(
            &parse(
                "select * from personal_info P, locations L where P.id = L.id;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I32(x) = row[0] {
                if let Value::I32(y) = row[6] {
                    assert!(x == y);
                } else {
                    panic!("Invalid value type");
                }
            } else {
                panic!("Invalid value type");
            }
        }

        let (_, results) = execute_query(
            &parse(
                "select * from personal_info P, locations L where (P.id < L.id) and (P.id <= P.age);",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I32(x) = row[0] {
                if let Value::I32(y) = row[6] {
                    if let Value::I64(z) = row[3] {
                        assert!(x < y);
                        assert!(x as i64 <= z);
                    } else {
                        panic!("Invalid value type");
                    }
                } else {
                    panic!("Invalid value type");
                }
            } else {
                panic!("Invalid value type");
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_boolean_predicate() {
        let mut user = create_demo_db("bool_predicate");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        // It's very tricky / weird to test the predicates, so we're just running SQL commands
        // and checking if the results are correct
        let (_, results) = execute_query(
            &parse(
                "select * from personal_info P, locations L where not (P.id = L.id) and not is_open;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        for row in results {
            if let Value::I32(x) = row[0] {
                if let Value::I32(y) = row[6] {
                    assert!(x != y);
                    assert!(row[8] == Value::Bool(false));
                } else {
                    panic!("Invalid value type");
                }
            } else {
                panic!("Invalid value type");
            }
        }

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_predicate() {
        let mut user = create_demo_db("invalid_predicates");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        // Unidentified variable
        execute_query(
            &parse(
                "select * from personal_info where age < 32 and x = '30';",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap_err();

        // Parsable but invalid
        let (_, results) = execute_query(
            &parse("select * from personal_info where age < 'Test';", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(results.is_empty());

        // Ambigous column name
        execute_query(
            &parse(
                "select * from personal_info, locations where id < 5;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap_err();

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_offset() {
        let mut user = create_demo_db("offset");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        let (_, results) = execute_query(
            &parse("select * from personal_info offset 2;", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert_eq!(results[0][0], Value::I32(1));
        assert_eq!(results[1][0], Value::I32(2));
        assert_eq!(results[2][0], Value::I32(24));
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_limit() {
        let mut user = create_demo_db("limit");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        //Case 1: Limit is 5
        let (_, results) = execute_query(
            &parse("select * from personal_info limit 5;", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        assert_eq!(results.len(), 5);

        //Case 2: Limit is 2
        let (_, results) = execute_query(
            &parse("select * from personal_info limit 2;", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    #[serial]
    fn test_limit_and_offset() {
        let mut user = create_demo_db("limit_&_offset");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();
        let (_, results) = execute_query(
            &parse("select * from personal_info offset 2 limit 2;", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], Value::I32(24));
        assert_eq!(results[1][0], Value::I32(4));

        let (_, results) = execute_query(
            &parse("select * from personal_info offset 2 limit 3;", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0][0], Value::I32(24));
        assert_eq!(results[1][0], Value::I32(4));

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_subquery1() {
        let mut user = create_demo_db("subquery");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let (_, results) = execute_query(
            &parse("select * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let _res = execute_update(
            &parse(
                "insert into personal_info select * from personal_info;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        );

        let (_, new_results) = execute_query(
            &parse("select * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(new_results.len() == results.len() * 2);

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_subquery2() {
        let mut user = create_demo_db("subquery");
        get_db_instance()
            .unwrap()
            .switch_branch(&"main".to_string(), &mut user)
            .unwrap();

        let (_, results) = execute_query(
            &parse("select * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let (_, condition_results) = execute_query(
            &parse("select * from personal_info where id < 5", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        let _res = execute_update(
            &parse(
                "insert into personal_info select * from personal_info where id < 5;",
                false,
            )
            .unwrap(),
            &mut user,
            &"".to_string(),
        );

        let (_, new_results) = execute_query(
            &parse("select * from personal_info", false).unwrap(),
            &mut user,
            &"".to_string(),
        )
        .unwrap();

        assert!(new_results.len() == results.len() + condition_results.len());

        delete_db_instance().unwrap();
    }
}
