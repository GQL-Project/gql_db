use std::cmp::Ordering;
use std::collections::HashMap;

use crate::util::dbtype::Column;
use crate::util::dbtype::Value;
use crate::util::row::Row;
use prost_types::Timestamp;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use super::query::resolve_reference;

/// Basically, these are pointers to functions that can take a row and return a bool
/// We could also encounter an invalid operation, like 1 + 'a' or 'a' + 'b'
/// They have to be done this way, as the actual function itself is not known until runtime
/// The SolvePredicate Function Type takes a row and returns a bool, used to filter rows
pub type SolvePredicate = Box<dyn Fn(&Row) -> Result<bool, String>>;
/// The SolveValue Function Type takes a row and returns a Value, which is used by SolvePredicate
pub type SolveValue = Box<dyn Fn(&Row) -> Result<JointValues, String>>;

// We could encounter cases with two different types of values, so we need to be able to handle both
#[derive(Debug)]
pub enum JointValues {
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

pub fn resolve_value(solver: &SolveValue, row: &Row) -> Result<Value, String> {
    match solver(row)? {
        JointValues::DBValue(v) => Ok(v),
        JointValues::SQLValue(v) => Value::from_sql_value(&v),
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

pub fn solve_value(
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
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.add(&right)
                }))
            }
            BinaryOperator::Minus => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.subtract(&right)
                }))
            }
            BinaryOperator::Multiply => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.multiply(&right)
                }))
            }
            BinaryOperator::Divide => {
                let left = solve_value(left, column_names, index_refs)?;
                let right = solve_value(right, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let left = left(row)?;
                    let right = right(row)?;
                    left.divide(&right)
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
                let binary = solve_predicate(pred, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let pred = binary(row)?;
                    Ok(JointValues::DBValue(Value::Bool(pred)))
                }))
            }
            _ => Err(format!("Invalid Binary Operator for Value: {}", op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => {
                let expr = solve_value(expr, column_names, index_refs)?;
                Ok(Box::new(move |row| expr(row)))
            }
            UnaryOperator::Minus => {
                let expr = solve_value(expr, column_names, index_refs)?;
                Ok(Box::new(move |row| {
                    let val = expr(row)?;
                    JointValues::DBValue(Value::I32(0)).subtract(&val)
                }))
            }
            _ => Err(format!("Invalid Unary Operator for Value: {}", op)),
        },
        _ => Err(format!("Unexpected Value Clause: {}", pred)),
    }
}

type ApplyInt = fn(i64, i64) -> Result<i64, String>;
type ApplyFloat = fn(f64, f64) -> Result<f64, String>;
type ApplyString = fn(&String, &String) -> Result<String, String>;

impl JointValues {
    fn add(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x + y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x + y);
        let apply_string = |x: &String, y: &String| Ok::<String, String>(x.to_string() + y);
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot add {:?} and {:?}", self, other))
    }

    fn subtract(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x - y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x - y);
        let apply_string = |x: &String, y: &String| Ok::<String, String>(x.replace(y, ""));
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot subtract {:?} and {:?}", self, other))
    }

    fn multiply(&self, other: &Self) -> Result<JointValues, String> {
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

    fn divide(&self, other: &Self) -> Result<JointValues, String> {
        let apply_int = |x: i64, y: i64| Ok::<i64, String>(x / y);
        let apply_float = |x: f64, y: f64| Ok::<f64, String>(x / y);
        let apply_string = |_: &String, _: &String| {
            Err::<String, String>("Cannot divide string by string".to_string())
        };
        self.apply(other, apply_int, apply_float, apply_string)
            .map_err(|_| format!("Cannot divide {:?} and {:?}", self, other))
    }

    fn apply(
        &self,
        other: &Self,
        int_func: ApplyInt,
        float_func: ApplyFloat,
        string_func: ApplyString,
    ) -> Result<JointValues, String> {
        Ok(match (self, other) {
            (Self::DBValue(l0), Self::DBValue(r0)) => match (l0, r0) {
                (Value::I32(l), Value::I32(r)) => {
                    JointValues::DBValue(Value::I32(int_func(*l as i64, *r as i64)? as i32))
                }
                (Value::Float(l), Value::Float(r)) => {
                    JointValues::DBValue(Value::Float(float_func(*l as f64, *r as f64)? as f32))
                }
                (Value::I64(l), Value::I64(r)) => {
                    JointValues::DBValue(Value::I64(int_func(*l, *r)?))
                }
                (Value::Double(l), Value::Double(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l, *r)?))
                }

                (Value::I32(l), Value::Float(r)) => {
                    JointValues::DBValue(Value::Float(float_func(*l as f64, *r as f64)? as f32))
                }
                (Value::I32(l), Value::I64(r)) => {
                    JointValues::DBValue(Value::I64(int_func(*l as i64, *r)?))
                }
                (Value::I32(l), Value::Double(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l as f64, *r)?))
                }

                (Value::Float(l), Value::I32(r)) => {
                    JointValues::DBValue(Value::Float(float_func(*l as f64, *r as f64)? as f32))
                }
                (Value::Float(l), Value::I64(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l as f64, *r as f64)?))
                }
                (Value::Float(l), Value::Double(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l as f64, *r as f64)?))
                }

                (Value::I64(l), Value::I32(r)) => {
                    JointValues::DBValue(Value::I64(int_func(*l, *r as i64)?))
                }
                (Value::I64(l), Value::Float(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l as f64, *r as f64)?))
                }
                (Value::I64(l), Value::Double(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l as f64, *r as f64)?))
                }

                (Value::Double(l), Value::I32(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l, *r as f64)?))
                }
                (Value::Double(l), Value::I64(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l, *r as f64)?))
                }
                (Value::Double(l), Value::Float(r)) => {
                    JointValues::DBValue(Value::Double(float_func(*l, *r as f64)?))
                }

                (Value::Timestamp(l), Value::Timestamp(r)) => {
                    Self::DBValue(Value::Timestamp(Timestamp {
                        seconds: int_func(l.seconds, r.seconds)?,
                        nanos: int_func(l.nanos as i64, r.nanos as i64)? as i32,
                    }))
                }
                (Value::Timestamp(l), Value::I32(r)) => {
                    Self::DBValue(Value::Timestamp(Timestamp {
                        seconds: int_func(l.seconds, *r as i64)?,
                        nanos: l.nanos,
                    }))
                }
                (Value::Timestamp(l), Value::I64(r)) => {
                    Self::DBValue(Value::Timestamp(Timestamp {
                        seconds: int_func(l.seconds, *r)?,
                        nanos: l.nanos,
                    }))
                }

                (Value::String(l), Value::String(r)) => {
                    Self::DBValue(Value::String(string_func(l, r)?))
                }
                (Value::String(l), Value::I32(r)) => {
                    Self::DBValue(Value::String(string_func(l, &r.to_string())?))
                }
                (Value::String(l), Value::I64(r)) => {
                    Self::DBValue(Value::String(string_func(l, &r.to_string())?))
                }
                (Value::String(l), Value::Float(r)) => {
                    Self::DBValue(Value::String(string_func(l, &r.to_string())?))
                }
                (Value::String(l), Value::Double(r)) => {
                    Self::DBValue(Value::String(string_func(l, &r.to_string())?))
                }
                _ => Err(format!("Cannot apply {:?} and {:?} together", l0, r0))?,
            },
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
        match (self, other) {
            (Self::DBValue(l0), Self::DBValue(r0)) => l0.partial_cmp(r0) == Some(Ordering::Equal),
            (Self::SQLValue(l0), Self::SQLValue(r0)) => {
                let l = Value::from_sql_value(l0);
                let r = Value::from_sql_value(r0);
                if l.is_err() || r.is_err() {
                    false
                } else {
                    l.unwrap().partial_cmp(&r.unwrap()) == Some(Ordering::Equal)
                }
            }
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

// Where (predicate) tests go here
#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        executor::query::execute_query,
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
}
