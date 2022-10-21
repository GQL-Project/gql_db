use crate::fileio::header::Schema;
use crate::fileio::tableio::delete_table_in_dir;
use crate::util::{
    dbtype::{Column, Value},
    row::{Row, RowInfo},
};
use crate::version_control::diff::Diff;
use crate::{
    executor::query::create_table,
    fileio::databaseio::{create_db_instance, get_db_instance, Database},
    user::userdata::User,
    util::row::RowLocation,
};

use super::dbtype::parse_time;

/// Creating a big database to run tests on, with enough data and some commits filled in.
pub fn create_demo_db(name: &str) -> User {
    let name = format!("benchmark_db_{name}");
    if let Ok(db) = Database::load_db(name.clone()) {
        db.delete_database().unwrap();
    }
    create_db_instance(&name).unwrap();
    let db = get_db_instance().unwrap();
    let mut user: User = User::new("test_user".to_string());
    let schema1: Schema = vec![
        ("id".to_string(), Column::I32),
        ("first_name".to_string(), Column::String(256)),
        ("last_name".to_string(), Column::String(256)),
        ("age".to_string(), Column::I64),
        (
            "height".to_string(),
            Column::Nullable(Box::new(Column::Float)),
        ),
        ("date_inserted".to_string(), Column::Timestamp),
    ];
    let schema2: Schema = vec![
        ("id".to_string(), Column::I32),
        ("location".to_string(), Column::String(64)),
        ("is_open".to_string(), Column::Bool),
    ];

    let (mut table1, diff) =
        create_table(&"personal_info".to_string(), &schema1, db, &mut user).unwrap();
    let (mut table2, diff2) =
        create_table(&"locations".to_string(), &schema2, db, &mut user).unwrap();
    user.append_diff(&Diff::TableCreate(diff));
    let diff = table1
        .insert_rows(vec![
            create_row1(1, "John", "Doe", 25, 5.5, "2020-01-01 01:00:00"),
            create_row1(2, "Jane", "Doe", 24, 5.2, "2020-01-05 01:12:00"),
            create_row1(3, "Greg", "Smith", 30, -1.0, "2020-01-02 01:00:11"),
            create_row1(4, "Sally", "Smith", 28, 5.6, "2020-01-03 12:00:23"),
            create_row1(5, "Bob", "Jones", 35, 5.9, "2020-01-04 01:00:11"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));

    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 1 on Main Branch".to_string(),
            &"Create Table and Insert Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table1
        .insert_rows(vec![
            create_row1(6, "Alice", "Jones", 32, 5.7, "2020-01-05 01:00:00"),
            create_row1(7, "Joe", "Smith", 30, -1.0, "2020-01-06 00:00:11"),
            create_row1(8, "Stephen", "Strange", 28, 5.6, "2020-01-07 12:00:23"),
            create_row1(9, "Tony", "Stark", 35, 5.9, "2020-01-08 00:00:11"),
            create_row1(10, "Bruce", "Banner", 32, 5.7, "2021-01-03 12:00:23"),
            create_row1(11, "Peter", "Parker", 30, -1.0, "2020-01-01 00:00:11"),
            create_row1(12, "Steve", "Rogers", 28, 5.6, "2020-01-01 12:00:23"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table1
        .rewrite_rows(vec![
            update_row(
                table1.pos_to_loc(2),
                create_row1(3, "Margaret", "Smith", 30, 23.0, "2020-01-02 00:00:11"),
            ),
            update_row(
                table1.pos_to_loc(3),
                create_row1(4, "Sally", "Adams", 118, 5.6, "2021-01-03 12:00:23"),
            ),
            update_row(
                table1.pos_to_loc(7),
                create_row1(8, "Stefano", "Strange", 35, 6.9, "2022-01-04 00:00:11"),
            ),
            update_row(
                table1.pos_to_loc(11),
                create_row1(12, "Captain", "Rogers", 135, 1.9, "2011-01-04 00:00:11"),
            ),
        ])
        .unwrap();
    user.append_diff(&Diff::Update(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 2 on Main Branch".to_string(),
            &"Insert Rows and Update Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table1
        .remove_rows(vec![
            table1.pos_to_loc(2),
            table1.pos_to_loc(4),
            table1.pos_to_loc(10),
            table1.pos_to_loc(11),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    user.append_diff(&Diff::TableCreate(diff2));

    let diff = table2
        .insert_rows(vec![
            create_row2(1, "Home", true),
            create_row2(2, "Work", false),
            create_row2(3, "School", true),
            create_row2(4, "Gym", false),
            create_row2(5, "Store", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 3 on Main Branch".to_string(),
            &"Create Table and Insert Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table2
        .insert_rows(vec![
            create_row2(6, "Restaurant", true),
            create_row2(7, "Bar", false),
            create_row2(8, "Park", true),
            create_row2(9, "Library", false),
            create_row2(10, "Museum", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table2
        .rewrite_rows(vec![
            update_row(table2.pos_to_loc(2), create_row2(3, "University", true)),
            update_row(table2.pos_to_loc(3), create_row2(4, "Gymnasium", true)),
            update_row(table2.pos_to_loc(7), create_row2(8, "Garden", false)),
            update_row(table2.pos_to_loc(9), create_row2(10, "Gallery", false)),
        ])
        .unwrap();
    user.append_diff(&Diff::Update(diff));
    let diff = table2
        .remove_rows(vec![
            table2.pos_to_loc(2),
            table2.pos_to_loc(4),
            table2.pos_to_loc(6),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 4 on Main Branch".to_string(),
            &"Insert Rows and Update Rows and Remove Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    db.create_branch(&"test_branch1".to_string(), &mut user)
        .unwrap();

    let diff = table1
        .insert_rows(vec![
            create_row1(13, "Natasha", "Romanoff", 35, 5.9, "2020-01-02 00:00:11"),
            create_row1(14, "Thor", "Odinson", 32, -1.0, "2020-01-03 00:00:00"),
            create_row1(15, "Wanda", "Maximoff", 30, -1.0, "2020-01-04 00:00:11"),
            create_row1(17, "Scott", "Lang", 35, 5.9, "2020-01-06 00:00:11"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table2
        .insert_rows(vec![
            create_row2(11, "Vacation", true),
            create_row2(12, "Workplace", false),
            create_row2(13, "School Store", true),
            create_row2(14, "Yoga Room", false),
            create_row2(15, "Store", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table1
        .rewrite_rows(vec![
            update_row(
                table1.pos_to_loc(2),
                create_row1(13, "Natalia", "Romanova", 32, 5.7, "2020-01-03 12:00:23"),
            ),
            update_row(
                table1.pos_to_loc(4),
                create_row1(14, "Thor", "Tennyson", 30, 5.6, "2020-01-01 00:00:11"),
            ),
            update_row(
                table1.pos_to_loc(10),
                create_row1(15, "Wanda", "Vision", 28, 5.6, "2020-01-01 12:00:23"),
            ),
            update_row(
                table1.pos_to_loc(11),
                create_row1(17, "Scottish", "Language", 35, 5.9, "2020-01-06 00:00:11"),
            ),
        ])
        .unwrap();
    user.append_diff(&Diff::Update(diff));
    let diff = table1
        .remove_rows(vec![
            table1.pos_to_loc(1),
            table1.pos_to_loc(4),
            table1.pos_to_loc(6),
            table1.pos_to_loc(8),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 5 on Test Branch 1 1".to_string(),
            &"Insert Rows and Update Rows and Remove Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table2
        .rewrite_rows(vec![
            update_row(table2.pos_to_loc(2), create_row2(3, "University", true)),
            update_row(table2.pos_to_loc(3), create_row2(4, "Gymnasium", true)),
            update_row(table2.pos_to_loc(7), create_row2(8, "Garden", false)),
            update_row(table2.pos_to_loc(9), create_row2(10, "Gallery", false)),
        ])
        .unwrap();
    user.append_diff(&Diff::Update(diff));
    let diff = table2
        .remove_rows(vec![
            table2.pos_to_loc(2),
            table2.pos_to_loc(4),
            table2.pos_to_loc(6),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 6 on Test Branch 1".to_string(),
            &"Update Rows and Remove Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table1
        .insert_rows(vec![
            create_row1(18, "Clint", "Barton", 32, 5.7, "2020-01-07 00:00:00"),
            create_row1(19, "Dwayne", "Johnson", 30, 5.8, "2020-01-08 00:00:11"),
            create_row1(20, "Chris", "Hemsworth", 28, -1.0, "2020-01-09 12:00:23"),
            create_row1(21, "Chris", "Evans", 35, 5.9, "2020-01-20 00:00:11"),
            create_row1(22, "Mark", "Ruffalo", 32, 5.7, "2020-01-21 00:00:00"),
            create_row1(23, "Benedict", "Cumberba", 30, 5.8, "2020-01-22 00:00:11"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table2
        .insert_rows(vec![
            create_row2(16, "Gym", true),
            create_row2(17, "Garden", false),
            create_row2(18, "Gallery", false),
            create_row2(19, "Gymnasium", true),
            create_row2(20, "University", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 7 on Test Branch 1".to_string(),
            &"Insert Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table1
        .remove_rows(vec![
            table1.pos_to_loc(1),
            table1.pos_to_loc(2),
            table1.pos_to_loc(12),
            table1.pos_to_loc(13),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    let diff = table2
        .remove_rows(vec![
            table2.pos_to_loc(1),
            table2.pos_to_loc(2),
            table2.pos_to_loc(3),
            table2.pos_to_loc(4),
        ])
        .unwrap();
    user.append_diff(&Diff::Remove(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 8 on Test Branch 1".to_string(),
            &"Remove Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    db.switch_branch(&"main".to_string(), &mut user).unwrap();
    let diff = table1
        .insert_rows(vec![
            create_row1(24, "Tom", "Holland", 28, -1.0, "2020-01-23 12:00:23"),
            create_row1(25, "Elizabeth", "Olsen", 35, 5.9, "2020-01-24 00:00:11"),
            create_row1(26, "Scarlett", "Johansson", 32, 5.7, "2020-01-25 00:00:00"),
            create_row1(27, "Chadwick", "Boseman", 30, 5.8, "2020-01-26 00:00:11"),
            create_row1(28, "Tom", "Hiddleston", 28, -1.0, "2020-01-27 12:00:23"),
            create_row1(29, "Paul", "Rudd", 35, 5.9, "2020-01-28 00:00:11"),
            create_row1(30, "Jeremy", "Renner", 32, 5.7, "2020-01-29 00:00:00"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table2
        .insert_rows(vec![
            create_row2(21, "Gym", true),
            create_row2(22, "Garden", false),
            create_row2(23, "Gallery", false),
            create_row2(24, "Gymnasium", true),
            create_row2(25, "University", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 9 on Main Branch".to_string(),
            &"Insert Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    let diff = table1
        .insert_rows(vec![
            create_row1(31, "Bruce", "Wayne", 30, 5.8, "2020-01-30 00:00:11"),
            create_row1(32, "Clark", "Kent", 28, -1.0, "2020-01-31 12:00:23"),
            create_row1(33, "Diana", "Prince", 35, 5.9, "2020-02-01 00:00:11"),
            create_row1(34, "Barry", "Allen", 32, 5.7, "2020-02-02 00:00:00"),
            create_row1(35, "Arthur", "Curry", 30, 5.8, "2020-02-03 00:00:11"),
            create_row1(36, "Hal", "Jordan", 28, -1.0, "2020-02-04 12:00:23"),
            create_row1(37, "Oliver", "Queen", 35, 5.9, "2020-02-05 00:00:11"),
            create_row1(38, "Victor", "Stone", 32, 5.7, "2020-02-06 00:00:00"),
            create_row1(39, "Kara", "Zor-El", 30, -1.0, "2020-02-07 00:00:11"),
            create_row1(40, "Barry", "Allen", 28, 5.6, "2020-02-08 12:00:23"),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));
    let diff = table2
        .insert_rows(vec![
            create_row2(27, "Dubai", false),
            create_row2(28, "London", true),
            create_row2(29, "New York", true),
            create_row2(30, "Paris", false),
            create_row2(31, "Tokyo", true),
        ])
        .unwrap();
    user.append_diff(&Diff::Insert(diff));

    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 10 on Main Branch".to_string(),
            &"Insert Rows".to_string(),
            &mut user,
            None,
        )
        .unwrap();

    db.create_branch(&"test_branch2".to_string(), &mut user)
        .unwrap();

    let diff =
        delete_table_in_dir(&table2.name, &db.get_current_working_branch_path(&user)).unwrap();
    user.append_diff(&Diff::TableRemove(diff));
    let _ = get_db_instance()
        .unwrap()
        .create_commit_and_node(
            &"Create Commit 11 on Test Branch 2".to_string(),
            &"Delete Table".to_string(),
            &mut user,
            None,
        )
        .unwrap();
    user
}

fn create_row1(id: i32, fname: &str, lname: &str, age: i64, height: f32, date: &str) -> Row {
    let mut row = Row::new();
    row.push(Value::I32(id));
    row.push(Value::String(fname.to_string()));
    row.push(Value::String(lname.to_string()));
    row.push(Value::I64(age));
    row.push(if height < 0.0 {
        Value::Null
    } else {
        Value::Float(height)
    });
    row.push(Value::Timestamp(parse_time(&date.to_string()).unwrap()));
    row
}

fn update_row(loc: RowLocation, row: Row) -> RowInfo {
    RowInfo {
        row,
        pagenum: loc.pagenum,
        rownum: loc.rownum,
    }
}

fn create_row2(id: i32, location: &str, is_open: bool) -> Row {
    let mut row = Row::new();
    row.push(Value::I32(id));
    row.push(Value::String(location.to_string()));
    row.push(Value::Bool(is_open));
    row
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::fileio::databaseio::delete_db_instance;

    use super::create_demo_db;

    #[test]
    #[serial]
    fn test_bench() {
        create_demo_db("test_bench");
        delete_db_instance().unwrap();
    }
}
