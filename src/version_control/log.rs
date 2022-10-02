use crate::fileio::databaseio::get_db_instance;

use super::branches::{BranchNode, Branches};

pub fn log() -> Result<Vec<Vec<String>>, String> {
    let branch_name: String = get_db_instance()?.get_current_branch_name();
    let branches_from_head: &Branches = get_db_instance()?.get_branch_file();

    // seperate to make debug easier
    let branch_instance = get_db_instance()?.get_branch_heads_file_mut();

    let branch_node = branch_instance
        .get_branch_node_from_head(&branch_name, &branches_from_head)
        .unwrap();

    let mut branch_nodes: Vec<BranchNode> = get_db_instance()?
        .get_branch_file()
        .traverse_branch_nodes(&branch_node)?;

    branch_nodes.reverse();

    // String to capture all the output
    let mut log_strings: Vec<Vec<String>> = Vec::new();

    for node in branch_nodes {
        let commit = get_db_instance()?
            .get_commit_file_mut()
            .fetch_commit(&node.commit_hash)?;
        println!("Commit Hash: {}", commit.hash);
        println!("Commit time stamp: {}", commit.timestamp);
        println!("Commit message: {}", commit.message);
        println!("-----------------------");

        let printed_vals: Vec<String> = vec![
            commit.hash,
            commit.timestamp,
            commit.message,
        ];
        log_strings.push(printed_vals);
    }

    Ok(log_strings)
}

#[cfg(test)]
mod tests {
    use crate::{fileio::{databaseio::{create_db_instance, delete_db_instance}, header::Schema}, version_control::{diff::Diff, commit::Commit}, executor::query::create_table, util::dbtype::*};

    use super::*;

    #[test]
    fn test_log_single_commit() {
        // Keep track of the diffs throughout the test
        let mut diffs: Vec<Diff> = Vec::new();

        // Create the database
        create_db_instance(&"log_test_db".to_string()).unwrap();

        // Create the schema
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        // Create a new table
        let result = create_table(&"table1".to_string(), &schema, get_db_instance().unwrap()).unwrap();
        let mut table = result.0;
        diffs.push(Diff::TableCreate(result.1));

        // Insert rows into the table
        let insert_diff = table.insert_rows(vec![vec![Value::I32(1), Value::String("John".to_string()), Value::I32(20)]]).unwrap();
        diffs.push(Diff::Insert(insert_diff));

        // Commit the changes
        let commit_result = get_db_instance().unwrap().create_commit_and_node(&diffs, &"First commit".to_string(), &"Create table1; Insert 1 Row;".to_string()).unwrap();
        let commit: Commit = commit_result.1;

        // Log the commits
        let result: Vec<Vec<String>> = log().unwrap();

        // Assert that the result is correct
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], commit.hash);
        assert_eq!(result[0][1], commit.timestamp);
        assert_eq!(result[0][2], commit.message);

        // Delete the database
        delete_db_instance().unwrap();
    }
}
