use crate::{fileio::databaseio::get_db_instance, user::userdata::User, util::row::RowLocation};

use super::{
    branch_heads::{self, BranchHead},
    branches::{BranchNode, Branches},
    commit::Commit,
};

/// This function implements the GQL log command
pub fn log(user: &User) -> Result<(String, Vec<Vec<String>>), String> {
    let branch_name: String = user.get_current_branch_name();
    let branches_from_head: &Branches = get_db_instance()?.get_branch_file();

    // seperate to make debug easier
    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();

    // If there are no commits, return an empty vector
    if branch_heads_instance.get_all_branch_heads()?.len() == 0 {
        return Ok(("No Commits!".to_string(), vec![]));
    }

    let branch_node =
        branch_heads_instance.get_branch_node_from_head(&branch_name, &branches_from_head)?;

    let mut branch_nodes: Vec<BranchNode> = get_db_instance()?
        .get_branch_file()
        .traverse_branch_nodes(&branch_node)?;

    branch_nodes.reverse();

    // String to capture all the output
    let mut log_strings: Vec<Vec<String>> = Vec::new();
    let mut log_string: String = String::new();

    for node in branch_nodes {
        let commit = get_db_instance()?
            .get_commit_file_mut()
            .fetch_commit(&node.commit_hash)?;

        log_string = format!("{}\nCommit: {}", log_string, commit.hash);
        log_string = format!("{}\nMessage: {}", log_string, commit.message);
        log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
        log_string = format!("{}\n-----------------------\n", log_string);

        let printed_vals: Vec<String> = vec![commit.hash, commit.timestamp, commit.message];
        log_strings.push(printed_vals);
    }

    Ok((log_string, log_strings))
}

/// Takes two commit hashes, and attempts to find a chain of commits
/// from the first commit to the second, assuming that the commits are
/// from the same branch.
pub fn squash(user: &User, hash1: String, hash2: String) -> Result<Commit, String> {
    let branch_name: String = user.get_current_branch_name();
    let branches: &mut Branches = get_db_instance()?.get_branch_file_mut();
    let head_mngr = get_db_instance()?.get_branch_heads_file_mut();

    if head_mngr.get_all_branch_heads()?.len() == 0 {
        return Err("No Commits in Current Branch!".to_string());
    }

    // Branch head
    let mut prev: Option<BranchNode> = None;
    let mut current = Some(head_mngr.get_branch_node_from_head(&branch_name, &branches)?);

    // Node ahead of Hash2, if None then Hash2 is the head
    let mut save_next: Option<BranchNode> = None;
    // Hash 1's node
    let mut save_first: Option<BranchNode> = None;
    let mut commit_hashes: Vec<String> = Vec::new();

    while let Some(node) = current {
        if node.commit_hash == hash2 {
            save_next = prev.clone();
            current = Some(node.clone());
            while current != None {
                let node = current.as_ref().unwrap();
                commit_hashes.push(node.commit_hash.clone());
                if node.commit_hash == hash1 {
                    save_first = Some(node.clone());
                    break;
                }
                current = branches.get_prev_branch_node(&node)?;
            }
        }
        prev = Some(node.clone());
        current = branches.get_prev_branch_node(&node)?;
    }

    if commit_hashes.len() == 0 {
        return Err("Commits not found in Current Branch".to_string());
    }

    let save_first = save_first.map_or(Err(format!("{} not found in Branch", hash1)), Ok)?;

    let commits = commit_hashes
        .into_iter()
        .map(|hash| get_db_instance()?.get_commit_file_mut().fetch_commit(&hash))
        .collect::<Result<Vec<Commit>, String>>()?;

    let squash_commit = get_db_instance()?
        .get_commit_file_mut()
        .combine_commits(&commits)?;

    // Create new_squash node, and make new_squash commit point to the hash1's previous commit
    let squash_node = branches.create_branch_node(
        head_mngr,
        branches.get_prev_branch_node(&save_first)?.as_ref(),
        &branch_name,
        &squash_commit.hash,
    )?;

    // Make save_next point to the new squash commit
    if let Some(mut node) = save_next {
        node.prev_rownum = squash_node.curr_rownum;
        node.prev_pagenum = squash_node.curr_pagenum;
        branches.update_branch_node(head_mngr, &node)?;
    } else {
        // If it doesn't exist, then the squash commit is the head
        head_mngr.set_branch_head(
            &branch_name,
            &RowLocation {
                pagenum: squash_node.curr_pagenum as u32,
                rownum: squash_node.curr_rownum as u16,
            },
        )?;
    }
    Ok(squash_commit)
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        executor::query::create_table,
        fileio::{
            databaseio::{create_db_instance, delete_db_instance},
            header::Schema,
        },
        util::dbtype::*,
        version_control::{commit::Commit, diff::Diff},
    };

    use super::*;

    #[test]
    #[serial]
    fn test_log_single_commit() {
        // Keep track of the diffs throughout the test
        let mut diffs: Vec<Diff> = Vec::new();

        // Create the database
        create_db_instance(&"log_test_db".to_string()).unwrap();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create the schema
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        // Create a new table
        let result = create_table(
            &"table1".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        let mut table = result.0;
        diffs.push(Diff::TableCreate(result.1));

        // Insert rows into the table
        let insert_diff = table
            .insert_rows(vec![vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(20),
            ]])
            .unwrap();
        diffs.push(Diff::Insert(insert_diff));

        user.set_diffs(&diffs);

        // Commit the changes
        let commit_result = get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First commit".to_string(),
                &"Create table1; Insert 1 Row;".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        let commit: Commit = commit_result.1;

        // Log the commits
        let result: Vec<Vec<String>> = log(&user).unwrap().1;

        // Assert that the result is correct
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], commit.hash);
        assert_eq!(result[0][1], commit.timestamp);
        assert_eq!(result[0][2], commit.message);

        // Delete the database
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_log_multiple_command() {
        // Keep track of the diffs throughout the test
        let mut diffs: Vec<Diff> = Vec::new();

        // Create the database
        create_db_instance(&"log_test_db1".to_string()).unwrap();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create the schema
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        // Create a new table
        let result = create_table(
            &"table1".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        let mut table = result.0;
        diffs.push(Diff::TableCreate(result.1));

        // Insert rows into the table
        let mut insert_diff = table
            .insert_rows(vec![vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(20),
            ]])
            .unwrap();
        diffs.push(Diff::Insert(insert_diff));

        insert_diff = table
            .insert_rows(vec![vec![
                Value::I32(2),
                Value::String("Saul Goodman".to_string()),
                Value::I32(42),
            ]])
            .unwrap();
        diffs.push(Diff::Insert(insert_diff));

        user.set_diffs(&diffs);

        // Commit the changes
        let mut commit_result = get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First commit".to_string(),
                &"Create table1; Insert 1 Row;".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        let commit: Commit = commit_result.1;

        commit_result = get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"Second commit".to_string(),
                &"Create table2; Insert 2 Row;".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        let second_commit: Commit = commit_result.1;

        // Log the commits
        let result: Vec<Vec<String>> = log(&user).unwrap().1;

        // Assert that the result is correct
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], commit.hash);
        assert_eq!(result[0][1], commit.timestamp);
        assert_eq!(result[0][2], commit.message);
        assert_eq!(result[1][0], second_commit.hash);
        assert_eq!(result[1][1], second_commit.timestamp);
        assert_eq!(result[1][2], second_commit.message);

        // Delete the database
        delete_db_instance().unwrap();
    }
}
