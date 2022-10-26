use crate::{fileio::databaseio::*, user::userdata::User};
use crate::fileio::databaseio::*;
//use crate::fileio::databaseio::Database::get_diffs_between_nodes;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::path;

use super::diff::revert_tables_from_diffs;
use super::{
    branches::{BranchNode, Branches},
    commit::Commit,
};

#[derive(Serialize, Deserialize)]
pub struct Log {
    hash: String,
    timestamp: String,
    message: String,
}

/// This function implements the GQL log command
pub fn log(user: &User) -> Result<(String, Vec<Vec<String>>, String), String> {
    let branch_name: String = user.get_current_branch_name();
    let branches_from_head: &Branches = get_db_instance()?.get_branch_file();

    // seperate to make debug easier
    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();

    // If there are no commits, return an empty vector
    if branch_heads_instance.get_all_branch_heads()?.len() == 0 {
        return Ok(("No Commits!".to_string(), vec![], "".to_string()));
    }

    let branch_node =
        branch_heads_instance.get_branch_node_from_head(&branch_name, &branches_from_head)?;

    let branch_nodes: Vec<BranchNode> = get_db_instance()?
        .get_branch_file()
        .traverse_branch_nodes(&branch_node)?;

    // String to capture all the output
    let mut log_strings: Vec<Vec<String>> = Vec::new();
    let mut log_string: String = String::new();
    let mut log_objects: Vec<Log> = Vec::new();

    for node in branch_nodes {
        let commit = get_db_instance()?
            .get_commit_file_mut()
            .fetch_commit(&node.commit_hash)?;

        let commit_clone = commit.clone();

        log_string = format!("{}\nCommit: {}", log_string, commit.hash);
        log_string = format!("{}\nMessage: {}", log_string, commit.message);
        log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
        log_string = format!("{}\n-----------------------\n", log_string);

        let printed_vals: Vec<String> = vec![commit.hash, commit.timestamp, commit.message];

        let log_object = Log {
            hash: commit_clone.hash,
            timestamp: commit_clone.timestamp,
            message: commit_clone.message,
        };

        log_objects.push(log_object);
        log_strings.push(printed_vals);
    }

    let json = serde_json::to_string(&log_objects).unwrap();

    Ok((log_string, log_strings, json))
}

/// Takes two commit hashes, and attempts to find a chain of commits
/// from the first commit to the second, assuming that the commits are
/// from the same branch.
/// Squashes are only permitted when no other branches use the
/// commits in a squash
pub fn squash(hash1: &String, hash2: &String, user: &User) -> Result<Commit, String> {
    let branch_name: String = user.get_current_branch_name();
    let branches: &mut Branches = get_db_instance()?.get_branch_file_mut();
    let head_mngr = get_db_instance()?.get_branch_heads_file_mut();

    if head_mngr.get_all_branch_heads()?.len() == 0 {
        return Err("No Commits in Current Branch!".to_string());
    }

    // Branch head
    let mut current = Some(head_mngr.get_branch_node_from_head(&branch_name, &branches)?);

    // Hash 1's node
    let mut save_first: Option<BranchNode> = None;
    // Hash 2's node
    let mut save_last: Option<BranchNode> = None;
    let mut commit_hashes: Vec<String> = Vec::new();

    while let Some(node) = current {
        if node.commit_hash == *hash2 {
            save_last = Some(node.clone());
            current = Some(node.clone());
            while current != None {
                let node = current.as_ref().unwrap();
                commit_hashes.push(node.commit_hash.clone());
                if !node.can_squash() {
                    return Err(format!(
                        "Could not squash, commit {} is shared across branches.",
                        node.commit_hash
                    ));
                }
                if node.commit_hash == *hash1 {
                    save_first = Some(node.clone());
                    break;
                }
                current = branches.get_prev_branch_node(&node)?;
            }
        }
        current = branches.get_prev_branch_node(&node)?;
    }

    if commit_hashes.len() == 0 {
        return Err("Commits not found in Current Branch".to_string());
    }

    let save_first = save_first.map_or(Err(format!("{} not found in Branch", hash1)), Ok)?;
    let save_last = save_last.map_or(Err(format!("{} not found in Branch", hash2)), Ok)?;

    let commits = commit_hashes
        .into_iter()
        .map(|hash| get_db_instance()?.get_commit_file_mut().fetch_commit(&hash))
        .rev()
        .collect::<Result<Vec<Commit>, String>>()?;

    let squash_commit = get_db_instance()?
        .get_commit_file_mut()
        .squash_commits(&commits, true)?;

    // Use the new commit hash, and make the current hash2 point to the commit before hash1.
    let squash_node = BranchNode {
        commit_hash: squash_commit.hash.clone(),
        branch_name: branch_name.clone(),
        prev_pagenum: save_first.prev_pagenum,
        prev_rownum: save_first.prev_rownum,
        curr_pagenum: save_last.curr_pagenum,
        curr_rownum: save_last.curr_rownum,
        num_kids: save_last.num_kids,
        is_head: save_last.is_head,
    };
    branches.update_branch_node(&squash_node)?;
    Ok(squash_commit)
}

/// Takes a commit hash, and checks if it exists in the current branch
/// If the commit exists in the user's branch,
/// the branch is reverted to the desired commit.
/// All changes are undone and this revert is saved as another commit
pub fn revert(user: &mut User, commit_hash: &String) -> Result<Commit, String> {
    let branch_name: String = user.get_current_branch_name();
    let branches_from_head: &Branches = get_db_instance()?.get_branch_file();

    // seperate to make debug easier
    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();

    // If there are no commits, return an empty vector
    if branch_heads_instance.get_all_branch_heads()?.len() == 0 {
        return Err("No Commits!".to_string());
    }

    //Grabbing the branch node from the head
    let branch_node =
        branch_heads_instance.get_branch_node_from_head(&branch_name, &branches_from_head)?;

    //Traversing the nodes to find the argument commit hash
    let branch_nodes: Vec<BranchNode> = get_db_instance()?
        .get_branch_file()
        .traverse_branch_nodes(&branch_node)?;

    // If the commit hash is not in the current branch, return an error
    let mut match_counter = 0;
    let mut match_node: BranchNode = BranchNode {
        commit_hash: "".to_string(),
        branch_name: "".to_string(),
        prev_pagenum: 0,
        prev_rownum: 0,
        curr_pagenum: 0,
        curr_rownum: 0,
        num_kids: 0,
        is_head: false,
    };
    //Looking for the commit hash in the branch nodes
    for node in branch_nodes {
        if node.commit_hash == *commit_hash {
            match_counter += 1;
            //Storing the matched commit's information 
            match_node = node;   
        }
    }

    // If the commit hash is not in the current branch, return an error
    if (match_counter == 0) {
        return Err("Commit doesn't exist in the current branch!".to_string());
    }
    else if (match_counter > 1) {
        return Err("Commit exists multiple times in branch! Something is seriously wrong!".to_string());
    }

    // Extracting the diffs between the two nodes
    let diffs = get_db_instance()?.get_diffs_between_nodes(Some(&match_node), &branch_node)?;

    // Obtaining the directory of all tables
    let branch_path: String = get_db_instance()?.get_current_branch_path(user);

    // Reverting the diffs
    revert_tables_from_diffs(&branch_path, &diffs)?;

    // Creating a revert commit
    let revert_message = format!("Reverted to commit {}", commit_hash);
    let revert_command = format!("revert");
    let revert_commit_and_node = get_db_instance()?.create_commit_and_node(&revert_message, &revert_command, user, None)?;
    let revert_commit = revert_commit_and_node.1;

    Ok(revert_commit)
}

/// Takes a user object and clears their branch of uncommitted changes.
/// This is done by moving the user to the permanent copy of the branch and deleting the temporary copy.
/// Returns Success or Error
pub fn discard(user: &mut User) -> Result<(), String> {
    //Storing the user's branch path
    let branch_path: String = get_db_instance()?.get_current_branch_path(user);

    //Setting the user to the permanent copy of the branch
    user.set_is_on_temp_commit(false);

    //Deleting the temp copy of the branch
    fs::remove_dir(branch_path + "-temp");

    //Get the path to where the new branch will be
    let new_branch_path: String = get_db_instance()?.get_branch_path_from_name(&user.get_current_branch_name());

    // Create the branch directory
    std::fs::create_dir_all(&new_branch_path).map_err(|e| {
        "Command::discard() Error: Failed to create directory for given branch path: "
            .to_owned()
           + &e.to_string()
    })?;

    // Copy all the tables from the main branch to the new branch directory
    let mut options = fs_extra::dir::CopyOptions::new();
    options.content_only = true;
    fs_extra::dir::copy(
        &(new_branch_path),
        &(new_branch_path.clone() + "-temp"),
        &options,
    )
    .map_err(|e| "Command::discard() Error: ".to_owned() + &e.to_string())?;
    
    Ok(())
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
        util::{
            bench::{create_demo_db, fcreate_db_instance},
            dbtype::*,
        },
        version_control::{commit::Commit, diff::Diff},
    };

    use super::*;

    #[test]
    #[serial]
    fn test_log_single_commit() {
        // Keep track of the diffs throughout the test
        let mut diffs: Vec<Diff> = Vec::new();

        // Create the database
        fcreate_db_instance(&"log_test_db");

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
        fcreate_db_instance(&"log_test_db1");

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
        assert_eq!(result[1][0], commit.hash);
        assert_eq!(result[1][1], commit.timestamp);
        assert_eq!(result[1][2], commit.message);
        assert_eq!(result[0][0], second_commit.hash);
        assert_eq!(result[0][1], second_commit.timestamp);
        assert_eq!(result[0][2], second_commit.message);

        // Delete the database
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_valid_squash() {
        let user = create_demo_db("squash_valid");
        let hashes = get_db_instance()
            .unwrap()
            .get_commit_file_mut()
            .get_hashes()
            .unwrap();

        // Commits 0 - 3 should be squashable
        let result = squash(&hashes[0], &hashes[2], &user).unwrap();
        // After sqaushing this, all the updates and removes should be gone
        for diff in result.diffs {
            match diff {
                Diff::Update(_) => panic!("Update diff should not exist"),
                Diff::Remove(_) => panic!("Remove diff should not exist"),
                Diff::TableRemove(_) => panic!("TableRemoveDiff should not exist"),
                _ => (),
            }
        }
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_squash_shared() {
        let mut user = create_demo_db("squash_invalid_shared");
        let hashes = get_db_instance()
            .unwrap()
            .get_commit_file_mut()
            .get_hashes()
            .unwrap();
        get_db_instance()
            .unwrap()
            .switch_branch(&"test_branch1".to_string(), &mut user)
            .unwrap();
        // Commits 3 - 5 should not be squasable, since 4 is shared with another branch
        let _ = squash(&hashes[2], &hashes[4], &user).unwrap_err();
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_invalid_squash_branch() {
        let mut user = create_demo_db("squash_invalid_branch");
        let hashes = get_db_instance()
            .unwrap()
            .get_commit_file_mut()
            .get_hashes()
            .unwrap();
        get_db_instance()
            .unwrap()
            .switch_branch(&"test_branch2".to_string(), &mut user)
            .unwrap();
        // Commits 5 - 7 should not be squasable, since user is on another branch
        let _ = squash(&hashes[4], &hashes[6], &user).unwrap_err();
        delete_db_instance().unwrap();
    }
}
