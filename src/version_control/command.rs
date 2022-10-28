use crate::{
    fileio::databaseio::{get_db_instance, MAIN_BRANCH_NAME},
    user::userdata::User,
};
use serde::{Deserialize, Serialize};
use serde_json;
use tonic::transport::Server;

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

/// This function deletes a branch from the database
pub fn del_branch(
    user: &User,
    branch_name: &String,
    flag: bool,
    all_users: Vec<User>,
) -> Result<String, String> {
    // Check if the branch is the master branch. If so, return an error
    // MAIN_BRANCH_NAME is the name of the master branch
    if branch_name == MAIN_BRANCH_NAME {
        return Err("ERROR: Cannot delete the master branch".to_string());
    }

    // Check if the branch is the current branch. If so, return an error
    // user.get_current_branch() is the name
    if user.get_current_branch_name() == *branch_name {
        return Err("ERROR: Cannot delete the current branch".to_string());
    }

    // checks if there are uncommited changes, if not, delete no matter what
    if !flag {
        // Check if branch has uncommitted changes.
        for client in all_users {
            if client.get_current_branch_name() == *branch_name {
                if client.is_on_temp_commit() {
                    return Err(
                        "ERROR: Branch has uncommitted changes. Use -f to force delete."
                            .to_string(),
                    );
                }
            }
        }
    }
    // delete branch head
    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();
    branch_heads_instance.delete_branch_head(branch_name)?;

    // delete all the rows where branch name = the branch head
    let branches_instance = get_db_instance()?.get_branch_file_mut();
    branches_instance.delete_branch_node(branch_name)?;

    let result_string = format!("Branch {} deleted", &branch_name);
    Ok(result_string.to_string())
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

/// This function is used to get the commits from a specific hash
pub fn info(hash: &String) -> Result<String, String> {
    let commit_file = get_db_instance()?.get_commit_file_mut();
    let commit = commit_file.fetch_commit(hash)?;

    let mut log_string: String = String::new();

    log_string = format!("{}\n-----------------------", log_string);
    log_string = format!("{}\nCommit: {}", log_string, commit.hash);
    log_string = format!("{}\nMessage: {}", log_string, commit.message);
    log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
    log_string = format!("{}\n-----------------------\n", log_string);

    return Ok(log_string.to_string());
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{
        executor::query::{create_table, execute_query},
        fileio::{
            databaseio::{delete_db_instance, Database},
            header::Schema,
        },
        parser::parser::{parse, parse_vc_cmd},
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

    // test if the branch deletes properly
    #[test]
    #[serial]
    fn test_del_branch0() {
        let query0 = "GQL branch branch_name";
        let query01 = "GQL branch branch_name1";
        let query2 = "GQL del branch_name";
        // Create a new user on the main branch
        fcreate_db_instance("gql_del_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();
        parse_vc_cmd(query01, &mut user, all_users.clone()).unwrap();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query2, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_ok());
    }

    // checks if it detects non existent branch
    #[test]
    #[serial]
    fn test_del_branch1() {
        let query = "GQL del branch_name1";
        // Create a new user on the main branch
        fcreate_db_instance("gql_del_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    // Tries to delete the current branch
    #[test]
    #[serial]
    fn test_del_branch2() {
        let query0 = "GQL branch branch_name";
        let query1 = "GQL del branch_name";
        // Create a new user on the main branch
        fcreate_db_instance("gql_del_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query1, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    // Tries to delete the branch with an uncommitted change
    #[test]
    #[serial]
    fn test_del_branch3() {
        let query0 = "GQL branch test";
        let query1 = "GQL switch_branch test";
        let query2 = "GQL branch test1";
        let query3 = "GQL del test";
        // Create a new user on the main branch
        fcreate_db_instance("gql_del_test");
        let mut user: User = User::new("test_user".to_string());
        let mut user1: User = User::new("test_user1".to_string());
        let mut all_users: Vec<User> = Vec::new();

        // first user creates a branch
        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();

        // second user joins that branch
        parse_vc_cmd(query1, &mut user1, all_users.clone()).unwrap();

        // second user makes an uncommitted change
        let load_db = Database::load_db("gql_del_test".to_string()).unwrap();
        create_table(
            &"testing".to_string(),
            &vec![("id".to_string(), Column::I32)],
            &load_db,
            &mut user1,
        )
        .unwrap();
        user1.set_is_on_temp_commit(true);

        // first user makes a new branch and moves there
        parse_vc_cmd(query2, &mut user, all_users.clone()).unwrap();

        all_users.push(user.clone());
        all_users.push(user1.clone());
        // first user tries to delete the branch with the uncommitted change
        parse_vc_cmd("GQL status", &mut user1, all_users.clone()).unwrap();
        let result = parse_vc_cmd(query3, &mut user, all_users.clone());

        // should not be able to delete
        assert!(result.is_err());
        delete_db_instance().unwrap();
        // new_db.delete_database().unwrap();
    }

    // Tries to delete the branch with an uncommitted change with -f
    #[test]
    #[serial]
    fn test_del_branch4() {
        let query0 = "GQL branch test";
        let query1 = "GQL switch_branch test";
        let query2 = "GQL branch test1";
        let query3 = "GQL del -f test";
        // Create a new user on the main branch
        fcreate_db_instance("gql_del_test");
        let mut user: User = User::new("test_user".to_string());
        let mut user1: User = User::new("test_user1".to_string());
        let mut all_users: Vec<User> = Vec::new();

        // first user creates a branch
        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();

        // second user joins that branch
        parse_vc_cmd(query1, &mut user1, all_users.clone()).unwrap();

        // second user makes an uncommitted change
        let load_db = Database::load_db("gql_del_test".to_string()).unwrap();
        create_table(
            &"testing".to_string(),
            &vec![("id".to_string(), Column::I32)],
            &load_db,
            &mut user1,
        )
        .unwrap();
        user1.set_is_on_temp_commit(true);

        // first user makes a new branch and moves there
        parse_vc_cmd(query2, &mut user, all_users.clone()).unwrap();

        all_users.push(user.clone());
        all_users.push(user1.clone());
        // first user tries to delete the branch with the uncommitted change
        parse_vc_cmd("GQL status", &mut user1, all_users.clone()).unwrap();
        let result = parse_vc_cmd(query3, &mut user, all_users.clone());

        // should not be able to delete
        assert!(result.is_ok());
        delete_db_instance().unwrap();
        // new_db.delete_database().unwrap();
    }

    // Tries to get the info without the commit hash
    #[test]
    #[serial]
    fn test_info_commit() {
        let query = "GQL info";
        // Create a new user on the main branch
        fcreate_db_instance("gql_info_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    //Tries to get the info with an invalid commit hash
    #[test]
    #[serial]
    fn test_info_commit1() {
        let query = "GQL info 123456789";
        // Create a new user on the main branch
        fcreate_db_instance("gql_info_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    // Tries to get the info with a valid commit hash
    #[test]
    #[serial]
    fn test_info_commit2() {
        let query0 = "GQL commit -m test";
        // Create a new user on the main branch
        fcreate_db_instance("gql_info_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());

        let mut load_db = Database::load_db("gql_info_test".to_string()).unwrap();
        create_table(
            &"testing".to_string(),
            &vec![("id".to_string(), Column::I32)],
            &load_db,
            &mut user,
        )
        .unwrap();

        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();

        let commit_file = load_db.get_commit_file_mut();
        let commit = commit_file.read_commit(1);
        let query1 = format!("GQL info {}", commit.unwrap().hash);

        let result = parse_vc_cmd(&query1, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_ok());
    }
}
