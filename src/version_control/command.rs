use crate::fileio::pageio::PageType;
use crate::{fileio::databaseio::*, user::userdata::User};

use crate::{
    fileio::{
        header::read_schema,
        pageio::{read_page, Page},
    },
    util::dbtype::Column,
};

use serde::{Deserialize, Serialize};
use serde_json;
use tabled::{builder::Builder, Style};

use std::fs;

use super::diff::{reverse_diffs, revert_tables_from_diffs};
use super::{
    branches::{BranchNode, Branches},
    commit::Commit,
};

#[derive(Serialize, Deserialize)]
pub struct Log {
    user_id: String,
    hash: String,
    timestamp: String,
    message: String,
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
    table_name: String,
    table_schema: Vec<String>,
    schema_type: Vec<String>,
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

        log_string = format!("{}\n---------- {}'s Commit ----------", log_string, commit.user_id);
        log_string = format!("{}\nCommit: {}", log_string, commit.hash);
        log_string = format!("{}\nMessage: {}", log_string, commit.message);
        log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
        log_string = format!("{}\n", log_string);

        let printed_vals: Vec<String> = vec![commit.user_id, commit.hash, commit.timestamp, commit.message];

        let log_object = Log {
            user_id: commit_clone.user_id,
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

/// Lists all the available branches, and a * next to the current branch
/// for the given user.
pub fn list_branches(user: &User) -> Result<String, String> {
    let branch_heads = get_db_instance()?.get_branch_heads_file_mut();

    let mut branch_string = String::new();

    for name in branch_heads.get_all_branch_names()? {
        if name == user.get_current_branch_name() {
            branch_string = format!("{}{}*\n", branch_string, name);
        } else {
            branch_string = format!("{}{}\n", branch_string, name);
        }
    }

    Ok(branch_string)
}

/// This function deletes a branch from the database
pub fn del_branch(
    user: &User,
    branch_name: &String,
    force: bool,
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
    if !force {
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
    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();
    let branches_instance = get_db_instance()?.get_branch_file_mut();

    // Find the node that this branch branched off of
    let mut temp_node: BranchNode =
        branch_heads_instance.get_branch_node_from_head(branch_name, branches_instance)?;
    loop {
        let temp_node_opt: Option<BranchNode> =
            branches_instance.get_prev_branch_node(&temp_node)?;
        if temp_node_opt.is_some() {
            temp_node = temp_node_opt.unwrap();
            if temp_node.branch_name != *branch_name {
                // We need to update the num kids of the node that this branch branched off of
                temp_node.num_kids -= 1;
                branches_instance.update_branch_node(&temp_node)?;
                break;
            }
        } else {
            break;
        }
    }

    // delete branch head
    branch_heads_instance.delete_branch_head(branch_name)?;

    // delete all the rows where branch name = the branch head
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
        .squash_commits(user.get_user_id(), &commits, true)?;

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

    // Checking the branch's status to ensure if the user is up-to-date
    let behind_check = user.get_status();
    if behind_check.1 {
        return Err(
            "ERR: Cannot revert when behind! Consider using Discard to delete your changes."
                .to_string(),
        );
    }

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
    let mut match_node = None;
    //Looking for the commit hash in the branch nodes
    for node in branch_nodes {
        if node.commit_hash == *commit_hash {
            if match_node.is_some() {
                return Err(
                    "Commit exists multiple times in branch! Something is seriously wrong!"
                        .to_string(),
                );
            }
            //Storing the matched commit's information
            match_node = Some(node);
        }
    }

    // If the commit hash is not in the current branch, return an error
    if let Some(node) = match_node {
        let diffs = get_db_instance()?.get_diffs_between_nodes(Some(&node), &branch_node)?;

        // Obtaining the directory of all tables
        let branch_path: String = get_db_instance()?.get_current_branch_path(user);

        // Reverting the diffs
        revert_tables_from_diffs(&branch_path, &diffs)?;

        let reversed_diffs = reverse_diffs(&diffs)?;
        for curr_diff in reversed_diffs {
            user.append_diff(&curr_diff);
        }
        // Creating a revert commit
        let revert_message = format!("Reverted to commit {}", commit_hash);
        let revert_command = format!("gql revert {}", commit_hash);
        let revert_commit_and_node = get_db_instance()?.create_commit_on_head(
            &revert_message,
            &revert_command,
            user,
            None,
        )?;
        let revert_commit = revert_commit_and_node.1;

        Ok(revert_commit)
    } else {
        Err("Commit not found in current branch!".to_string())
    }
}

/// Takes a user object and clears their branch of uncommitted changes.
/// This is done by moving the user to the permanent copy of the branch and deleting the temporary copy.
/// Returns Success or Error
pub fn discard(user: &mut User) -> Result<(), String> {
    //Storing the user's branch path
    let branch_path: String = get_db_instance()?.get_temp_db_dir_path(user);

    if user.is_on_temp_commit() {
        //Deleting the temp copy of the branch
        fs::remove_dir_all(branch_path).map_err(|e| e.to_string())?;
        user.set_diffs(&Vec::new());
    }

    user.set_is_on_temp_commit(false);
    user.set_commands(&Vec::new());

    Ok(())
}

/// This function is used to get the commits from a specific hash
pub fn info(hash: &String) -> Result<String, String> {
    let commit_file = get_db_instance()?.get_commit_file_mut();
    let commit = commit_file.fetch_commit(hash)?;

    let mut log_string: String = String::new();

    log_string = format!("{}\n---------- {}'s Commit ----------", log_string, commit.user_id);
    log_string = format!("{}\nCommit: {}", log_string, commit.hash);
    log_string = format!("{}\nMessage: {}", log_string, commit.message);
    log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
    log_string = format!("{}\nChanges Made:", log_string);
    for diffs in commit.diffs {
        log_string = format!("{}\n{}", log_string, diffs.to_string());
    }
    log_string = format!("{}\n----------------------------------------\n", log_string);

    return Ok(log_string.to_string());
}

/// This function outputs all of the possible tables and Schemas in the current branch
pub fn schema_table(user: &User) -> Result<(String, String), String> {
    // Get the list of all the tables in the database
    let instance = get_db_instance()?;
    let all_table_paths = instance.get_table_paths(user);

    let mut page_read: Vec<Box<Page>> = Vec::new();
    for path in all_table_paths.clone().unwrap() {
        let (page, page_type) = read_page(0, &path)?;
        if page_type == PageType::Header {
            page_read.push(page);
        } else {
            return Err(format!("Page 0 in {} is not a header page", path));
        }
    }

    if page_read.clone().len() == 0 {
        return Ok(("No tables in current branch!".to_string(), "".to_string()));
    }

    // Call read_schema for each table
    let mut schemas: Vec<Vec<String>> = Vec::new();
    let mut schema_types: Vec<Vec<Column>> = Vec::new();
    let mut schema_objects: Vec<Schema> = Vec::new();
    for page_num in page_read.clone() {
        let schema_object = read_schema(&page_num)?;
        schemas.push(
            schema_object
                .iter()
                .map(|(name, _typ)| name.clone())
                .collect::<Vec<String>>()
                .clone(),
        );
        schema_types.push(
            schema_object
                .iter()
                .map(|(_name, typ)| typ.clone())
                .collect::<Vec<Column>>()
                .clone(),
        );
    }

    let table_names = instance.get_tables(user);
    let mut log_string: String = String::new();

    for i in 0..schemas.len() {
        log_string = format!(
            "{}\nTable: {}\n",
            log_string,
            table_names.clone().unwrap()[i]
        );
        let mut builder = Builder::default();
        builder.set_columns(schemas[i].clone());

        let schemaz = schema_types[i]
            .clone()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        builder.add_record(schemaz.clone());

        let mut table_schema = builder.build();
        table_schema.with(Style::rounded());
        log_string = format!("{}\n{}\n\n", log_string, table_schema);

        // for the -j
        let schema_object = Schema {
            table_name: table_names.clone().unwrap()[i].clone(),
            table_schema: schemas[i].clone(),
            schema_type: schemaz.clone(),
        };
        schema_objects.push(schema_object);
    }

    let json = serde_json::to_string(&schema_objects).unwrap();
    Ok((log_string, json))
}

#[cfg(test)]
mod tests {

    use serial_test::serial;

    use crate::{
        executor::query::{create_table, insert},
        fileio::{
            databaseio::{delete_db_instance, Database},
            header::Schema,
            tableio::Table,
        },
        parser::parser::parse_vc_cmd,
        util::{
            bench::{create_demo_db, fcreate_db_instance},
            dbtype::*,
            row::Row,
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
            .create_commit_on_head(
                &"First commit".to_string(),
                &"Create table1; Insert 1 Row;".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        let commit: Commit = commit_result.1;

        // Log the commits
        let result: Vec<Vec<String>> = log(&user).unwrap().1;

        println!("result {:?}", result);

        // Assert that the result is correct
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], commit.user_id);
        assert_eq!(result[0][1], commit.hash);
        assert_eq!(result[0][2], commit.timestamp);
        assert_eq!(result[0][3], commit.message);

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
            .create_commit_on_head(
                &"First commit".to_string(),
                &"Create table1; Insert 1 Row;".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        let commit: Commit = commit_result.1;

        commit_result = get_db_instance()
            .unwrap()
            .create_commit_on_head(
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
        assert_eq!(result[1][0], commit.user_id);
        assert_eq!(result[1][1], commit.hash);
        assert_eq!(result[1][2], commit.timestamp);
        assert_eq!(result[1][3], commit.message);
        assert_eq!(result[0][0], second_commit.user_id);
        assert_eq!(result[0][1], second_commit.hash);
        assert_eq!(result[0][2], second_commit.timestamp);
        assert_eq!(result[0][3], second_commit.message);

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
        let query2 = "GQL delete branch_name";
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
        let query = "GQL delete branch_name1";
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
        let query1 = "GQL delete branch_name";
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
        let query3 = "GQL delete test";
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
        let query3 = "GQL delete -f test";
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
        let query1 = format!("GQL info {}", commit.clone().unwrap().hash);

        let result = parse_vc_cmd(&query1, &mut user, all_users.clone());
        assert!(result.is_ok());
        assert!(result.clone().unwrap().contains(&"--------- test_user's Commit ---------".to_string()));
        assert!(result.clone().unwrap().contains(&commit.clone().unwrap().hash));
        assert!(result.clone().unwrap().contains(&commit.clone().unwrap().message));
        assert!(result.clone().unwrap().contains(&commit.unwrap().timestamp));
        delete_db_instance().unwrap();
    }

    // Checks that discard deletes the -temp dir
    #[test]
    #[serial]
    fn test_discard_command() {
        //Creating db instance
        let db_name: String = "gql_discard_test".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user: User = User::new("test_user".to_string());

        let table_name1: String = "table1".to_string();

        //Making temp changes

        // Create a new table on main branch
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];

        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user)
            .unwrap();
        let mut _table1_info =
            create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();

        // Insert rows into the table on new branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Bruce Wayne".to_string())],
            vec![Value::I32(2), Value::String("Selina Kyle".to_string())],
            vec![Value::I32(3), Value::String("Damian Wayne".to_string())],
        ];
        let _res = insert(rows, table_name1, get_db_instance().unwrap(), &mut user).unwrap();

        // Storing temp directory path
        let temp_main_dir: String = get_db_instance().unwrap().get_temp_db_dir_path(&user);
        assert_ne!(user.get_diffs().len(), 0);
        assert_eq!(std::path::Path::new(&temp_main_dir).exists(), true);

        //Calling Discard
        discard(&mut user).unwrap();

        //Asserting that the user isn't on a temp commit
        assert_eq!(user.is_on_temp_commit(), false);

        // Asserting user diffs are now empty
        assert_eq!(user.get_diffs().len(), 0);
        assert_eq!(std::path::Path::new(&temp_main_dir).exists(), false);
        delete_db_instance().unwrap();
    }

    // Checks that discard deletes the -temp dir
    #[test]
    #[serial]
    fn test_discard_command_after_commit() {
        //Creating db instance
        let db_name: String = "gql_discard_test".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user: User = User::new("test_user".to_string());

        let table_name1: String = "table1".to_string();

        //Making temp changes

        // Create a new table on main branch
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];

        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user)
            .unwrap();
        create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();

        // Insert rows into the table on main branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Bruce Wayne".to_string())],
            vec![Value::I32(2), Value::String("Selina Kyle".to_string())],
            vec![Value::I32(3), Value::String("Damian Wayne".to_string())],
        ];
        let _res = insert(
            rows,
            table_name1.clone(),
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();

        // Create a commit on the main branch
        get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"First Commit".to_string(),
                &"Create Table & Added Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user)
            .unwrap();

        // Making temp_changes
        let rows2: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Dick Grayson".to_string())],
            vec![Value::I32(2), Value::String("Jason Todd".to_string())],
            vec![Value::I32(3), Value::String("Tim Drake".to_string())],
        ];
        let _res = insert(rows2, table_name1, get_db_instance().unwrap(), &mut user).unwrap();

        // Storing temp directory path
        let temp_main_dir: String = get_db_instance().unwrap().get_temp_db_dir_path(&user);
        assert_ne!(user.get_diffs().len(), 0);
        assert_eq!(std::path::Path::new(&temp_main_dir).exists(), true);

        //Calling Discard
        discard(&mut user).unwrap();

        //Asserting that the user isn't on a temp commit
        assert_eq!(user.is_on_temp_commit(), false);

        // Asserting user diffs are now empty
        assert_eq!(user.get_diffs().len(), 0);
        assert_eq!(std::path::Path::new(&temp_main_dir).exists(), false);
        delete_db_instance().unwrap();
    }

    // Checks that revert works with a valid commit hash
    #[test]
    #[serial]
    fn test_revert_command() {
        //Creating db instance
        let db_name = "gql_revert_test".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user: User = User::new("test_user".to_string());

        let table_name1: String = "table1".to_string();

        // Copying main dir to store state of database
        let copy_dir: String = "test_revert_copy_dir".to_string();
        std::fs::create_dir_all(copy_dir.clone()).unwrap();

        // Create a new table on the main
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user)
            .unwrap();
        let mut _table1_info =
            create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();

        // Create a commit on the main branch
        let node_commit1 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"First Commit".to_string(),
                &"Create Table;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user)
            .unwrap();

        // Insert rows into the table on main
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Bruce Wayne".to_string())],
            vec![Value::I32(2), Value::String("Selina Kyle".to_string())],
            vec![Value::I32(3), Value::String("Damian Wayne".to_string())],
        ];
        let _res = insert(
            rows,
            table_name1.clone(),
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();

        // To copy a single table to that dir
        std::fs::copy(
            (_table1_info.0).path.clone(),
            format!(
                "{}{}{}.db",
                copy_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
        )
        .unwrap();

        // Create commit on main branch
        let _node_commit2 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"Second Commit on Main - Added Wayne family".to_string(),
                &"Insert;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Reverting commit 2
        let _revert_commit = revert(&mut user, &(node_commit1.1).hash).unwrap();

        // Checking if the revert command made a difference
        // Get the directories for all the branches
        let main_branch_table_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        // Read in all the tables from the branch directories before we compare them
        let table_main: Table =
            Table::new(&main_branch_table_dir, &"table1".to_string(), None).unwrap();
        let table_copy: Table = Table::new(&copy_dir, &"table1".to_string(), None).unwrap();

        // Make sure that the main branch isn't the same as the copied folder
        assert_eq!(
            compare_tables(&table_main, &table_copy, &main_branch_table_dir, &copy_dir),
            false
        );
        delete_db_instance().unwrap();

        // Clean up
        std::fs::remove_dir_all("./test_revert_copy_dir").unwrap();
    }

    /// Helper that compares two tables to make sure that they are identical, but in separate directories
    fn compare_tables(
        table1: &Table,
        table2: &Table,
        table1dir: &String,
        table2dir: &String,
    ) -> bool {
        if table1dir == table2dir {
            return false;
        }

        // Make sure that table1 and table2 are the same and they point to the right directories
        if std::path::Path::new(&table1.path)
            != std::path::Path::new(&format!("{}/{}.db", table1dir, table1.name))
        {
            return false;
        }

        if std::path::Path::new(&table2.path)
            != std::path::Path::new(&format!("{}/{}.db", table2dir, table1.name))
        {
            return false;
        }

        if !file_diff::diff(&table1.path, &table2.path) {
            return false;
        }
        true
    }

    // Tries to get the all the table in a branch with no table
    #[test]
    #[serial]
    fn test_tables() {
        let query = "GQL table";
        // Create a new user on the main branch
        fcreate_db_instance("gql_tables_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());
        let result = parse_vc_cmd(query, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.unwrap() == "No tables in current branch!".to_string());
    }

    // Tries to get the all the table in a branch with a table
    #[test]
    #[serial]
    fn test_tables1() {
        let query = "GQL table";
        // Create a new user on the main branch
        fcreate_db_instance("gql_tables_test");
        let mut user: User = User::new("test_user".to_string());
        let mut all_users: Vec<User> = Vec::new();
        all_users.push(user.clone());

        let load_db = Database::load_db("gql_tables_test".to_string()).unwrap();
        create_table(
            &"testing".to_string(),
            &vec![("id".to_string(), Column::I32)],
            &load_db,
            &mut user,
        )
        .unwrap();

        let result = parse_vc_cmd(query, &mut user, all_users.clone());

        delete_db_instance().unwrap();
        assert!(result.is_ok());
    }
}
