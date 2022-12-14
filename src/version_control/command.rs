use crate::fileio::pageio::PageType;
use crate::{fileio::databaseio::*, user::userdata::User};

use crate::{
    fileio::{
        header::read_schema,
        pageio::{read_page, Page},
    },
    util::dbtype::Column,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json;
use tabled::{builder::Builder, Style};

use std::collections::HashMap;
use std::fs;

use super::branch_heads::BranchHEADs;
use super::diff::*;
use super::merge::{create_merge_diffs, MergeConflictResolutionAlgo};
use super::merged_branches::MergedBranch;
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

#[derive(Serialize)]
pub struct CommitGraphNode {
    pub commit_hash: String,
    pub branch_name: String,
    pub column: u32,
    pub row: u32,
    pub first_branch_commit: bool,
    pub is_merged_branch: bool,
}

#[derive(Serialize)]
pub struct CommitGraphEdge {
    pub src_commit_hash: String,
    pub dest_commit_hash: String,
}

#[derive(Serialize)]
pub struct CommitGraph {
    pub nodes: Vec<CommitGraphNode>,
    pub edges: Vec<CommitGraphEdge>,
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

        log_string = format!(
            "{}\n---------- {}'s Commit ----------",
            log_string, commit.user_id
        );
        log_string = format!("{}\nCommit: {}", log_string, commit.hash);
        log_string = format!("{}\nMessage: {}", log_string, commit.message);
        log_string = format!("{}\nTimestamp: {}", log_string, commit.timestamp);
        log_string = format!("{}\n", log_string);

        let printed_vals: Vec<String> = vec![
            commit.user_id,
            commit.hash,
            commit.timestamp,
            commit.message,
        ];

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
    let hash1 = get_db_instance()?
        .get_commit_file_mut()
        .resolve_commit(hash1)?;
    let hash2 = get_db_instance()?
        .get_commit_file_mut()
        .resolve_commit(hash2)?;

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

    let squash_commit = get_db_instance()?.get_commit_file_mut().squash_commits(
        user.get_user_id(),
        &commits,
        true,
    )?;

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

    //TODO Modify this to work with an abbreviated hash - this does not look too hard to implement

    let commit_hash = get_db_instance()?
        .get_commit_file_mut()
        .resolve_commit(commit_hash)?;

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
        // not sure what is going wrong here
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

    log_string = format!(
        "{}\n---------- {}'s Commit ----------",
        log_string, commit.user_id
    );
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

/// This function is used to update the user's copy of the db
/// to the latest commit if the user is behind
/// Takes in user object and Returns Success or Error
pub fn pull(
    user: &mut User,
    merge_conflict_algo: MergeConflictResolutionAlgo,
) -> Result<String, String> {
    // Checking the branch's status to ensure if the user is up-to-date
    let behind_check = user.get_status();
    if !behind_check.1 {
        return Ok("Your branch is already up-to-date.".to_string());
    }

    //Getting user branch info
    let user_branch_name = user.get_current_branch_name();
    let user_branch_path = get_db_instance()?.get_current_branch_path(user);

    // Get the branch node of the head commit of the user's branch
    let branches_from_head: &Branches = get_db_instance().unwrap().get_branch_file();
    let branch_heads_instance = get_db_instance().unwrap().get_branch_heads_file_mut();
    let branch_node =
        branch_heads_instance.get_branch_node_from_head(&user_branch_name, &branches_from_head)?;

    //If the head node exists, get changes and apply them

    //current_node = the actual head of the branch
    //user_curr_node = the node that was the head when the user last updated
    let current_node = branch_node;
    let user_curr_node = user
        .get_user_branch_head()
        .ok_or("User branch head not found".to_string())?;

    //Getting diffs between the two nodes
    let diffs_to_pull =
        get_db_instance()?.get_diffs_between_nodes(Some(&user_curr_node), &current_node)?;

    if user.get_diffs().len() != 0 {
        // Target Diffs are changes user has made
        let target_diffs = user.get_diffs();

        // Source Diffs are the changes that the user
        // missed out on while making their own changes
        let source_commits: Vec<Commit> =
            get_db_instance()?.get_commits_between_nodes(Some(&user_curr_node), &current_node)?;
        // Only need to squash the commits if there are some
        let mut source_diffs: Vec<Diff> = Vec::new();
        if source_commits.len() > 0 {
            let source_squashed_cmt: Commit = get_db_instance()?
                .get_commit_file_mut()
                .squash_commits(user.get_user_id(), &source_commits, false)?;
            source_diffs = source_squashed_cmt.diffs;
        }

        // Getting the diffs for the merge operation
        let merged_diffs = create_merge_diffs(
            &source_diffs,
            &target_diffs,
            &get_db_instance()?.get_current_working_branch_path(user),
            merge_conflict_algo,
        )?;

        // Apply the merged diffs
        construct_tables_from_diffs(
            &get_db_instance()?.get_current_working_branch_path(user),
            &merged_diffs,
        )?;
        return Ok(
            ("Your branch is now up-to-date. The changes you missed".to_owned()
                + "were merged with your uncommitted changes")
                .to_string(),
        );
    }

    // Applying diffs to user's branch
    construct_tables_from_diffs(&user_branch_path, &diffs_to_pull)?;
    // Updating user's branch head
    user.set_user_branch_head(None);
    //user.set_user_branch_head(&current_node);
    Ok("Your branch is now up-to-date!".to_string())
}

/// Lists all the commits for all branches
pub fn list_all_commits() -> Result<String, String> {
    let branch_heads_file: &mut BranchHEADs = get_db_instance()?.get_branch_heads_file_mut();
    let branches_file: &Branches = &mut get_db_instance()?.get_branch_file();
    let branch_names: Vec<String> = branch_heads_file.get_all_branch_names()?;

    // Get all the branch nodes for each branch HEAD
    let mut branch_head_nodes: Vec<BranchNode> = Vec::new();
    for branch_name in branch_names {
        let branch_head_node: BranchNode =
            branch_heads_file.get_branch_node_from_head(&branch_name, branches_file)?;
        branch_head_nodes.push(branch_head_node);
    }

    // Collect all the branch nodes for each branch.
    // Store it in a vector of tuples where each tuple is (branch_name, branch_nodes)
    // The branch_nodes are in chronological order (origin is first, head is last)
    let mut all_branch_nodes: Vec<(String, Vec<BranchNode>)> = Vec::new();
    let mut max_branch_len: usize = 0;
    for head_node in branch_head_nodes {
        let branch_name: String = head_node.branch_name.clone();
        let mut branch_nodes: Vec<BranchNode> = Vec::new();
        let mut this_branch_len: usize = 1;

        let mut current_node: BranchNode = head_node;
        branch_nodes.push(current_node.clone());
        while let Some(prev_node) = branches_file.get_prev_branch_node(&current_node)? {
            branch_nodes.push(prev_node.clone());
            current_node = prev_node;
            this_branch_len += 1;
        }
        branch_nodes.reverse();
        all_branch_nodes.push((branch_name, branch_nodes));

        if this_branch_len > max_branch_len {
            max_branch_len = this_branch_len;
        }
    }

    // Get all the past merged branches
    let past_merged_branches: Vec<MergedBranch> = get_db_instance()?
        .get_merged_branches_file_mut()
        .get_merged_branches()?;

    // Turn all branch nodes into a json table thingy
    let mut graph: CommitGraph = CommitGraph {
        nodes: Vec::new(),
        edges: Vec::new(),
    };

    // Maps the branch_name to the column number
    let mut used_cols: HashMap<String, u32> = HashMap::new();
    for i in 0..max_branch_len {
        // Get the unique branch nodes at this row level
        let mut unique_branch_nodes: Vec<BranchNode> = Vec::new();
        for (branch_name, branch_nodes) in &all_branch_nodes {
            if branch_nodes.len() > i && *branch_name == branch_nodes[i].branch_name {
                unique_branch_nodes.push(branch_nodes[i].clone());
            }
        }

        // Add the unique branch nodes to the graph
        for unique_node in unique_branch_nodes {
            let branch_name: String = unique_node.branch_name.clone();
            let commit_hash: String = unique_node.commit_hash.clone();

            // If this is the first time we've seen this branch, add it to the used_cols map
            let mut is_first_branch_commit: bool = false;
            if !used_cols.contains_key(&branch_name) {
                let mut new_col: u32 = 0;
                loop {
                    if !used_cols.values().contains(&new_col) {
                        break;
                    }
                    new_col += 1;
                }
                used_cols.insert(branch_name.clone(), new_col);
                is_first_branch_commit = true;
            }

            // If one of the past merged_branches has its source branch as this branch and it has a different branch name,
            // then we need to add a node and edges
            // In this case, the branch was deleted after the merge
            if let Some(merged_branch) = past_merged_branches
                .iter()
                .find(|x| x.source_commit == commit_hash && x.branch_name != branch_name)
            {
                let merged_branch_name: String = merged_branch.branch_name.clone();
                let dest_branch_col: u32 = match used_cols.get(&merged_branch_name) {
                    Some(col) => *col,
                    None => {
                        let mut new_col: u32 = 0;
                        loop {
                            if !used_cols.values().contains(&new_col) {
                                break;
                            }
                            new_col += 1;
                        }
                        used_cols.insert(merged_branch_name.clone(), new_col);
                        new_col
                    }
                };
                let branch_node_hash: String = commit_hash.to_string() + "_";
                let dest_branch_node: CommitGraphNode = CommitGraphNode {
                    commit_hash: branch_node_hash.clone(),
                    branch_name: merged_branch_name.clone(),
                    column: dest_branch_col,
                    row: i as u32,
                    first_branch_commit: true,
                    is_merged_branch: true,
                };
                graph.nodes.push(dest_branch_node);

                let edge: CommitGraphEdge = CommitGraphEdge {
                    src_commit_hash: commit_hash.clone(),
                    dest_commit_hash: branch_node_hash.clone(),
                };
                graph.edges.push(edge);
                let edge: CommitGraphEdge = CommitGraphEdge {
                    src_commit_hash: branch_node_hash.clone(),
                    dest_commit_hash: merged_branch.destination_commit.clone(),
                };
                graph.edges.push(edge);
            } else if let Some(merged_branch) = past_merged_branches
                .iter()
                .find(|x| x.source_commit == commit_hash && x.branch_name == branch_name)
            {
                let edge: CommitGraphEdge = CommitGraphEdge {
                    src_commit_hash: merged_branch.source_commit.clone(),
                    dest_commit_hash: merged_branch.destination_commit.clone(),
                };
                graph.edges.push(edge);
            }

            // Add the node to the graph
            graph.nodes.push(CommitGraphNode {
                commit_hash: commit_hash.clone(),
                column: used_cols[&branch_name],
                branch_name,
                row: i as u32,
                first_branch_commit: is_first_branch_commit,
                is_merged_branch: false,
            });

            // Add the edge to the graph if it exists
            if let Some(prev_node) = branches_file.get_prev_branch_node(&unique_node)? {
                let edge = CommitGraphEdge {
                    src_commit_hash: prev_node.commit_hash.clone(),
                    dest_commit_hash: commit_hash.clone(),
                };
                graph.edges.push(edge);
            }
        }
    }

    return Ok(serde_json::to_string(&graph).map_err(|e| e.to_string())?);
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
        assert!(result
            .clone()
            .unwrap()
            .contains(&"--------- test_user's Commit ---------".to_string()));
        assert!(result
            .clone()
            .unwrap()
            .contains(&commit.clone().unwrap().hash));
        assert!(result
            .clone()
            .unwrap()
            .contains(&commit.clone().unwrap().message));
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

    // Testing the pull command when the user is up-to-date
    #[test]
    #[serial]
    fn test_pull_no_changes() {
        //Creating db instance
        let db_name = "gql_pull_test_no_change".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user: User = User::new("test_user".to_string());

        // Create a new table on the main
        let table_name1: String = "table1".to_string();
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
        get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"First Commit".to_string(),
                &"Create Table;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        //Calling pull when there's no new changes
        let pull_result = pull(&mut user, MergeConflictResolutionAlgo::NoConflicts);

        assert_eq!(pull_result.is_ok(), true);
        assert_eq!(
            pull_result.unwrap(),
            "Your branch is already up-to-date.".to_string()
        );
        assert_eq!(user.get_status().1, false);

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_pull_w_one_change() {
        //Creating db instance
        let db_name = "gql_pull_test_w_one_change".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user1: User = User::new("test_user1".to_string());
        let mut user2: User = User::new("test_user2".to_string());

        // Main Dir path
        let main_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        // Copying main dir to store state of database
        let user2_dir: String = "test_pull_user2_dir".to_string();
        std::fs::create_dir_all(user2_dir.clone()).unwrap();

        // Creating a dir to compare to
        let compare_dir: String = "test_pull_compare_dir".to_string();
        std::fs::create_dir_all(compare_dir.clone()).unwrap();

        // Create a new table on the main
        let table_name1: String = "table1".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();
        let mut _table1_info = create_table(
            &table_name1,
            &schema,
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // Create a commit on the main branch
        let node_commit1 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"First Commit".to_string(),
                &"Create Table;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        let table1 = Table::new(&main_dir, &table_name1, None).unwrap();

        // Copying node_commit1 state to user2 dir to emulate user being behind
        std::fs::copy(
            &table1.path,
            format!(
                "{}{}{}.db",
                user2_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
        )
        .unwrap();

        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();

        // User1 makes some more changes that User2 misses out on
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Bruce Wayne".to_string())],
            vec![Value::I32(2), Value::String("Selina Kyle".to_string())],
            vec![Value::I32(3), Value::String("Damian Wayne".to_string())],
        ];
        let _res = insert(
            rows,
            table_name1.clone(),
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1 creates commit on main branch
        let _node_commit2 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"Second Commit on Main - Added Wayne family".to_string(),
                &"Insert;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        // Copying main dir to compare dir to verify pull worked
        std::fs::copy(
            &table1.path,
            format!(
                "{}{}{}.db",
                compare_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
        )
        .unwrap();

        // Copying user2 state to main dir to emulate user being behind
        std::fs::copy(
            format!(
                "{}{}{}.db",
                user2_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
            table1.path,
        )
        .unwrap();

        // Setting the user's branch head to the first commit
        user2.set_user_branch_head(Some(&node_commit1.0));

        // Calling pull
        let pull_result = pull(&mut user2, MergeConflictResolutionAlgo::NoConflicts);

        assert_eq!(pull_result.is_ok(), true);
        assert_eq!(
            pull_result.unwrap(),
            "Your branch is now up-to-date!".to_string()
        );

        // Get the directories for all the branches
        let main_branch_table_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        // Read in all the tables from the branch directories before we compare them
        let table_user2_main: Table =
            Table::new(&main_branch_table_dir, &"table1".to_string(), None).unwrap();
        let table_compare_copy: Table =
            Table::new(&compare_dir, &"table1".to_string(), None).unwrap();

        let table_old_state: Table = Table::new(&user2_dir, &"table1".to_string(), None).unwrap();

        // Make sure that the main branch table isn't the same as the table copy in the user2 dir
        assert_eq!(
            compare_tables(
                &table_user2_main,
                &table_old_state,
                &main_branch_table_dir,
                &user2_dir
            ),
            false
        );

        // Make sure that the main branch table is the same as the table copy in the compare dir
        assert_eq!(
            compare_tables(
                &table_user2_main,
                &table_compare_copy,
                &main_branch_table_dir,
                &compare_dir
            ),
            true
        );
        delete_db_instance().unwrap();

        //Deleting the revert_copy dir after test
        std::fs::remove_dir_all(user2_dir).unwrap();
        std::fs::remove_dir_all(compare_dir).unwrap();
    }

    #[test]
    #[serial]
    fn test_pull_multiple_changes_1_table() {
        //Creating db instance
        let db_name = "gql_pull_test_multi_changes".to_string();
        fcreate_db_instance(&db_name);

        //Creating a new user
        let mut user1: User = User::new("test_user1".to_string());
        let mut user2: User = User::new("test_user2".to_string());

        // Main Dir path
        let main_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        // Creating user2_dir to store user2 state
        let user2_dir: String = "test_pull_user2_dir".to_string();
        std::fs::create_dir_all(user2_dir.clone()).unwrap();

        // Creating compare_dir to store final state of branch to compare against
        // and verify pull_changes works
        let compare_dir: String = "test_pull_compare_dir".to_string();
        std::fs::create_dir_all(compare_dir.clone()).unwrap();

        /* === USER 1 CHANGES === */
        // User1: Table Create -> Table1
        let table_name1: String = "table1".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();
        let mut _table1_info = create_table(
            &table_name1,
            &schema,
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1: Commit
        let node_commit1 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"First Commit".to_string(),
                &"Create Table;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        // Storing table1 as an object for future reference
        let table1 = Table::new(&main_dir, &table_name1, None).unwrap();

        // Copying node_commit1 state to user2 dir to store state
        std::fs::copy(
            &table1.path,
            format!(
                "{}{}{}.db",
                user2_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
        )
        .unwrap();

        // User1: Insert -> Rows
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Bruce Wayne".to_string())],
            vec![Value::I32(2), Value::String("Selina Kyle".to_string())],
            vec![Value::I32(3), Value::String("Damian Wayne".to_string())],
        ];
        let _res = insert(
            rows,
            table_name1.clone(),
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1: Commit
        let _node_commit2 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"Second Commit on Main - Added Wayne family".to_string(),
                &"Insert;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        // User1: Insert -> Rows2
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();
        let rows2: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("Clark Kent".to_string())],
            vec![Value::I32(2), Value::String("Lois Lane".to_string())],
            vec![Value::I32(3), Value::String("Jon Kent".to_string())],
        ];
        let _res2 = insert(
            rows2,
            table_name1.clone(),
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1: Commit
        let _node_commit3 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"Third Commit on Main - Added Kent family".to_string(),
                &"Insert;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        // User1: TableCreate -> Table2
        let table_name2: String = "table2".to_string();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
            ("alias".to_string(), Column::String(50)),
        ];
        get_db_instance()
            .unwrap()
            .create_temp_branch_directory(&mut user1)
            .unwrap();
        let mut _table2_info = create_table(
            &table_name2,
            &schema,
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1: Insert -> Rows3
        let rows3: Vec<Row> = vec![
            vec![
                Value::I32(1),
                Value::String("Clark Kent".to_string()),
                Value::I32(34),
                Value::String("Superman".to_string()),
            ],
            vec![
                Value::I32(2),
                Value::String("Lois Lane".to_string()),
                Value::I32(34),
                Value::String("N/A".to_string()),
            ],
            vec![
                Value::I32(3),
                Value::String("Jon Kent".to_string()),
                Value::I32(9),
                Value::String("Superboy".to_string()),
            ],
        ];
        let _res2 = insert(
            rows3,
            table_name2.clone(),
            get_db_instance().unwrap(),
            &mut user1,
        )
        .unwrap();

        // User1: Commit
        let _node_commit4 = get_db_instance()
            .unwrap()
            .create_commit_on_head(
                &"Fourth Commit on Main - Added new table w/ Kent family".to_string(),
                &"TableCreate; Insert;".to_string(),
                &mut user1,
                None,
            )
            .unwrap();

        // Storing Table2 object for future reference
        let table2 = Table::new(&main_dir, &table_name2, None).unwrap();

        /* === LOADING USER2 STATE INTO MAIN & SAVING USER1 STATE === */
        // Copying files from main to compare_dir to store branch's final state
        std::fs::copy(
            &table1.path,
            format!(
                "{}{}{}.db",
                compare_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
        )
        .unwrap();
        std::fs::copy(
            &table2.path,
            format!(
                "{}{}{}.db",
                compare_dir,
                std::path::MAIN_SEPARATOR,
                &table_name2
            ),
        )
        .unwrap();

        // Copying files from user2_dir to load user2 state into main dir
        std::fs::copy(
            format!(
                "{}{}{}.db",
                user2_dir,
                std::path::MAIN_SEPARATOR,
                &table_name1
            ),
            table1.path,
        )
        .unwrap();
        std::fs::remove_file(&table2.path).unwrap(); // Removing table2 from main as user2 missed it

        // Setting the user's branch head to the first commit
        user2.set_user_branch_head(Some(&node_commit1.0));

        // Calling pull
        let pull_result = pull(&mut user2, MergeConflictResolutionAlgo::NoConflicts);

        /* === ASSERTS === */
        assert_eq!(pull_result.is_ok(), true);
        assert_eq!(
            pull_result.unwrap(),
            "Your branch is now up-to-date!".to_string()
        );

        // Get the directories for all the branches
        let main_branch_table_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        // Read in all the table 1s from the branch directories before we compare them
        let table_user2_main: Table =
            Table::new(&main_branch_table_dir, &"table1".to_string(), None).unwrap();
        let table_compare_copy: Table =
            Table::new(&compare_dir, &"table1".to_string(), None).unwrap();

        let table_old_state: Table = Table::new(&user2_dir, &"table1".to_string(), None).unwrap();

        // Make sure that the main branch table isn't the same as the table copy in the user2 dir
        assert_eq!(
            compare_tables(
                &table_user2_main,
                &table_old_state,
                &main_branch_table_dir,
                &user2_dir
            ),
            false
        );

        // Make sure that the main branch table is the same as the table copy in the compare dir
        assert_eq!(
            compare_tables(
                &table_user2_main,
                &table_compare_copy,
                &main_branch_table_dir,
                &compare_dir
            ),
            true
        );
        // Asserting that the table2 file was created
        assert_eq!(std::path::Path::new(&table2.path).exists(), true);

        //Deleting the revert_copy dir after test
        delete_db_instance().unwrap();
        std::fs::remove_dir_all(user2_dir).unwrap();
        std::fs::remove_dir_all(compare_dir).unwrap();
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
