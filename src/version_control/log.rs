use crate::fileio::databaseio::get_db_instance;

use super::branches::{BranchNode, Branches};

pub fn log() -> Result<(), String> {
    let branch_name: String = get_db_instance()?.get_current_branch_name();
    let branches_from_head: &Branches = get_db_instance()?.get_branch_file();

    // seperate to make debug easier
    let branch_instance = get_db_instance()?.get_current_branch_head_mut();

    let branch_node = branch_instance
        .get_branch_node_from_head(&branch_name, &branches_from_head)
        .unwrap();

    let mut branch_nodes: Vec<BranchNode> = get_db_instance()?
        .get_branch_file()
        .traverse_branch_nodes(&branch_node)?;

    branch_nodes.reverse();

    for node in branch_nodes {
        let commit = get_db_instance()?
            .get_commit_file_mut()
            .fetch_commit(&node.commit_hash)?;
        println!("Commit Hash: {}", commit.hash);
        println!("Commit time stamp: {}", commit.timestamp);
        println!("Commit message: {}", commit.message);
        println!("-----------------------");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::fileio::databaseio::create_db_instance;

    use super::*;

    #[test]
    fn test_log() {
        let result = log();
        assert!(result.is_ok());
    }

    #[test]
    fn test_log2() {
        create_db_instance(&"log_test_db".to_string()).unwrap();
    }
}
