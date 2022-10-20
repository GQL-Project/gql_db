use crate::{fileio::databaseio::get_db_instance, user::userdata::User};

use super::branches::{BranchNode, Branches};

/// This function deletes a branch from the database
pub fn del_branch(user: &User, branch_name: &String, flag: bool) -> Result<String, String> {
    // Check if branch has uncommitted changes. If so, return an error

    // Delete the branch

    let result_string = format!("Branch {} deleted", &branch_name);
    Ok(result_string.to_string())
}