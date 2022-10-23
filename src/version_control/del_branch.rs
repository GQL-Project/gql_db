use crate::{fileio::databaseio::get_db_instance, user::userdata::User};

/// This function deletes a branch from the database
pub fn del_branch(user: &User, branch_name: &String, flag: bool) -> Result<String, String> {
    let uncommitted= false;
    // Check if branch has uncommitted changes.
    
    if uncommitted && !flag {
        return Ok("Branch has uncommitted changes. Use -f to force delete.".to_string());
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