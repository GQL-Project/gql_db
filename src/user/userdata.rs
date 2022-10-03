use crate::fileio::databaseio::*;

#[derive(Debug, Clone)]
pub struct User {
    user_id: String,     // The id of the user
    branch_name: String, // The name of the branch that the user is currently on
}

impl User {
    /// Create a new user with the given id, which defaults to the main branch
    pub fn new(user_id: String) -> Self {
        Self {
            branch_name: MAIN_BRANCH_NAME.to_string(),
            user_id: user_id,
        }
    }

    /// Get the id of the user
    pub fn get_user_id(&self) -> String {
        self.user_id.clone()
    }

    /// Get the name of the branch that the user is currently on
    pub fn get_current_branch_name(&self) -> String {
        self.branch_name.clone()
    }
}