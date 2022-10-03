use crate::{fileio::databaseio::*, version_control::diff::Diff};

#[derive(Debug, Clone)]
pub struct User {
    user_id: String,     // The id of the user
    branch_name: String, // The name of the branch that the user is currently on
    diffs: Vec<Diff>,    // The changes that the user has made that are in an uncommitted state
}

impl User {
    /// Create a new user with the given id, which defaults to the main branch
    pub fn new(user_id: String) -> Self {
        Self {
            user_id: user_id,
            branch_name: MAIN_BRANCH_NAME.to_string(),
            diffs: Vec::new(),
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
    
    /// Get the list of diffs that the user has made
    pub fn get_diffs(&self) -> Vec<Diff> {
        self.diffs.clone()
    }

    /// Append a diff to the user's changes
    pub fn append_diff(&mut self, diff: &Diff) {
        self.diffs.push(diff.clone());
    }
}