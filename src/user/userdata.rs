use crate::{fileio::databaseio::*, version_control::diff::Diff};

pub const SYSTEM_USER_ID: &str = "system";

#[derive(Debug, Clone)]
pub struct User {
    user_id: String,         // The id of the user
    branch_name: String, // The name of the branch that the user is currently on (DOES NOT INCLUDE TEMP BRANCH SUFFIX)
    is_on_temp_commit: bool, // Whether the user is on a temporary commit. (uncommitted changes)
    // The temporary commit is the folder <db_name>-<branch_name>-<user_id>
    diffs: Vec<Diff>, // The changes that the user has made that are in an uncommitted state
}

impl User {
    /// Create a new user with the given id, which defaults to the main branch
    pub fn new(user_id: String) -> Self {
        Self {
            user_id: user_id,
            branch_name: MAIN_BRANCH_NAME.to_string(),
            is_on_temp_commit: false,
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

    /// Set the name of the branch that the user is currently on to a new branch
    pub fn set_current_branch_name(&mut self, new_branch_name: &String) {
        self.branch_name = new_branch_name.clone();
    }

    /// Get the list of diffs that the user has made
    pub fn get_diffs(&self) -> Vec<Diff> {
        self.diffs.clone()
    }

    /// Append a diff to the user's changes
    pub fn append_diff(&mut self, diff: &Diff) {
        self.diffs.push(diff.clone());
    }

    /// Replaces the user's diffs with the given list of diffs
    pub fn set_diffs(&mut self, diffs: &Vec<Diff>) {
        self.diffs = diffs.clone();
    }

    /// Whether the user is currently on a temporary commit
    pub fn is_on_temp_commit(&self) -> bool {
        self.is_on_temp_commit
    }

    /// Set whether the user is currently on a temporary commit
    pub fn set_is_on_temp_commit(&mut self, is_on_temp_commit: bool) {
        self.is_on_temp_commit = is_on_temp_commit;
    }
}
