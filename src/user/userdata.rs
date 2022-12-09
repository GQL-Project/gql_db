use crate::{
    fileio::databaseio::*,
    version_control::{
        branches::{BranchNode, Branches},
        diff::Diff,
    },
};

use super::usercreds::UserPermissions;

/*#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum UserPermissions {
    Read,
    Write,
    ReadAndWrite,
    Admin,
}*/

#[derive(Debug, Clone)]
pub struct User {
    user_id: String,         // The id of the user
    branch_name: String, // The name of the branch that the user is currently on (DOES NOT INCLUDE TEMP BRANCH SUFFIX)
    is_on_temp_commit: bool, // Whether the user is on a temporary commit. (uncommitted changes)
    // The temporary commit is the folder <db_name>-<branch_name>-<user_id>
    diffs: Vec<Diff>, // The changes that the user has made that are in an uncommitted state
    commands: Vec<String>, // The commands that the user has executed that are in an uncommitted state
    branch_head: Option<BranchNode>, // The commit id of the head of the branch that the user is currently on
    user_permissions: UserPermissions, // The user's abiltiy to read / write to a database
}

impl User {
    /// Create a new user with the given id, which defaults to the main branch
    pub fn new(user_id: String) -> Self {
        Self {
            user_id: user_id,
            branch_name: MAIN_BRANCH_NAME.to_string(),
            is_on_temp_commit: false,
            diffs: Vec::new(),
            commands: Vec::new(),
            branch_head: None,
            user_permissions: UserPermissions::ReadAndWrite,
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

    /// Get the permissions that the user has
    pub fn get_permissions(&self) -> UserPermissions {
        self.user_permissions.clone()
    }

    /// Append a diff to the user's changes
    pub fn append_diff(&mut self, diff: &Diff) {
        self.diffs.push(diff.clone());
    }

    /// Replaces the user's diffs with the given list of diffs
    pub fn set_diffs(&mut self, diffs: &Vec<Diff>) {
        self.diffs = diffs.clone();
    }

    /// Get the list of commands that the user has executed
    pub fn get_commands(&self) -> Vec<String> {
        self.commands.clone()
    }

    /// Append a command to the user's commands
    pub fn append_command(&mut self, command: &String) {
        self.commands.push(command.clone());
    }

    /// Replaces the user's commands with the given list of commands
    pub fn set_commands(&mut self, commands: &Vec<String>) {
        self.commands = commands.clone();
    }

    /// Replaces the user's permissions with the given permission
    pub fn set_permissions(&mut self, permissions: &UserPermissions) {
        self.user_permissions = permissions.clone();
    }

    /// Whether the user is currently on a temporary commit
    pub fn is_on_temp_commit(&self) -> bool {
        self.is_on_temp_commit
    }

    /// Set whether the user is currently on a temporary commit
    pub fn set_is_on_temp_commit(&mut self, is_on_temp_commit: bool) {
        if is_on_temp_commit {
            let branches_from_head: &Branches = get_db_instance().unwrap().get_branch_file();
            let branch_heads_instance = get_db_instance().unwrap().get_branch_heads_file_mut();

            let branch_node = branch_heads_instance
                .get_branch_node_from_head(&self.branch_name, &branches_from_head);

            match branch_node {
                Ok(node) => {
                    self.branch_head = Some(node);
                }
                Err(_e) => {
                    self.branch_head = None;
                }
            }
        } else {
            self.branch_head = None;
        }
        self.is_on_temp_commit = is_on_temp_commit;
    }

    // GQL Status
    pub fn get_status(&self) -> (String, bool) {
        let mut is_behind = false;

        let branches_from_head: &Branches = get_db_instance().unwrap().get_branch_file();
        let branch_heads_instance = get_db_instance().unwrap().get_branch_heads_file_mut();

        let branch_node =
            branch_heads_instance.get_branch_node_from_head(&self.branch_name, &branches_from_head);

        let mut status = String::new();
        status.push_str(&format!("On branch {}\n", self.branch_name));

        match branch_node {
            Ok(node) => match self.branch_head.clone() {
                Some(head) => {
                    if node.commit_hash == head.commit_hash {
                        // User's temp branch is at the same commit as the branch head
                        status.push_str(&format!(
                            "Your branch is up to date with {}\n\n",
                            self.branch_name
                        ));
                    } else {
                        // User's temp branch is behind
                        let branch_nodes: Vec<BranchNode> = get_db_instance()
                            .unwrap()
                            .get_branch_file()
                            .traverse_branch_nodes(&node)
                            .unwrap();

                        let mut num_commits_behind = 0;
                        for n in branch_nodes {
                            if n.commit_hash == head.commit_hash {
                                break;
                            }
                            num_commits_behind += 1;
                        }

                        if num_commits_behind > 0 {
                            is_behind = true;
                        }

                        status.push_str(&format!(
                            "Your branch is behind {} by {} commits\n\n",
                            self.branch_name, num_commits_behind
                        ));
                    }
                }
                None => status.push_str(&format!(
                    "Your branch is up to date with {}\n\n",
                    self.branch_name
                )),
            },
            Err(_e) => status.push_str(&format!(
                "Your branch is up to date with {}\n\n",
                self.branch_name
            )),
        }

        // List of uncommitted changes
        if self.commands.len() > 0 {
            status.push_str(&format!(
                "You have {} uncommitted changes:\n\n",
                self.commands.len()
            ));
            for command in &self.commands {
                status.push_str(&format!("{}\n", command));
            }
        } else {
            status.push_str("Nothing to commit, working tree clean\n");
        }
        (status, is_behind)
    }
}
