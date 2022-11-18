use clap::Parser as ArgParser;
use clap::Subcommand;

// vec[0] = "GQL"
// vec[1] = <command>
// vec[2] = <flags> (optional) or <args> (optional)
// vec[3 and more] = <args>
#[derive(ArgParser, Debug)]
#[clap(author, version, about, long_about = "None")]
pub struct VersionControl {
    #[command(subcommand)]
    pub subcmd: VersionControlSubCommand,
}

#[derive(Subcommand, Debug)]
#[clap(rename_all = "snake_case")]
pub enum VersionControlSubCommand {
    /// Creates a new commit with the given message, at the current branch
    Commit {
        /// The commit message
        #[arg(long, short)]
        message: String,
    },
    /// Logs the history of commits for the current branch
    Log {
        /// Displays the log in json format (default is false)
        #[arg(long, short, default_value = "false")]
        json: bool,
    },
    /// Retrieves changes made in the referenced commit
    Info {
        /// The commit hash
        commit: String,
        /// Displays the log in json format (default is false)
        #[arg(long, short, default_value = "false")]
        json: bool,
    },
    /// Tells if there are any uncommitted changes
    Status,
    /// Joins all the commits between the two given commits into a single commit
    #[clap(aliases = &["squash"])]
    SquashCommit {
        /// The commit to squash from
        src_commit: String,
        /// The commit to squash till
        dest_commit: String,
    },
    /// Reverts the given commit
    #[clap(aliases = &["revert"])]
    RevertCommit {
        /// The commit to revert
        commit: String,
    },
    /// Discards the temporary changes made in the working directory
    #[clap(aliases = &["discard"])]
    DiscardChanges,
    /// Creates a new branch with the given name
    #[clap(aliases = &["create", "branch"])]
    CreateBranch {
        /// The name of the new branch
        branch_name: String,
    },
    /// Lists all branches, with the current branch marked with an asterisk
    #[clap(aliases = &["list"])]
    ListBranch {
        /// Just lists the current branch name (default is false)
        #[arg(long, short, default_value = "false")]
        current: bool,
    },
    /// Switches to the given branch
    #[clap(aliases = &["switch"])]
    SwitchBranch {
        /// The name of the branch to switch to
        branch_name: String,
    },
    /// Merges the given two branches together
    #[clap(aliases = &["merge"])]
    MergeBranch {
        /// The name of the branch to merge from
        src_branch: String,
        /// The name of the branch to merge into
        dest_branch: String,
        /// A message to be used for the merge commit
        message: String,
        /// Whether to delete the source branch after the merge (default is false)
        #[arg(long, short, default_value = "false")]
        delete_src: bool,
        /// The algorithm to use for resolving merge conflicts (options: "ours", "theirs", "clean")
        #[arg(long, short, default_value = "clean")]
        strategy: String,
    },
    /// Deletes the given branch
    #[clap(aliases = &["delete", "del"])]
    DeleteBranch {
        /// The name of the branch to delete
        branch_name: String,
        /// Whether to forcably delete the branch, ignoring uncommitted changes (default is false)
        #[arg(long, short, default_value = "false")]
        force: bool,
    },
    #[clap(aliases = &["view_branch"])]
    BranchView,
    /// Returns all of tables in the current branch
    #[clap(aliases = &["table", "schema", "scehma_table"])]
    SchemaTable {
        /// Displays the log in json format (default is false)
        #[arg(long, short, default_value = "false")]
        json: bool,
    },
    /// Returns the current user and all users
    User,
}
