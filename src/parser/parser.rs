use crate::user::userdata::User;
use crate::version_control::command;
use crate::version_control::commit::Commit;
use crate::version_control::merge::MergeConflictResolutionAlgo;
use crate::{fileio::databaseio::get_db_instance, version_control::command::revert};

use clap::Parser as ClapParser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::vc_commands::{VersionControl, VersionControlSubCommand};

/// A parse function, that starts with a string and returns an AST representation of the query.
/// If an error happens, an Err(msg) is returned.
pub fn parse(query: &str, _update: bool) -> Result<Vec<Statement>, String> {
    if query.len() == 0 {
        return Err("Empty query".to_string());
    }

    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, query);

    // println!("AST: {:?}", ast);
    return ast.map_err(|e| e.to_string());
}

/// This method parses a version control command's query string into the individual components.
/// Format "GQL <command> <flags> <args>"
pub fn parse_vc_cmd(query: &str, user: &mut User, all_users: Vec<User>) -> Result<String, String> {
    let command = shellwords::split(query)
        .map_err(|e| format!("Mismatched quotes while parsing query: {}", e))?;
    let parse = VersionControl::try_parse_from(command);
    match parse {
        Ok(parse) => {
            match parse.subcmd {
                VersionControlSubCommand::Commit { message } => {
                    // Make sure the user has some changes to commit
                    if user.get_diffs().len() == 0 {
                        return Err("No changes to commit".to_string());
                    }

                    let (res_node, res_commit) = get_db_instance()?.create_commit_and_node(
                        &message.to_string(),
                        &user.get_commands().join(":"),
                        user,
                        None,
                    )?;
                    Ok(format!(
                        "Commit created on branch {} with hash {}",
                        res_node.branch_name, res_commit.hash
                    ))
                }
                VersionControlSubCommand::Log { json } => {
                    let log_results = command::log(user)?;
                    if json {
                        Ok(log_results.2)
                    } else {
                        Ok(log_results.0)
                    }
                }
                VersionControlSubCommand::Info { commit: hash } => command::info(&hash),
                VersionControlSubCommand::Status => Ok(user.get_status()),
                VersionControlSubCommand::CreateBranch { branch_name } => {
                    get_db_instance()?
                        .create_branch(&branch_name, user)
                        .map_err(|e| e.to_string())?;
                    Ok(format!("Branch {} created!", branch_name))
                }
                VersionControlSubCommand::ListBranch { current } => {
                    if current {
                        Ok(user.get_current_branch_name())
                    } else {
                        command::list_branches(user)
                    }
                }
                VersionControlSubCommand::SwitchBranch { branch_name } => {
                    get_db_instance()?
                        .switch_branch(&branch_name, user)
                        .map_err(|e| e.to_string())?;
                    Ok(format!("Branch switched to {}", branch_name))
                }
                VersionControlSubCommand::MergeBranch {
                    src_branch,
                    dest_branch,
                    message,
                    delete_src,
                    strategy,
                } => {
                    // Get the strategy from the command string
                    let merge_strategy = match strategy.as_str() {
                        "ours" => MergeConflictResolutionAlgo::UseSource,
                        "theirs" => MergeConflictResolutionAlgo::UseTarget,
                        "clean" => MergeConflictResolutionAlgo::NoConflicts,
                        _ => Err(
                            "Invalid strategy: Must be one of 'ours', 'theirs', or 'clean'"
                                .to_string(),
                        )?,
                    };

                    if src_branch == dest_branch {
                        return Err("Cannot merge a branch into itself".to_string());
                    }

                    // Make sure user does not have any uncommitted changes
                    if user.get_diffs().len() > 0 {
                        return Err("Cannot merge with uncommitted changes".to_string());
                    }

                    // Swap user to the destination branch
                    get_db_instance()?
                        .switch_branch(&dest_branch, user)
                        .map_err(|e| e.to_string())?;

                    // Merge the source branch into the destination branch
                    let merge_commit: Commit = get_db_instance()?
                        .merge_branches(
                            &src_branch,
                            user,
                            &message,
                            true,
                            merge_strategy,
                            delete_src,
                        )
                        .map_err(|e| e.to_string())?;

                    Ok(format!("Merge Successful Made at hash {}", merge_commit.hash).to_string())
                }
                VersionControlSubCommand::DeleteBranch { branch_name, force } => {
                    let branch_heads_instance = get_db_instance()?.get_branch_heads_file_mut();
                    let branch_exist =
                        branch_heads_instance.does_branch_exist(branch_name.clone())?;
                    if !branch_exist {
                        return Err("Branch does not exist".to_string());
                    }
                    let del_results =
                        command::del_branch(user, &branch_name.clone(), force, all_users)?;
                    Ok(del_results)
                }
                VersionControlSubCommand::SquashCommit {
                    src_commit,
                    dest_commit,
                } => {
                    let squash_results = command::squash(&src_commit, &dest_commit, user)?;
                    Ok(format!(
                        "Squash Commit Made at hash: {}",
                        squash_results.hash
                    ))
                }
                VersionControlSubCommand::RevertCommit { commit } => {
                    let revert_results = command::revert(user, &commit)?;
                    Ok(format!("Reverted Commit at hash: {}", revert_results.hash))
                }
                VersionControlSubCommand::DiscardChanges => {
                    command::discard(user)?;
                    Ok("Discarded changes".to_string())
                }
                VersionControlSubCommand::SchemaTable => command::schema_table(user),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::{fileio::databaseio::delete_db_instance, util::bench::fcreate_db_instance};

    use super::*;

    #[test]
    #[serial]
    fn test_parse_vc_cmd() {
        let query = "GQL commit -m \"This is a commit message\"";
        // Create a new user on the main branch
        fcreate_db_instance("gql_log_db_instance_1");
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        delete_db_instance().unwrap();
        // We want it to return an error because the user has no changes to commit
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd2() {
        let query = "GQL commit";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd3() {
        let query = "GQL branch";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd4() {
        let query = "GQL branch branch_name";
        // Create a new user on the main branch
        fcreate_db_instance("gql_log_db_instance_2");
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_ok());
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd5() {
        let query = "GQL branch branch name";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd6() {
        let query0 = "GQL branch branch_name";
        let query = "GQL switch_branch branch_name";
        // Create a new user on the main branch
        fcreate_db_instance("TEST_DB");
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        parse_vc_cmd(query0, &mut user, all_users.clone()).unwrap();
        let result = parse_vc_cmd(query, &mut user, all_users);
        delete_db_instance().unwrap();
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd7() {
        let query = "GQL switch_branch branch name";
        // Create a new user on the main branch
        fcreate_db_instance("TEST_DB");
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd8() {
        let query = "GQL log";
        fcreate_db_instance("gql_log_db_instance_3");

        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());

        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_ok());

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd9() {
        let query = "GQL log -m";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd10() {
        let query = "GQL revert commit_hash";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd11() {
        let query = "GQL revert";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd12() {
        let query = "GQL status";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd13() {
        let query = "GQL status -m";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd14() {
        let query = "GQL commit -m ";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd15() {
        let query = "GQL commit -m \"\"";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd16() {
        let query = "GQL log -json";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let all_users: Vec<User> = Vec::new();
        let result = parse_vc_cmd(query, &mut user, all_users);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd() {
        let query = "SELECT * FROM test_table";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd2() {
        let query = "SELECT * FROM test_table WHERE id = 1";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd3() {
        let query = "SELECT * FROM test_table WHERE id = 1 AND name = \"test\"";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd4() {
        let query = "SELECT * FROM test_table WHERE id = 1 AND name = \"test\" OR age = 20";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd5() {
        let query = "CREATE TABLE customers (customer_id int, name varchar(255), age int);";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd6() {
        let query = "DROP TABLE dataquestDB;";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd7() {
        let query = "DRP TABLE customers;";
        let result = parse(query, false);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd8() {
        let query = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');";
        let result = parse(query, false);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd9() {
        let query = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUE ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');";
        let result = parse(query, false);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd10() {
        let query = "";
        let result = parse(query, false);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd11() {
        let query = "gql SELECT * FROM test_table WHERE id = 1 AND name = \"test\" OR age = 20";
        let result = parse(query, false);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_sql_cmd12() {
        let query = "UPDATE Customers SET ContactName = 'Alfred Schmidt', City= 'Frankfurt' WHERE CustomerID = 1;";
        let result = parse(query, true);
        assert!(result.is_ok());
    }
}
