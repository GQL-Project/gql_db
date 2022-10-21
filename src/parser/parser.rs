use crate::fileio::databaseio::get_db_instance;
use crate::user::userdata::User;
use crate::version_control::command;
use crate::version_control::commit::Commit;
use crate::version_control::merge::MergeConflictResolutionAlgo;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
pub fn parse_vc_cmd(query: &str, user: &mut User) -> Result<String, String> {
    if query.len() == 0 || query == "GQL" {
        return Err("Empty VC Command query".to_string());
    }
    let command = query.split_whitespace();
    let mut vec = command.collect::<Vec<&str>>();

    // vec[0] = "GQL"
    // vec[1] = <command>
    // vec[2] = <flags> (optional) or <args> (optional)
    // vec[3 and more] = <args>

    // Switch case on the command
    match vec[1] {
        "commit" => {
            // commit (Possible flags: -m [maybe -a? Copilot recommended it])
            // message
            if vec.len() > 2 {
                if vec[2] != "-m" {
                    // error message here
                    return Err("Invalid Flag for Commit VC Command".to_string());
                } else {
                    // -m message here
                    // vec[4 and above] should be a commit message
                    if vec.len() == 3 {
                        return Err("Commit message cannot be empty".to_string());
                    }

                    // vec[3] and above are the commit messages
                    let mut message: String = String::new();
                    for i in 3..vec.len() {
                        message.push_str(&vec[i].replace(&"\"".to_string(), &"".to_string()));
                        message.push_str(" ");
                    }

                    if message == "\"\"" {
                        return Err("Commit message cannot be empty".to_string());
                    }

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
                    return Ok(format!(
                        "Commit created on branch {} with hash {}",
                        res_node.branch_name, res_commit.hash
                    ));
                }
            } else {
                // commit with no message
                return Err("Must include a commit message".to_string());
            }
        }
        "branch" => {
            // branch (CoPilot rec: Possible flags: -l (list branches), -c (current branch))
            // Needs an argument
            if vec.len() < 3 {
                // error message here
                return Err(format!("Invalid VC Command: {}", vec.join(" ")));
            } else if !vec[2].to_string().starts_with("-") && vec.len() > 3 {
                // spaces in the branch name
                // error message here
                return Err("Invalid Branch Name".to_string());
            } else {
                // using a flag that's not supposed to be used
                if vec[2].to_string().starts_with("-")
                    && vec[2].to_string() != "-l"
                    && vec[2].to_string() != "-c"
                {
                    return Err(format!(
                        "Invalid Flag for Branch VC Command: {}",
                        vec[2].to_string()
                    ));
                }
                if vec[2].to_string() == "-l" {
                    // We want to return a list of branches
                    let branch_names: Vec<String> = get_db_instance()?.get_all_branch_names()?;
                    // Join the branch_names into a single comma separated string
                    let branch_names_str: String = branch_names.join(",");

                    return Ok(branch_names_str);
                } else if vec[2].to_string() == "-c" {
                    // We want to return the current branch
                    return Ok(user.get_current_branch_name());
                } else {
                    // vec[2] should be a branch name
                    // create branch
                    get_db_instance()?
                        .create_branch(&vec[2].to_string(), user)
                        .map_err(|e| e.to_string())?;

                    return Ok(format!("Branch {} created!", &vec[2]));
                }
            }
        }
        "switch_branch" => {
            // merge
            // Needs an argument
            if vec.len() < 3 {
                // error message here
                return Err(format!("Invalid VC Command: {}", vec.join(" ")));
            } else if vec.len() > 3 {
                // spaces in the branch name
                // error message here
                return Err(format!("Invalid Branch Name '{}'", vec.join(" ")));
            } else {
                // vec[2] should be a branch name
                get_db_instance()?
                    .switch_branch(&vec[2].to_string(), user)
                    .map_err(|e| e.to_string())?;
                return Ok(format!("Branch switched to {}", &vec[2]));
            }
        }
        "merge_branch" => {
            // merge (One Arguments: <src_branch_name> <dest_branch_name> <message> (optional -d for deleting source branch) (optional -s <strategy> for strategy))
            // merges into the current branch

            // Combine the message into a single string
            let mut parsed_message: String = String::new();
            let mut post_msg_vec: Vec<&str> = Vec::new();
            for i in 4..vec.len() {
                if vec[i].to_string() == "-d" || vec[i].to_string() == "-s" {
                    post_msg_vec = vec[i..vec.len()].to_vec();
                    break;
                }
                parsed_message.push_str(&vec[i].replace(&"\"".to_string(), &"".to_string()));
                parsed_message.push_str(" ");
            }

            vec.splice(4..vec.len(), vec![parsed_message.trim()]);
            vec.append(&mut post_msg_vec);

            if vec.len() < 5 || vec.len() > 8 {
                return Err(format!(
                    "Invalid Merge Command, expected at least 3 arguments not {}",
                    vec.len() - 2
                )
                .to_string());
            }

            println!("Merge Command: {:?}", vec);

            let src_branch_name: String = vec[2].to_string();
            let dest_branch_name: String = vec[3].to_string();
            let message: String = vec[4].to_string();

            /// Get the strategy from the command string
            fn get_strategy(strategy: &str) -> Result<MergeConflictResolutionAlgo, String> {
                match strategy {
                    "ours" => Ok(MergeConflictResolutionAlgo::UseSource),
                    "theirs" => Ok(MergeConflictResolutionAlgo::UseTarget),
                    "clean" => Ok(MergeConflictResolutionAlgo::NoConflicts),
                    _ => Err(
                        "Invalid strategy: Must be one of 'ours', 'theirs', or 'clean'".to_string(),
                    ),
                }
            }

            // Check optional arguments
            let mut delete_src_branch: bool = false;
            let mut merge_strategy: MergeConflictResolutionAlgo =
                MergeConflictResolutionAlgo::NoConflicts;
            if vec.len() == 6 {
                if vec[5] == "-d" {
                    delete_src_branch = true;
                } else {
                    return Err("Invalid Merge Command. Invalid flag.".to_string());
                }
            } else if vec.len() == 7 {
                if vec[5] == "-s" {
                    merge_strategy = get_strategy(vec[5])?;
                } else {
                    return Err("Invalid Merge Command. Invalid flag.".to_string());
                }
            } else if vec.len() == 8 {
                if vec[5] == "-d" {
                    delete_src_branch = true;
                    if vec[6] == "-s" {
                        merge_strategy = get_strategy(vec[7])?;
                    } else {
                        return Err("Invalid Merge Command. Invalid flag.".to_string());
                    }
                } else if vec[5] == "-s" {
                    merge_strategy = get_strategy(vec[6])?;
                    if vec[7] == "-d" {
                        delete_src_branch = true;
                    } else {
                        return Err("Invalid Merge Command. Invalid flag.".to_string());
                    }
                } else {
                    return Err("Invalid Merge Command. Invalid flag.".to_string());
                }
            }

            if src_branch_name == dest_branch_name {
                return Err("Cannot merge a branch into itself".to_string());
            }

            // Make sure user does not have any uncommitted changes
            if user.get_diffs().len() > 0 {
                return Err("Cannot merge with uncommitted changes".to_string());
            }

            // Swap user to the destination branch
            get_db_instance()?
                .switch_branch(&dest_branch_name, user)
                .map_err(|e| e.to_string())?;

            // Merge the source branch into the destination branch
            let merge_commit: Commit = get_db_instance()?
                .merge_branches(
                    &src_branch_name,
                    user,
                    &message,
                    true,
                    merge_strategy,
                    delete_src_branch,
                )
                .map_err(|e| e.to_string())?;

            return Ok(format!("Merge Successful Made at hash {}", merge_commit.hash).to_string());
        }
        "log" => {
            // log (NO FLAGS OR ARGS)
            if vec.len() != 2 && vec.len() != 3 {
                // Error message here
                return Err(format!("Invalid VC Command: {}", vec.join(" ")));
            }

            let log_results = command::log(user)?;
            let log_string: String = log_results.0;

            if vec.len() == 3 {
                if vec[2] != "-json" {
                    // Error message here
                    return Err("Invalid VC Command".to_string());
                } else {
                    // Return the log in JSON format
                    return Ok(log_results.2);
                }
            }
            return Ok(log_string);
        }
        "squash" => {
            // squash (Two Arguments: <hash1> <hash2>)
            if vec.len() != 4 {
                // Error message here
                return Err("Invalid Squash Command, expected two Commit Hashes".to_string());
            }

            let hash1 = vec[2].to_string();
            let hash2 = vec[3].to_string();
            let squash_results = command::squash(&hash1, &hash2, user)?;
            return Ok(format!(
                "Squash Commit Made at hash: {}",
                squash_results.hash
            ));
        }
        "revert" => {
            // revert (Needs an argument)
            if vec.len() != 3 {
                // error message here
                return Err(format!("Invalid VC Command: {}", vec.join(" ")));
            } else {
                // vec[2] should be a commit hash
                return Ok("Valid Revert Command".to_string());
            }
        }
        "status" => {
            // status (NO FLAGS OR ARGS)
            if vec.len() != 2 {
                // error message here
                return Err(format!("Invalid VC Command: {}", vec.join(" ")));
            }
            return Ok("Valid Status Command".to_string());
        }
        _ => {
            // error message here
            return Err(format!("Invalid VC Command: {}", vec.join(" ")));
        }
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
        let result = parse_vc_cmd(query, &mut user);
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
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd3() {
        let query = "GQL branch";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd4() {
        let query = "GQL branch branch_name";
        // Create a new user on the main branch
        fcreate_db_instance("gql_log_db_instance_2");
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_ok());
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd5() {
        let query = "GQL branch branch name";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
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
        parse_vc_cmd(query0, &mut user).unwrap();
        let result = parse_vc_cmd(query, &mut user);
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
        let result = parse_vc_cmd(query, &mut user);
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

        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_ok());

        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd9() {
        let query = "GQL log -m";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd10() {
        let query = "GQL revert commit_hash";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd11() {
        let query = "GQL revert";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd12() {
        let query = "GQL status";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd13() {
        let query = "GQL status -m";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd14() {
        let query = "GQL commit -m ";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd15() {
        let query = "GQL commit -m \"\"";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd16() {
        let query = "GQL log -json";
        // Create a new user on the main branch
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
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
