use crate::fileio::databaseio::get_db_instance;
use crate::user::userdata::User;
use crate::version_control::log;
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
    let vec = command.collect::<Vec<&str>>();

    // vec[0] = "GQL"
    // vec[1] = <command>
    // vec[2] = <flags> (optional) or <args> (optional)
    // vec[3 and more] = <args>

    // Switch case on the command
    match vec[1] {
        "commit" => {
            // commit (Possible flags: -m [maybe -a? Copilot recommended it])
            println!("{:?}", "commit command");
            // message
            if vec.len() > 2 {
                if vec[2] != "-m" {
                    // error message here
                    println!("Invalid flag for commit");
                    return Err("Invalid Flag for Commit VC Command".to_string());
                } else {
                    // -m message here
                    // vec[4 and above] should be a commit message
                    if vec.len() == 3 {
                        println!("No commit message");
                        return Err("Commit message cannot be empty".to_string());
                    }

                    // vec[3] and above are the commit messages
                    let mut message: String = String::new();
                    for i in 3..vec.len() {
                        message.push_str(&vec[i].replace(&"\"".to_string(), &"".to_string()));
                        message.push_str(" ");
                    }

                    if message == "\"\"" {
                        println!("No commit message");
                        return Err("Commit message cannot be empty".to_string());
                    }

                    // Make sure the user has some changes to commit
                    if user.get_diffs().len() == 0 {
                        println!("No changes to commit");
                        return Err("No changes to commit".to_string());
                    }

                    let (res_node, res_commit) =
                        get_db_instance().unwrap().create_commit_and_node(
                            &message.to_string(),
                            &user.get_commands().join(":"),
                            user,
                            None,
                        )?;
                    println!("Successful commit");
                    return Ok(format!(
                        "Commit created on branch {} with hash {}",
                        res_node.branch_name, res_commit.hash
                    ));
                }
            } else {
                // commit with no message
                println!("No commit message");
                return Err("Must include a commit message".to_string());
            }
        }
        "branch" => {
            // branch (CoPilot rec: Possible flags: -d, -m, -l (list branches))
            // Needs an argument
            println!("{:?}", "branch command");
            if vec.len() < 3 {
                // error message here
                println!("Invalid Branch VC Command");
                return Err("Invalid VC Command".to_string());
            } else if !vec[2].to_string().starts_with("-") && vec.len() > 3 {
                // spaces in the branch name
                // error message here
                println!("Invalid Branch Name");
                return Err("Invalid Branch Name".to_string());
            } else {
                // using a flag that's not supposed to be used
                if vec[2].to_string().starts_with("-") && vec[2].to_string() != "-l" {
                    println!("Invalid Flag for Branch VC Command");
                    return Err("Invalid flag".to_string());
                }
                if vec[2].to_string() == "-l" {
                    // We want to return a list of branches
                    let branch_names: Vec<String> = get_db_instance()?.get_all_branch_names()?;
                    // Join the branch_names into a single comma separated string
                    let branch_names_str: String = branch_names.join(",");
                    println!("Branch command with list flag");

                    return Ok(branch_names_str);
                } else {
                    // vec[2] should be a branch name
                    // create branch
                    get_db_instance()?
                        .create_branch(&vec[2].to_string(), user)
                        .map_err(|e| e.to_string())?;

                    println!("Successful branch");
                    return Ok(format!("Branch {} created!", &vec[2]));
                }
            }
        }
        "switch_branch" => {
            // merge
            // Needs an argument
            println!("{:?}", "switch branch command");
            if vec.len() < 3 {
                // error message here
                println!("Invalid Switch Branch VC Command");
                return Err("Invalid VC Command".to_string());
            } else if vec.len() > 3 {
                // spaces in the branch name
                // error message here
                println!("Invalid Branch Name");
                return Err("Invalid Branch Name".to_string());
            } else {
                // vec[2] should be a branch name
                println!("Valid Switch Branch VC Command");
                get_db_instance()?
                    .switch_branch(&vec[2].to_string(), user)
                    .map_err(|e| e.to_string())?;
                return Ok("Valid Switch Branch Command".to_string());
            }
        }
        "log" => {
            // log (NO FLAGS OR ARGS)
            println!("{:?}", "log command");

            if vec.len() != 2 {
                // Error message here
                println!("Invalid Log VC Command");
                return Err("Invalid VC Command".to_string());
            }

            let log_results = log::log(user)?;
            let log_string: String = log_results.0;
            println!("Successful log");

            return Ok(log_string);
        }
        "revert" => {
            // revert (Needs an argument)
            println!("{:?}", "revert command");
            if vec.len() != 3 {
                // error message here
                println!("Invalid Revert VC Command");
                return Err("Invalid VC Command".to_string());
            } else {
                // vec[2] should be a commit hash
                println!("Valid Revert VC Command");
                return Ok("Valid Revert Command".to_string());
            }
        }
        "status" => {
            // status (NO FLAGS OR ARGS)
            println!("{:?}", "status command");
            if vec.len() != 2 {
                // error message here
                println!("Invalid Status VC Command");
                return Err("Invalid VC Command".to_string());
            }
            println!("Successful status");
            return Ok("Valid Status Command".to_string());
        }
        _ => {
            // error message here
            println!("Invalid VC Command");
            return Err("Invalid VC Command".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::fileio::databaseio::{create_db_instance, delete_db_instance};

    use super::*;

    #[test]
    #[serial]
    fn test_parse_vc_cmd() {
        let query = "GQL commit -m \"This is a commit message\"";
        // Create a new user on the main branch
        create_db_instance(&"gql_log_db_instance_ 1".to_string()).unwrap();
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
        create_db_instance(&"gql_log_db_instance_2".to_string()).unwrap();
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
        create_db_instance(&"TEST_DB".to_string()).unwrap();
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
        create_db_instance(&"TEST_DB".to_string()).unwrap();
        let mut user: User = User::new("test_user".to_string());
        let result = parse_vc_cmd(query, &mut user);
        delete_db_instance().unwrap();
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_parse_vc_cmd8() {
        let query = "GQL log";
        create_db_instance(&"gql_log_db_instance_3".to_string()).unwrap();

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
