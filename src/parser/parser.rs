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

    println!("AST: {:?}", ast);
    ast.map_err(|e| e.to_string())
}

/// This method parses a version control command's query string into the individual components.
/// Format "GQL <command> <flags> <args>"
pub fn parse_vc_cmd(query: &str) -> Result<String, String> {
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
                    return Err("Invalid Flag for Commit VC Command".to_string());
                } else {
                    // -m message here
                    // vec[4 and above] should be a commit message
                    return Ok("Commit with message".to_string());
                }
            } else {
                // commit with no message
                return Ok("Commit with no message".to_string());
            }
        }
        "branch" => {
            // branch (CoPilot rec: Possible flags: -d, -m)
            // Needs an argument
            println!("{:?}", "branch command");
            if vec.len() < 3 {
                // error message here
                return Err("Invalid VC Command".to_string());
            } else if vec.len() > 3 {
                // spaces in the branch name
                // error message here
                return Err("Invalid Branch Name".to_string());
            } else {
                // vec[2] should be a branch name
                return Ok("Valid Branch Command".to_string());
            }
        }
        "switch_branch" => {
            // merge
            // Needs an argument
            println!("{:?}", "switch branch command");
            if vec.len() < 3 {
                // error message here
                return Err("Invalid VC Command".to_string());
            } else if vec.len() > 3 {
                // spaces in the branch name
                // error message here
                return Err("Invalid Branch Name".to_string());
            } else {
                // vec[2] should be a branch name
                return Ok("Valid Switch Branch Command".to_string());
            }
        }
        "log" => {
            // log (NO FLAGS OR ARGS)
            println!("{:?}", "log command");

            if vec.len() != 2 {
                // Error message here
                return Err("Invalid VC Command".to_string());
            }

            log::log()?;

            return Ok("Valid Log Command".to_string());
        }
        "revert" => {
            // revert (Needs an argument)
            println!("{:?}", "revert command");
            if vec.len() != 3 {
                // error message here
                return Err("Invalid VC Command".to_string());
            } else {
                // vec[2] should be a commit hash
                return Ok("Valid Revert Command".to_string());
            }
        }
        "status" => {
            // status (NO FLAGS OR ARGS)
            println!("{:?}", "status command");
            if vec.len() != 2 {
                // error message here
                return Err("Invalid VC Command".to_string());
            }
            return Ok("Valid Status Command".to_string());
        }
        _ => {
            // error message here
            return Err("Invalid VC Command".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vc_cmd() {
        let query = "GQL commit -m \"This is a commit message\"";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd2() {
        let query = "GQL commit";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd3() {
        let query = "GQL branch";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vc_cmd4() {
        let query = "GQL branch branch_name";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd5() {
        let query = "GQL branch branch name";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vc_cmd6() {
        let query = "GQL switch_branch branch_name";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd7() {
        let query = "GQL switch_branch branch name";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vc_cmd8() {
        let query = "GQL log";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd9() {
        let query = "GQL log -m";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vc_cmd10() {
        let query = "GQL revert commit_hash";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd11() {
        let query = "GQL revert";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vc_cmd12() {
        let query = "GQL status";
        let result = parse_vc_cmd(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vc_cmd13() {
        let query = "GQL status -m";
        let result = parse_vc_cmd(query);
        assert!(result.is_err());
    }
}
