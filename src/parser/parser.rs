/// A parse function, that starts with a string and returns an AST representation of the query.
/// If an error happens, an Err(msg) is returned.
pub fn parse(query: &str, _update: bool) -> Result<String, String> {
    if query.len() == 0 {
        return Err("Empty query".to_string());
    }
    Ok(query.to_string())
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
                    println!("{:?}", "Invalid VC Command");
                    return Err("Invalid Flag for Commit VC Command".to_string());
                } else {
                    // -m message here
                    // vec[4 and above] should be a commit message
                    println!("{:?}", "Commit with message");
                }
            } else if vec.len() == 2 {
                // commit with no message
                println!("{:?}", "Commit with no message");
            }
        }
        "branch" => {
            // branch (CoPilot rec: Possible flags: -d, -m)
            // Needs an argument
            println!("{:?}", "branch command");
            if vec.len() < 3 { 
                // error message here
                println!("{:?}", "Invalid VC Command");
                return Err("Invalid VC Command".to_string());
            } else if vec.len() > 3 { // spaces in the branch name
                // error message here
                println!("{:?}", "Invalid Branch Name");
                return Err("Invalid Branch Name".to_string());        
            } else {
                // vec[2] should be a branch name
                println!("{:?}", "Valid Branch Command");
            }
        }
        "switch branch" => {
            // merge
            // Needs an argument
            println!("{:?}", "switch branch command");
            if vec.len() < 3 { 
                // error message here
                println!("{:?}", "Invalid VC Command");
                return Err("Invalid VC Command".to_string());
            } else if vec.len() > 3 { // spaces in the branch name
                // error message here
                println!("{:?}", "Invalid Branch Name");   
                return Err("Invalid Branch Name".to_string());          
            } else {
                // vec[2] should be a branch name
                println!("{:?}", "Valid Switch Branch Command");
            }
        }
        "log" => {
            // log (NO FLAGS OR ARGS)
            println!("{:?}", "log command");

            if vec.len() != 2 {
                // Error message here
                println!("{:?}", "Invalid VC Command");
                return Err("Invalid VC Command".to_string());
            }
        }
        "revert" => {
            // revert (Needs an argument)
            println!("{:?}", "revert command");
            if vec.len() != 3 {
                // error message here
                println!("{:?}", "Invalid VC Command");
                return Err("Invalid VC Command".to_string());
            } else {
                // vec[2] should be a commit hash
            }
        }
        "status" => {
            // status (NO FLAGS OR ARGS)
            println!("{:?}", "status command");
            if vec.len() != 2 {
                // error message here
                println!("{:?}", "Invalid VC Command");
                return Err("Invalid VC Command".to_string());
            }
        }
        _ => {
            // error message here
            println!("{:?}", "Invalid VC Command");
            return Err("Invalid VC Command".to_string());
        }
    }
    Ok("1".to_string()) // temporary, need to fix it somehow
}
