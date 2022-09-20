/// A parse function, that starts with a string and returns either a table for query commands
/// or a string for 
pub fn execute(ast: &String, update: bool) -> Result<String, String> {
    if ast.len() == 0 {
        return Err("Empty AST".to_string());
    }

    Ok("0".to_string())
}