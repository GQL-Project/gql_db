use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// A parse function, that starts with a string and returns an AST representation of the query.
/// If an error happens, an Err(msg) is returned.
pub fn parse(query: &str, update: bool) -> Result<String, String> {
    if query.len() == 0 {
        return Err("Empty query".to_string());
    }

    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, query);

    println!("AST: {:?}", ast);
    Ok(query.to_string())
}

/// This method parses a version control command's query string into the individual components.
pub fn parse_vc_cmd(query: &str) -> Result<String, String> {
    if query.len() == 0 {
        return Err("Empty VC Command query".to_string());
    }
    Ok("1".to_string())
}
