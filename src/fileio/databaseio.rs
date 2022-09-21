use std::path::Path;
use std::env;

const MAIN_BRANCH_NAME: &str = "main";

#[derive(Clone)]
pub struct Database {
    pub path: String,
    pub branch_name: String,
    // TODO: maybe add permissions here
}

impl Database {
    /// Creates a new database at the given path if one does not already exist.
    /// If it already exists, it will load that database.
    pub fn new(database_name: String) -> Result<Database, String> {
        let db_base_path = Database::get_database_base_path()?;
        // If the database base path doesn't exist, create it
        if !Path::new(&db_base_path.clone()).exists() {
            std::fs::create_dir(&db_base_path.clone()).map_err(|e| e.to_string())?;
        }
        
        // Create the database directory string
        let mut db_path = db_base_path.clone();
        db_path.push('/');
        db_path.push_str(database_name.as_str());
        db_path.push('-');
        db_path.push_str(MAIN_BRANCH_NAME);
        // If the database doesn't exist already, create a directory for it.
        if !Path::new(&db_path.clone()).exists() {
            std::fs::create_dir(&db_path).map_err(|e| e.to_string())?;
        }
        
        Ok(Database {
            path: db_path,
            branch_name: MAIN_BRANCH_NAME.to_string(), // Set branch_id to the main branch name
        })
    }


    /// Creates a new branch for the database.
    /// The branch name must not exist exist already.
    /// It returns true on success, and false on failure.
    pub fn create_branch(&mut self, branch_name: String) -> Result<bool, String> {
        // TODO: implementation
        Ok(true)
    }


    /// Switches the database to the given branch.
    /// The branch MUST exist already.
    /// It returns true on success, and false on failure.
    pub fn switch_branch(&mut self, branch_name: String) -> Result<bool, String> {
        // TODO: implementation
        Ok(true)
    }


    /// Private static method that returns the full absolute path to the databases directory within gql_db
    fn get_database_base_path() -> Result<String, String> {
        match env::current_exe() {
            Ok(path) => {
                let mut dir: String = path.canonicalize()
                    .expect("The current exe should exist")
                    .parent()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                dir.push_str("/databases");         // Append the databases directory to the path
                dir = dir.replace("\\\\?\\", ""); // remove wonkiness on Windows
                
                Ok(dir)
            }
            Err(e) => Err(e.to_string()),
        }
    }
}