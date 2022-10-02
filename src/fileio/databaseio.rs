use super::tableio::*;
use crate::version_control::{branch_heads::*, branches::BranchNode};
use glob::glob;
use std::env;
use std::path::Path;

// Branch Constants
pub const MAIN_BRANCH_NAME: &str = "main";
pub const DB_NAME_BRANCH_SEPARATOR: char = '-';

// Deltas File Constants
pub const DELTAS_FILE_NAME: &str = "deltas";
pub const DELTAS_FILE_EXTENSION: &str = ".gql";

// Commit Headers File Constants
pub const COMMIT_HEADERS_FILE_NAME: &str = "commitheaders";
pub const COMMIT_HEADERS_FILE_EXTENSION: &str = ".gql";

// Branches File Constants
pub const BRANCHES_FILE_NAME: &str = "branches";
pub const BRANCHES_FILE_EXTENSION: &str = ".gql";

// Branch HEADs File Constants
pub const BRANCH_HEADS_FILE_NAME: &str = "branch_heads";
pub const BRANCH_HEADS_FILE_EXTENSION: &str = ".gql";

#[derive(Clone)]
pub struct Database {
    db_path: String, // This is the full patch to the database directory: <path>/<db_name>
    db_name: String, // This is the name of the database (not the path)
    branch_path: String, // This is the full path to the database branch directory: <path>/<db_name>/<branch_name>
    branch_name: String, // The name of the branch that this database is currently on
    branch_heads: BranchHEADs, // The BranchHEADs file object for this database
    connected_clients: Vec<String>, // The list of clients that are currently connected to this database at this branch
                                    // TODO: maybe add permissions here
}

impl Database {
    /// Creates a new database at the given path.
    /// It will return an error if the database already exists.
    pub fn new(database_name: String) -> Result<Database, String> {
        let db_base_path = Database::get_database_base_path()?;
        // If the databases base path './databases/' doesn't exist, create it
        if !Path::new(&db_base_path.clone()).exists() {
            std::fs::create_dir(&db_base_path.clone())
                .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;
        }

        // Create the database directory './databases/<database_name>'
        let mut db_path = db_base_path.clone();
        db_path.push(std::path::MAIN_SEPARATOR);
        db_path.push_str(database_name.as_str());
        // If the database already exists, return an error
        if Path::new(&db_path.clone()).exists() {
            return Err("Database::new() Error: Database already exists".to_owned());
        }
        std::fs::create_dir(&db_path)
            .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;

        // Create the deltas file, which holds the deltas for the commits
        // './databases/<database_name>/deltas.gql'
        let deltas_file_path = Database::append_deltas_file_path(db_path.clone());
        std::fs::File::create(&deltas_file_path)
            .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;

        // Create the commit headers file, which holds all the headers for the commits
        // './databases/<database_name>/commitheaders.gql'
        let commit_headers_file_path = Database::append_commit_headers_file_path(db_path.clone());
        std::fs::File::create(&commit_headers_file_path)
            .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;

        // Create the branches file, which holds all the branches for the database
        // './databases/<database_name>/branches.gql'
        let branches_file_path = Database::append_branches_file_path(db_path.clone());
        std::fs::File::create(&branches_file_path)
            .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;

        // Create the branch_heads file, which holds all the branch HEADs for the database
        // './databases/<database_name>/branch_heads.gql'
        let branch_heads: BranchHEADs = BranchHEADs::new(&db_path.clone(), true)?;

        // Now create the directory for the main branch
        // './databases/<database_name>/<database_name>-<branch_name>/'
        let mut main_branch_path = db_path.clone();
        main_branch_path.push(std::path::MAIN_SEPARATOR);
        main_branch_path.push_str(database_name.as_str());
        main_branch_path.push(DB_NAME_BRANCH_SEPARATOR);
        main_branch_path.push_str(MAIN_BRANCH_NAME);
        // Create a directory for the main branch database.
        std::fs::create_dir(&main_branch_path)
            .map_err(|e| "Database::new() Error: ".to_owned() + &e.to_string())?;

        // TODO: construct the main branch database from the diffs file

        Ok(Database {
            db_path: db_path,
            db_name: database_name,
            branch_path: main_branch_path,
            branch_name: MAIN_BRANCH_NAME.to_string(), // Set branch_id to the main branch name
            branch_heads: branch_heads,
            connected_clients: Vec::new(),
        })
    }

    /// Opens an existing database at the given path.
    /// It will return an error if the database doesn't exist.
    pub fn load_db(database_name: String) -> Result<Database, String> {
        let db_base_path = Database::get_database_base_path()?;

        // Create the database directory if needed './databases/<database_name>'
        let mut db_path = db_base_path.clone();
        db_path.push(std::path::MAIN_SEPARATOR);
        db_path.push_str(database_name.as_str());
        // If the database doesn't already exist, return an error
        if !Path::new(&db_path.clone()).exists() {
            return Err("Database::load_db() Error: Database does not exist".to_owned());
        }

        // Now get the directory for the main branch
        // './databases/<database_name>/<database_name>-<branch_name>/'
        let mut main_branch_path = db_path.clone();
        main_branch_path.push(std::path::MAIN_SEPARATOR);
        main_branch_path.push_str(database_name.as_str());
        main_branch_path.push(DB_NAME_BRANCH_SEPARATOR);
        main_branch_path.push_str(MAIN_BRANCH_NAME);

        // Load the branch_heads.gql file, which holds all the branch HEADs for the database
        let branch_heads: BranchHEADs = BranchHEADs::new(&db_path.clone(), false)?;

        Ok(Database {
            db_path: db_path,
            db_name: database_name,
            branch_path: main_branch_path,
            branch_name: MAIN_BRANCH_NAME.to_string(), // Set branch_id to the main branch name
            branch_heads: branch_heads,
            connected_clients: Vec::new(),
        })
    }

    /// Returns the database's name
    pub fn get_database_name(&self) -> String {
        self.db_name.clone()
    }

    /// Returns the database's current branch name
    pub fn get_current_branch_name(&self) -> String {
        self.branch_name.clone()
    }

    /// Returns the database's path: <path>/<db_name>
    pub fn get_database_path(&self) -> String {
        self.db_path.clone()
    }

    /// Returns the database's current branch path: <path>/<db_name>/<branch_name>
    pub fn get_current_branch_path(&self) -> String {
        self.branch_path.clone()
    }

    /// Returns the path to the database's deltas file: <path>/<db_name>/deltas.gql
    pub fn get_deltas_file_path(&self) -> String {
        let db_dir_path = self.get_database_path();
        // Return the deltas file path appended to the database path
        Database::append_deltas_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branches file: <path>/<db_name>/branches.gql
    pub fn get_commit_headers_file_path(&self) -> String {
        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_commit_headers_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branch HEADs file: <path>/<db_name>/branch_heads.gql
    pub fn get_branches_file_path(&self) -> String {
        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_branches_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branch HEADs file: <path>/<db_name>/branch_heads.gql
    pub fn get_branch_heads_file_path(&self) -> String {
        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_branch_heads_file_path(db_dir_path.clone())
    }

    /// Returns the clients that are connected to the database at this branch
    pub fn get_connected_clients(&self) -> Vec<String> {
        self.connected_clients.clone()
    }

    /// Returns the file path to the table if it exists on the current branch
    pub fn get_table_path(&self, table_name: &String) -> Result<String, String> {
        let mut table_path = self.get_current_branch_path();
        table_path.push(std::path::MAIN_SEPARATOR);
        table_path.push_str(table_name.as_str());
        table_path.push_str(TABLE_FILE_EXTENSION);
        if Path::new(&table_path.clone()).exists() {
            Ok(table_path)
        } else {
            Err("Error: Table does not exist".to_string())
        }
    }

    /// Returns a list of file paths to all the tables on the current branch
    pub fn get_all_table_paths(&self) -> Result<Vec<String>, String> {
        let mut table_paths: Vec<String> = Vec::new();
        let branch_path: String = self.get_current_branch_path();
        let mut table_path: String = branch_path.clone();
        table_path.push(std::path::MAIN_SEPARATOR);
        table_path.push_str("*");
        table_path.push_str(TABLE_FILE_EXTENSION);
        for entry in glob(&table_path)
            .map_err(|e| "Database::get_all_table_paths() Error: ".to_owned() + &e.to_string())?
        {
            match entry {
                Ok(path) => {
                    table_paths.push(path.to_str().unwrap().to_string());
                }
                Err(e) => {
                    return Err(
                        "Database::get_all_table_paths() Error: ".to_owned() + &e.to_string()
                    );
                }
            }
        }
        Ok(table_paths)
    }

    /// Deletes the database at the given path.
    /// It also deletes the database object.
    pub fn delete_database(self) -> Result<(), String> {
        // Remove the directory and all files within it
        std::fs::remove_dir_all(self.get_database_path()).map_err(|e| e.to_string())?;
        // Destroy self
        drop(self);
        Ok(())
    }

    /// Creates a new branch for the database.
    /// The branch name must not exist exist already.
    /// It returns true on success, and false on failure.
    pub fn create_branch(&mut self, branch_name: &String) -> Result<(), String> {
        // Check if the branch name already exists. We want to verify that it doesn't exist already.
        match self.branch_heads.get_branch_head(branch_name) {
            Ok(_) => {
                return Err("Database::create_branch() Error: Branch already exists".to_owned());
            }
            Err(_) => {} // Do nothing, we expect this error
        }

        //TODO: Ryan User Story 18

        Ok(())
    }

    /// Switches the database to the given branch.
    /// The branch MUST exist already.
    /// It returns true on success, and false on failure.
    pub fn switch_branch(&mut self, _branch_name: String) -> Result<(), String> {
        // TODO: implementation
        Ok(())
    }

    /*********************************************************************************************/
    /*                                       Private Methods                                     */
    /*********************************************************************************************/

    /// Private static method that returns the full absolute path to the databases directory
    fn get_database_base_path() -> Result<String, String> {
        match env::current_exe() {
            Ok(path) => {
                let mut dir: String = path
                    .canonicalize()
                    .expect("The current exe should exist")
                    .parent()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();

                dir.push_str("/databases"); // Append the databases directory to the path
                dir = dir.replace("\\\\?\\", ""); // remove wonkiness on Windows

                Ok(dir)
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// Private static method that appends the deltas file path to the database_path
    fn append_deltas_file_path(database_path: String) -> String {
        let mut deltas_file_path = database_path;
        deltas_file_path.push(std::path::MAIN_SEPARATOR);
        deltas_file_path.push_str(DELTAS_FILE_NAME);
        deltas_file_path.push_str(DELTAS_FILE_EXTENSION);
        deltas_file_path
    }

    /// Private static method that appends the commit_headers file path to the database_path
    fn append_commit_headers_file_path(database_path: String) -> String {
        let mut commit_headers_file_path = database_path;
        commit_headers_file_path.push(std::path::MAIN_SEPARATOR);
        commit_headers_file_path.push_str(COMMIT_HEADERS_FILE_NAME);
        commit_headers_file_path.push_str(COMMIT_HEADERS_FILE_EXTENSION);
        commit_headers_file_path
    }

    /// Private static method that appends the branches file path to the database_path
    fn append_branches_file_path(database_path: String) -> String {
        let mut branches_file_path = database_path;
        branches_file_path.push(std::path::MAIN_SEPARATOR);
        branches_file_path.push_str(BRANCHES_FILE_NAME);
        branches_file_path.push_str(BRANCHES_FILE_EXTENSION);
        branches_file_path
    }

    /// Private static method that appends the branch heads file path to the database_path
    fn append_branch_heads_file_path(database_path: String) -> String {
        let mut branch_heads_file_path = database_path;
        branch_heads_file_path.push(std::path::MAIN_SEPARATOR);
        branch_heads_file_path.push_str(BRANCH_HEADS_FILE_NAME);
        branch_heads_file_path.push_str(BRANCH_HEADS_FILE_EXTENSION);
        branch_heads_file_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{executor::query::create_table, fileio::header::Schema, util::dbtype::Column};
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_db_creation() {
        let db_name = "test_creation_db".to_string();
        let db_base_path: String = Database::get_database_base_path().unwrap()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + db_name.clone().as_str();

        // Make sure database does not already exist
        assert_eq!(
            Path::new(&db_base_path).exists(),
            false,
            "Database {} already exists, cannot run test",
            db_base_path
        );

        // Create the database
        let new_db: Database = Database::new(db_name.clone()).unwrap();

        // Make sure database does exist now
        assert_eq!(Path::new(&db_base_path).exists(), true);

        // Delete the database
        new_db.delete_database().unwrap();

        // Make sure database does not exist anymore
        assert_eq!(Path::new(&db_base_path).exists(), false);
    }

    #[test]
    #[serial]
    fn test_db_file_path_getters() {
        let db_name = "test_file_path_getters_db".to_string();
        let db_branch_name: String =
            db_name.clone() + &DB_NAME_BRANCH_SEPARATOR.to_string() + MAIN_BRANCH_NAME;
        let db_base_path: String = Database::get_database_base_path().unwrap()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + db_name.clone().as_str();
        let full_path_to_branch: String = db_base_path.clone()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + &db_branch_name.clone();

        // Make sure database does not already exist
        assert_eq!(
            Path::new(&db_base_path).exists(),
            false,
            "Database {} already exists, cannot run test",
            db_base_path
        );

        // Create the database
        let new_db: Database = Database::new(db_name.clone()).unwrap();

        // Make sure database does exist now
        assert_eq!(Path::new(&db_base_path).exists(), true);

        // Make sure the database path is correct
        assert_eq!(new_db.get_database_path(), db_base_path.clone());

        // Make sure the current branch path is correct
        assert_eq!(
            new_db.get_current_branch_path(),
            full_path_to_branch.clone()
        );

        // Make sure the deltas file path is correct
        assert_eq!(
            new_db.get_deltas_file_path(),
            db_base_path.clone()
                + std::path::MAIN_SEPARATOR.to_string().as_str()
                + DELTAS_FILE_NAME
                + DELTAS_FILE_EXTENSION
        );

        // Make sure the commit headers file path is correct
        assert_eq!(
            new_db.get_commit_headers_file_path(),
            db_base_path.clone()
                + std::path::MAIN_SEPARATOR.to_string().as_str()
                + COMMIT_HEADERS_FILE_NAME
                + COMMIT_HEADERS_FILE_EXTENSION
        );

        // Delete the database
        new_db.delete_database().unwrap();

        // Make sure database does not exist anymore
        assert_eq!(Path::new(&db_base_path).exists(), false);
    }

    #[test]
    #[serial]
    fn test_get_table_path() {
        // This tests creating a table within the database and that it is created in the correct directory
        let db_name = "test_get_table_path_db".to_string();
        let db_branch_name: String =
            db_name.clone() + &DB_NAME_BRANCH_SEPARATOR.to_string() + MAIN_BRANCH_NAME;
        let db_base_path: String = Database::get_database_base_path().unwrap()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + db_name.clone().as_str();
        let full_path_to_branch: String = db_base_path.clone()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + &db_branch_name.clone();

        // Make sure database does not already exist
        assert_eq!(
            Path::new(&db_base_path).exists(),
            false,
            "Database {} already exists, cannot run test",
            db_base_path
        );

        // Create the database
        let new_db: Database = Database::new(db_name.clone()).unwrap();

        // Make sure database does exist now
        assert_eq!(Path::new(&db_base_path).exists(), true);

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        create_table(&"test_table".to_string(), &schema, &new_db).unwrap();

        // Make sure the table path is correct
        assert_eq!(
            new_db.get_table_path(&"test_table".to_string()).unwrap(),
            full_path_to_branch.clone()
                + std::path::MAIN_SEPARATOR.to_string().as_str()
                + "test_table"
                + TABLE_FILE_EXTENSION
        );

        // Delete the database
        new_db.delete_database().unwrap();

        // Make sure database does not exist anymore
        assert_eq!(Path::new(&db_base_path).exists(), false);
    }

    #[test]
    fn test_load_db() {
        // This tests creating a database, saving it, and then loading it back in
        let db_name = "test_load_db".to_string();
        let db_branch_name: String =
            db_name.clone() + &DB_NAME_BRANCH_SEPARATOR.to_string() + MAIN_BRANCH_NAME;
        let db_base_path: String = Database::get_database_base_path().unwrap()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + db_name.clone().as_str();
        let full_path_to_branch: String = db_base_path.clone()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + &db_branch_name.clone();

        // Create the database
        let new_db: Database = Database::new(db_name.clone()).unwrap();

        // Make sure database does exist now
        assert_eq!(Path::new(&db_base_path).exists(), true);

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        create_table(&"test_table".to_string(), &schema, &new_db).unwrap();

        // Load the database
        let loaded_db: Database = Database::load_db(db_name.clone()).unwrap();

        // Make sure the database path is correct
        assert_eq!(loaded_db.get_database_path(), db_base_path.clone());

        // Make sure the current branch path is correct
        assert_eq!(
            loaded_db.get_current_branch_path(),
            full_path_to_branch.clone()
        );

        // Make sure the table path is correct
        assert_eq!(
            loaded_db.get_table_path(&"test_table".to_string()).unwrap(),
            full_path_to_branch.clone()
                + std::path::MAIN_SEPARATOR.to_string().as_str()
                + "test_table"
                + TABLE_FILE_EXTENSION
        );

        // Delete the database
        loaded_db.delete_database().unwrap();
    }
}
