use super::tableio::*;
use crate::version_control::branches::BranchNode;
use crate::version_control::commit::Commit;
use crate::version_control::{
    branch_heads::*, branches::Branches, commitfile::CommitFile, diff::Diff,
};
use crate::user::userdata::*;
use parking_lot::{ReentrantMutex, ReentrantMutexGuard};
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

// #[derive(Clone)] I'm keeping this commented. We do NOT want the database to be cloneable.
pub struct Database {
    db_path: String,           // This is the full patch to the database directory: <path>/<db_name>
    db_name: String,           // This is the name of the database (not the path)
    branch_heads: BranchHEADs, // The BranchHEADs file object for this database
    branches: Branches,        // The Branches file object for this database
    commit_file: CommitFile,   // The CommitFile object for this database
    mutex: ReentrantMutex<()>, // This is the mutex that is used to lock the database
    // TODO: maybe add permissions here
}

static mut DATABASE_INSTANCE: Option<Database> = None;

pub fn get_db_instance() -> Result<&'static mut Database, String> {
    unsafe {
        match DATABASE_INSTANCE {
            Some(ref mut db) => Ok(db),
            None => Err("Database::get_instance() Error: Database instance not set".to_owned()),
        }
    }
}

pub fn create_db_instance(database_name: &String) -> Result<(), String> {
    unsafe {
        DATABASE_INSTANCE = Some(Database::new(database_name.clone())?);
    }
    Ok(())
}

pub fn load_db_instance(database_name: &String) -> Result<(), String> {
    match Database::load_db(database_name.clone()) {
        Ok(db) => unsafe {
            DATABASE_INSTANCE = Some(db);
        },
        // Try to create a new database if one doesn't exist
        Err(_) => create_db_instance(database_name)?,
    }
    Ok(())
}

pub fn delete_db_instance() -> Result<(), String> {
    unsafe {
        match DATABASE_INSTANCE {
            Some(ref mut db) => {
                db.delete_database_dir()?;
                DATABASE_INSTANCE = None;

                return Ok(());
            }
            None => {
                return Err("Database::get_instance() Error: Database instance not set".to_owned());
            }
        }
    }
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

        // Create the branches file object
        let branches: Branches = Branches::new(&db_path.clone(), true)?;

        // Create the commit file object
        let commit_file: CommitFile = CommitFile::new(&db_path.clone(), true)?;

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
            branch_heads: branch_heads,
            branches: branches,
            commit_file: commit_file,
            mutex: ReentrantMutex::new(()),
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

        // Load the branch_heads.gql file, which holds all the branch HEADs for the database
        let branch_heads: BranchHEADs = BranchHEADs::new(&db_path.clone(), false)?;

        // Create the branches file object
        let branches: Branches = Branches::new(&db_path.clone(), false)?;

        // Create the commit file object
        let commit_file: CommitFile = CommitFile::new(&db_path.clone(), false)?;

        Ok(Database {
            db_path: db_path,
            db_name: database_name,
            branch_heads: branch_heads,
            branches: branches,
            commit_file: commit_file,
            mutex: ReentrantMutex::new(()),
        })
    }

    /// Creates a commit and a branch node in the appropriate files.
    pub fn create_commit_and_node(
        &mut self,
        diffs: &Vec<Diff>,
        commit_msg: &String,
        command: &String,
        user: &User
    ) -> Result<(BranchNode, Commit), String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let commit = self.commit_file.create_commit(
            commit_msg.to_string(),
            command.to_string(),
            diffs.clone(),
        )?;
        if self.branch_heads.get_all_branch_heads()?.len() == 0 {
            let node = self.branches.create_branch_node(
                &mut self.branch_heads,
                None,
                &user.get_current_branch_name(),
                &commit.hash,
            )?;
            return Ok((node, commit));
        }
        let prev_node = self
            .branch_heads
            .get_branch_node_from_head(&user.get_current_branch_name(), &self.branches)?;
        let node = self.branches.create_branch_node(
            &mut self.branch_heads,
            Some(&prev_node),
            &user.get_current_branch_name(),
            &commit.hash,
        )?;
        Ok((node, commit))
    }

    /// Returns the database's current branch path for a user: <path>/<db_name>/<branch_name>
    pub fn get_current_branch_path(&self, user: &User) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let branch_name: String = user.get_current_branch_name();
        let path: String = format!("{}{}{}{}{}", 
            self.db_path, 
            std::path::MAIN_SEPARATOR, 
            self.db_name, 
            DB_NAME_BRANCH_SEPARATOR,
            branch_name);
        path
    }

    /// Returns the database's current branch HEAD
    pub fn get_branch_heads_file(&self) -> &BranchHEADs {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        &self.branch_heads
    }

    pub fn get_branch_heads_file_mut(&mut self) -> &mut BranchHEADs {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        &mut self.branch_heads
    }

    /// Returns the database's branch
    pub fn get_branch_file(&self) -> &Branches {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        &self.branches
    }

    pub fn get_branch_file_mut(&mut self) -> &mut Branches {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        &mut self.branches
    }

    /// returns the database's commit file
    pub fn get_commit_file_mut(&mut self) -> &mut CommitFile {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        &mut self.commit_file
    }

    /// Returns the path to the database's deltas file: <path>/<db_name>/deltas.gql
    pub fn get_deltas_file_path(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let db_dir_path = self.get_database_path();
        // Return the deltas file path appended to the database path
        Database::append_deltas_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branches file: <path>/<db_name>/branches.gql
    pub fn get_commit_headers_file_path(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_commit_headers_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branch HEADs file: <path>/<db_name>/branch_heads.gql
    pub fn get_branches_file_path(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_branches_file_path(db_dir_path.clone())
    }

    /// Returns the path to the database's branch HEADs file: <path>/<db_name>/branch_heads.gql
    pub fn get_branch_heads_file_path(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let db_dir_path = self.get_database_path();
        // Return the branches file path appended to the database path
        Database::append_branch_heads_file_path(db_dir_path.clone())
    }

    /// Returns the file path to the table if it exists on the current branch
    pub fn get_table_path(&self, table_name: &String, user: &User) -> Result<String, String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let mut table_path: String = self.get_current_branch_path(user);
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
    pub fn get_all_table_paths(&self, user: &User) -> Result<Vec<String>, String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        let mut table_paths: Vec<String> = Vec::new();
        let branch_path: String = self.get_current_branch_path(user);
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
        // Create an empty clause to allow obtaining the mutex
        {
            // Make sure to lock the database before doing anything
            let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

            // Remove the directory and all files within it
            self.delete_database_dir()?;
        }
        // Now drop the object that the lock has been released
        // Destroy self
        drop(self);
        Ok(())
    }

    /// Deletes the directories of the database
    fn delete_database_dir(&self) -> Result<(), String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        // Remove the directory and all files within it
        std::fs::remove_dir_all(self.get_database_path()).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Creates a new branch for the database.
    /// The branch name must not exist exist already.
    /// It returns true on success, and false on failure.
    pub fn create_branch(&mut self, branch_name: &String) -> Result<(), String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

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

    /// Returns the database's name
    pub fn get_database_name(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        self.db_name.clone()
    }

    /// Returns the database's path: <path>/<db_name>
    pub fn get_database_path(&self) -> String {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

        self.db_path.clone()
    }

    /// Switches the database to the given branch.
    /// The branch MUST exist already.
    /// It returns true on success, and false on failure.
    pub fn switch_branch(&mut self, _branch_name: String) -> Result<(), String> {
        // Make sure to lock the database before doing anything
        let _lock: ReentrantMutexGuard<()> = self.mutex.lock();

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
    use crate::{
        executor::query::create_table,
        fileio::header::Schema,
        util::{
            dbtype::{Column, Value},
            row::Row,
        },
        version_control,
    };
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

        // Create a user on the main branch
        let user: User = User::new("test_user".to_string());

        // Make sure the current branch path is correct
        assert_eq!(
            new_db.get_current_branch_path(&user),
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

        // Create a user on the main branch
        let user: User = User::new("test_user".to_string());

        create_table(&"test_table".to_string(), &schema, &new_db, &user).unwrap();

        // Make sure the table path is correct
        assert_eq!(
            new_db.get_table_path(&"test_table".to_string(), &user).unwrap(),
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

        // Create a user on the main branch
        let user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        create_table(&"test_table".to_string(), &schema, &new_db, &user).unwrap();

        // Load the database
        let loaded_db: Database = Database::load_db(db_name.clone()).unwrap();

        // Make sure the database path is correct
        assert_eq!(loaded_db.get_database_path(), db_base_path.clone());

        // Make sure the current branch path is correct
        assert_eq!(
            loaded_db.get_current_branch_path(&user),
            full_path_to_branch.clone()
        );

        // Make sure the table path is correct
        assert_eq!(
            loaded_db.get_table_path(&"test_table".to_string(), &user).unwrap(),
            full_path_to_branch.clone()
                + std::path::MAIN_SEPARATOR.to_string().as_str()
                + "test_table"
                + TABLE_FILE_EXTENSION
        );

        // Delete the database
        loaded_db.delete_database().unwrap();
    }

    #[test]
    fn test_create_commit_branch_node() {
        // This tests creating a commit branch node
        let db_name = "test_create_commit_branch_node".to_string();
        let db_branch_name: String =
            db_name.clone() + &DB_NAME_BRANCH_SEPARATOR.to_string() + MAIN_BRANCH_NAME;
        let db_base_path: String = Database::get_database_base_path().unwrap()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + db_name.clone().as_str();
        let full_path_to_branch: String = db_base_path.clone()
            + std::path::MAIN_SEPARATOR.to_string().as_str()
            + &db_branch_name.clone();

        // Create the database
        create_db_instance(&db_name).unwrap();

        // Make sure database does exist now
        assert_eq!(Path::new(&db_base_path).exists(), true);

        // Create a user on the main branch
        let user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];

        let table_result = create_table(
            &"test_table".to_string(),
            &schema,
            get_db_instance().unwrap(),
            &user
        )
        .unwrap();
        let mut table = table_result.0;

        let rows: Vec<Row> = vec![
            vec![
                Value::I32(1),
                Value::String("John".to_string()),
                Value::I32(30),
            ],
            vec![
                Value::I32(2),
                Value::String("Jane".to_string()),
                Value::I32(25),
            ],
            vec![
                Value::I32(3),
                Value::String("Joe".to_string()),
                Value::I32(20),
            ],
        ];

        let mut diffs: Vec<Diff> = Vec::new();
        diffs.push(version_control::diff::Diff::TableCreate(
            table_result.1.clone(),
        ));
        let insert_diff = table.insert_rows(rows).unwrap();

        diffs.push(version_control::diff::Diff::Insert(insert_diff));

        let results = get_db_instance().unwrap()
            .create_commit_and_node(
                &diffs,
                &"commit_msg".to_string(),
                &"create table; insert rows".to_string(),
                &user
            )
            .unwrap();

        let branch_node = results.0;
        let commit = results.1;
        // Make sure commit is correct
        let fetched_commit = get_db_instance().unwrap()
            .get_commit_file_mut()
            .fetch_commit(&commit.hash)
            .unwrap();

        // compare commit and fetched commit
        assert_eq!(commit, fetched_commit);

        // Make sure branch node is correct
        let fetched_branch_node = get_db_instance().unwrap()
            .get_branch_heads_file_mut()
            .get_branch_head(&user.get_current_branch_name())
            .unwrap();

        //compare branch node and fetched branch node
        assert_eq!(
            fetched_branch_node.branch_name,
            user.get_current_branch_name()
        );

        let target_node = get_db_instance().unwrap()
            .get_branch_heads_file_mut()
            .get_branch_node_from_head(
                &fetched_branch_node.branch_name,
                get_db_instance().unwrap().get_branch_file(),
            )
            .unwrap();

        // Assert that the target node and the branch node are the same
        assert_eq!(target_node, branch_node);

        // Delete the database
        delete_db_instance().unwrap();
    }
}
