use std::collections::HashMap;

use crate::{
    fileio::{
        databaseio,
        header::{write_header, Header},
        pageio::*,
        tableio::Table,
    },
    util::{
        dbtype::{Column, Value},
        row::Row,
    },
};

#[derive(Debug, Clone)]
pub struct UserCred {
    pub username: String,
    pub password: String,
}

pub struct UserCREDs {
    user_creds_table: Table,
}

impl UserCREDs {
    /// Creates a new UserCREDs object to store the user creds for the database.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: &String, create_file: bool) -> Result<UserCREDs, String> {
        // Get filepath info
        let usercreds_filename: String = format!(
            "{}{}",
            databaseio::USER_CREDS_FILE_NAME.to_string(),
            databaseio::USER_CREDS_FILE_EXTENSION.to_string()
        );
        let mut filepath: String = format!(
            "{}{}{}",
            dir_path,
            std::path::MAIN_SEPARATOR,
            usercreds_filename
        );
        // If the directory path is not given, use the current directory
        if dir_path.len() == 0 {
            filepath = usercreds_filename;
        }

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("username".to_string(), Column::String(32)),
                ("password".to_string(), Column::String(225)),
            ];
            // TODO: With some more work, we could possibly add some indexes to find the user from the username index
            let header = Header {
                num_pages: 2,
                schema,
                index_top_level_pages: HashMap::new(),
            };
            write_header(&filepath, &header)?;

            // Write a blank page to the table
            let page = [0u8; PAGE_SIZE];
            write_page(1, &filepath, &page, PageType::Data)?;
        }

        Ok(UserCREDs {
            user_creds_table: Table::new(
                &dir_path.clone(),
                &databaseio::USER_CREDS_FILE_NAME.to_string(),
                Some(&databaseio::USER_CREDS_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    /// Takes in a user id and returns the corresponding user cred.
    /// If the user id does not exist, returns an error.
    pub fn get_user(&mut self, username: &String) -> Result<UserCred, String> {
        let user_creds: Vec<UserCred> = self.get_all_user_creds()?;

        for user_cred in user_creds {
            if user_cred.username == *username {
                return Ok(user_cred);
            }
        }

        return Err(format!("User '{}' not present in user CREDs file", username).to_string());
    }

    /// Returns a list of all users on the database
    pub fn get_all_usernames(&mut self) -> Result<Vec<String>, String> {
        let mut usernames: Vec<String> = Vec::new();

        for row_info in self.user_creds_table.by_ref().into_iter().clone() {
            let row: Row = row_info.row;

            // Get the username
            match row.get(0) {
                Some(Value::String(username)) => usernames.push(username.to_string()),
                _ => return Err("Error: User ID not found".to_string()),
            }
        }
        Ok(usernames)
    }

    /// Returns if the user exists in the database
    pub fn does_user_exist(&mut self, username: String) -> Result<bool, String> {
        let usernames = self.get_all_usernames()?;
        for id in usernames {
            if id == username {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Read the user creds file and return a vector of UserCred objects
    pub fn get_all_user_creds(&mut self) -> Result<Vec<UserCred>, String> {
        let mut user_creds: Vec<UserCred> = Vec::new();

        for row_info in self.user_creds_table.by_ref().into_iter().clone() {
            let row: Row = row_info.row;

            let username: String;
            let password: String;

            // Get the username
            match row.get(0) {
                Some(Value::String(username_f)) => username = username_f.to_string(),
                _ => return Err("Error: User id not found".to_string()),
            }

            // Get the password
            match row.get(1) {
                Some(Value::String(password_f)) => password = password_f.to_string(),
                _ => return Err("Error: Password not found".to_string()),
            }

            let usercred: UserCred = UserCred {
                username: username,
                password: password,
            };

            user_creds.push(usercred);
        }

        Ok(user_creds)
    }

    /// Writes a new user to the user creds file.
    /// Returns an error if a user with the given username already exists.
    pub fn create_user(&mut self, user_cred: &UserCred) -> Result<(), String> {
        // Make sure that a user doesn't already have the same username
        let user_creds: Vec<UserCred> = self.get_all_user_creds()?;
        for user in user_creds {
            if user.username == user_cred.username {
                return Err("Error: Username already exists".to_string());
            }
        }

        let rows: Vec<Vec<Value>> = vec![
            // Just make one new row
            vec![
                Value::String(user_cred.username.clone()),
                Value::String(user_cred.password.clone()),
            ],
        ];
        self.user_creds_table.insert_rows(rows)?;
        Ok(())
    }
}
