use crate::{
    fileio::{
        databaseio,
        header::{schema_size, write_header, Header, Schema},
        pageio::*,
        tableio::Table,
    },
    util::{
        dbtype::{Column, Value},
        row::{Row, RowInfo},
    },
};

#[derive(Debug, Clone)]
pub struct UserCred {
    pub user_id: String,
    pub password: String,
    pub is_admin: bool,
}

pub struct UserCREDs {
    filepath: String,
    user_creds_table: Table,
}

impl UserCred {
    pub fn new(user_id: String, password: String, is_admin: bool) -> UserCred {
        UserCred {
            user_id,
            password,
            is_admin,
        }
    }

    pub fn get_user_id(&self) -> String {
        self.user_id.clone()
    }

    pub fn get_password(&self) -> String {
        self.password.clone()
    }

    pub fn is_admin(&self) -> bool {
        self.is_admin
    }

    pub fn set_is_admin(&mut self, is_admin: bool) {
        self.is_admin = is_admin;
    }

    pub fn set_password(&mut self, password: String) {
        self.password = password;
    }

    pub fn set_user_id(&mut self, user_id: String) {
        self.user_id = user_id;
    }
}

impl UserCREDs {
    pub fn new(dir_path: &String, create_file: bool) -> Result<UserCREDs, String> {
        // Get filepath info
        let branch_filename: String = format!(
            "{}{}",
            databaseio::USER_CREDS_FILE_NAME.to_string(),
            databaseio::USER_CREDS_FILE_EXTENSION.to_string()
        );
        let mut filepath: String = format!(
            "{}{}{}",
            dir_path,
            std::path::MAIN_SEPARATOR,
            branch_filename
        );
        // If the directory path is not given, use the current directory
        if dir_path.len() == 0 {
            filepath = branch_filename;
        }

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("user_id".to_string(), Column::String(32)),
                ("password".to_string(), Column::String(32)),
                ("is_admin".to_string(), Column::Bool),
            ];
            let header = Header {
                num_pages: 2,
                schema,
            };
            write_header(&filepath, &header)?;

            // Write a blank page to the table
            let page = [0u8; PAGE_SIZE];
            write_page(1, &filepath, &page)?;
        }

        Ok(UserCREDs {
            filepath: filepath.clone(),
            user_creds_table: Table::new(
                &dir_path.clone(),
                &databaseio::USER_CREDS_FILE_NAME.to_string(),
                Some(&databaseio::USER_CREDS_FILE_EXTENSION.to_string()),
            )?,
        })
    }
}
