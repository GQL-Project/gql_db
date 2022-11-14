use std::sync::{Arc, Mutex, MutexGuard};

use crate::fileio::databaseio::{get_db_instance, load_db_instance};
use crate::user::usercreds::UserCred;
use crate::user::userdata::*;

#[derive(Debug, Default)]
pub struct Connection {
    pub clients: Arc<Mutex<Vec<User>>>,
}

impl Connection {
    /* Client Management Methods */
    /// Gets a mutable reference to the client with the given ID.
    pub fn get_client<'a>(&self, id: &String) -> Result<&'a mut User, String> {
        let mut clients: MutexGuard<Vec<User>> = self.clients.lock().unwrap();

        // Get a mutable pointer to the clients vector
        let ptr: *mut User = clients.as_mut_ptr();

        // Get the index of the client with the given id
        let index: usize = clients
            .iter()
            .position(|client| client.get_user_id() == *id)
            .ok_or("Client not found")?;

        // Get a mutable reference to the client
        let client: &mut User = unsafe { &mut *ptr.add(index) };

        // Return the client
        Ok(client)
    }

    pub fn new_client(
        &self,
        username: String,
        password: String,
        create: bool,
    ) -> Result<String, String> {
        // Generate and add a new unique client ID.
        if get_db_instance().is_err() {
            load_db_instance(&"realdb.db".to_string())?;
        }
        let user_creds_instance = get_db_instance()?.get_user_creds_file_mut();
        if !user_creds_instance.does_user_exist("admin".to_string())? {
            user_creds_instance.create_user(&UserCred {
                username: "admin".to_string(),
                password: "admin".to_string(),
            })?;
        }
        if create {
            if user_creds_instance.does_user_exist(username.clone())? {
                return Err("User already exists".to_string());
            } else {
                user_creds_instance.create_user(&UserCred {
                    username: username.clone(),
                    password: password.clone(),
                })?;
            }
        } else {
            if !user_creds_instance.does_user_exist(username.clone())? {
                return Err("User does not exist".to_string());
            } else {
                if user_creds_instance.get_user(&username)?.password != password {
                    return Err("Incorrect password".to_string());
                }
            }
        }

        let user: User = User::new(username.clone());
        self.clients.lock().unwrap().push(user.clone());
        Ok(username)
    }

    pub fn remove_client(&self, id: String) -> Result<(), String> {
        self.clients
            .lock()
            .unwrap()
            .retain(|x| &x.get_user_id() != &id);
        Ok(())
    }

    pub fn get_all_branches_clients_are_connected_to(&self) -> Vec<String> {
        let mut branches: Vec<String> = Vec::new();
        for client in self.clients.lock().unwrap().iter() {
            let branch_name: String = client.get_current_branch_name();
            if !branches.contains(&branch_name) {
                branches.push(branch_name.clone());
            }
        }
        branches
    }

    /// Gets the list of clients, but cloned in a non-mutable way.
    pub fn get_clients_readonly(&self) -> Vec<User> {
        self.clients.lock().unwrap().clone()
    }
}

// When we close the Connection (database is exiting), we can clean up the database.
impl Drop for Connection {
    fn drop(&mut self) {
        let clients = self.get_clients_readonly();
        for mut client in clients {
            if client.is_on_temp_commit() {
                get_db_instance()
                    .unwrap()
                    .delete_temp_branch_directory(&mut client)
                    .expect("Failed to delete temp branch directory");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fileio::{databaseio::*, header::Schema},
        util::dbtype::Column,
        util::{bench::fcreate_db_instance, dbtype::Value, row::*},
        version_control::diff::{Diff, InsertDiff},
    };
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_new_client() {
        let connection = Connection::default();
        // Create a new database instance
        fcreate_db_instance(&"test_new_client");

        let id = connection
            .new_client("admin".to_string(), "admin".to_string(), false)
            .unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 1);
        assert_eq!(connection.get_clients_readonly()[0].get_user_id(), id);

        // Delete new database instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_new_clients() {
        let connection = Connection::default();
        // Create a new database instance
        fcreate_db_instance(&"test_new_clients");

        let id1 = connection
            .new_client("admin".to_string(), "admin".to_string(), false)
            .unwrap();
        let id2 = connection
            .new_client("admin".to_string(), "admin".to_string(), false)
            .unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 2);
        assert_eq!(connection.get_clients_readonly()[0].get_user_id(), id1);
        assert_eq!(connection.get_clients_readonly()[1].get_user_id(), id2);

        // Delete new database instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_remove_client() {
        let connection = Connection::default();
        // Create a new database instance
        fcreate_db_instance(&"test_new_clients");

        let id = connection
            .new_client("admin".to_string(), "admin".to_string(), false)
            .unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 1);
        assert_eq!(connection.get_clients_readonly()[0].get_user_id(), id);
        connection.remove_client(id.clone()).unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 0);
        assert_eq!(does_contain_id(&connection, &id), false);

        // Delete new database instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_remove_non_client() {
        let connection = Connection::default();
        // Create a new database instance
        fcreate_db_instance(&"test_new_clients");

        let id = connection
            .new_client("admin".to_string(), "admin".to_string(), false)
            .unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 1);
        assert_eq!(connection.get_clients_readonly()[0].get_user_id(), id);
        connection.remove_client("12345".to_string()).unwrap();
        assert_eq!(connection.get_clients_readonly().len(), 1);
        assert_eq!(does_contain_id(&connection, &id), true);

        // Delete new database instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_mutability_of_get_client() {
        let connection: Connection = Connection::default();
        // Create a new database instance
        fcreate_db_instance(&"test_new_clients");

        // Create a scope for appending the diff
        {
            // Create then retrieve the client
            let id: String = connection
                .new_client("admin".to_string(), "admin".to_string(), false)
                .unwrap();
            let client: &mut User = connection.get_client(&id).unwrap();

            let schema: Schema = vec![
                ("id".to_string(), Column::I32),
                ("name".to_string(), Column::String(50)),
                ("age".to_string(), Column::I32),
            ];
            client.append_diff(&Diff::Insert(InsertDiff {
                table_name: "test".to_string(),
                schema,
                rows: vec![RowInfo {
                    row: vec![
                        Value::I32(1),
                        Value::String("test".to_string()),
                        Value::I32(1),
                    ],
                    rownum: 0,
                    pagenum: 0,
                }],
            }));
        }

        // Check the array and make sure the diff was added
        let id: String = connection.get_clients_readonly()[0].get_user_id();
        let client: &User = connection.get_client(&id).unwrap();
        assert_eq!(client.get_diffs().len(), 1);

        match client.get_diffs()[0] {
            Diff::Insert(ref diff) => {
                assert_eq!(diff.table_name, "test");
                assert_eq!(diff.schema.len(), 3);
                assert_eq!(diff.rows.len(), 1);
            }
            _ => assert!(false), // The diff should be an insert
        }

        // Delete new database instance
        delete_db_instance().unwrap();
    }

    fn does_contain_id(connection: &Connection, id: &String) -> bool {
        let mut does_contain_id: bool = false;
        for client in connection.get_clients_readonly() {
            if client.get_user_id() == id.clone() {
                does_contain_id = true;
            }
        }
        does_contain_id
    }
}
