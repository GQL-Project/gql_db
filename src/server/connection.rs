use std::sync::{Arc, Mutex};

use crate::fileio::databaseio::{
    create_db_instance, delete_db_instance, get_db_instance, load_db_instance,
};
use crate::user::userdata::*;

#[derive(Debug, Default)]
pub struct Connection {
    pub clients: Arc<Mutex<Vec<User>>>,
}

impl Connection {
    pub fn new() -> Self {
        Self::default()
    }

    /* Client Management Methods */
    pub fn get_clients(&self) -> Vec<User> {
        self.clients.lock().unwrap().clone()
    }

    pub fn get_client(&self, id: &String) -> Result<User, String> {
        let clients = self.clients.lock().unwrap();
        for client in clients.iter() {
            if client.get_user_id() == *id {
                return Ok(client.clone());
            }
        }
        Err(format!("Client with id {} not found", id))
    }

    pub fn new_client(&self) -> Result<String, String> {
        // Generate and add a new unique client ID.
        let id = rand::random::<i64>().to_string();
        if get_db_instance().is_err() {
            load_db_instance(&"realdb.db".to_string())?;
        }
        let user: User = User::new(id.clone());
        self.clients.lock().unwrap().push(user.clone());
        Ok(id)
    }

    pub fn remove_client(&self, id: String) -> Result<(), String> {
        self.clients
            .lock()
            .unwrap()
            .retain(|x| &x.get_user_id() != &id);
        if self.clients.lock().unwrap().len() != 0 {
            Ok(())
        } else {
            delete_db_instance()
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn test_new_client() {
        let connection = Connection::new();
        let id = connection.new_client().unwrap();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0].get_user_id(), id);
    }

    #[test]
    #[serial]
    fn test_new_clients() {
        let connection = Connection::new();
        let id1 = connection.new_client().unwrap();
        let id2 = connection.new_client().unwrap();
        assert_eq!(connection.get_clients().len(), 2);
        assert_eq!(connection.get_clients()[0].get_user_id(), id1);
        assert_eq!(connection.get_clients()[1].get_user_id(), id2);
    }

    #[test]
    #[serial]
    fn test_remove_client() {
        let connection = Connection::new();
        let id = connection.new_client().unwrap();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0].get_user_id(), id);
        connection.remove_client(id.clone()).unwrap();
        assert_eq!(connection.get_clients().len(), 0);
        assert_eq!(does_contain_id(&connection, &id), false);
    }

    #[test]
    #[serial]
    fn test_remove_non_client() {
        let connection = Connection::new();
        let id = connection.new_client().unwrap();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0].get_user_id(), id);
        connection.remove_client("12345".to_string()).unwrap();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(does_contain_id(&connection, &id), true);
    }

    fn does_contain_id(connection: &Connection, id: &String) -> bool {
        let mut does_contain_id: bool = false;
        for client in connection.get_clients() {
            if client.get_user_id() == id.clone() {
                does_contain_id = true;
            }
        }
        does_contain_id
    }
}
