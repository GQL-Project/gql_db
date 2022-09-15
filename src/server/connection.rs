use crate::util::valuetype::*;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct Connection {
    pub clients: Arc<Mutex<Vec<String>>>,
}

impl Connection {
    pub fn new() -> Self {
        Self::default()
    }

    /* Connection Wrappers */
    pub fn connect_db(&self) -> String {
        let id = rand::random::<i64>().to_string();
        self.add_client(id.clone());
        id
    }

    pub fn disconnect_db(&self, id: String) {
        self.remove_client(id);
    }

    pub fn run_query(&self, id: String, query: String) -> (Vec<String>, Vec<Vec<ValueType>>) {
        // TODO: Make wrapper functions to make <Vec<String>>, Vec<<String>> into this format.
        (vec![String::from("id"), String::from("name")], vec![vec![ValueType::ValI32(1), ValueType::ValString(String::from("test"))]])
    }

    pub fn run_update(&self, id: String, query: String) -> (bool, String) {
        (true, "Success".to_string())
    }

    /* Client Management Methods */
    pub fn get_clients(&self) -> Vec<String> {
        self.clients.lock().unwrap().clone()
    }

    pub fn add_client(&self, id: String) {
        self.clients.lock().unwrap().push(id);
    }

    pub fn remove_client(&self, id: String) {
        self.clients.lock().unwrap().retain(|x| x != &id);
    }
}
