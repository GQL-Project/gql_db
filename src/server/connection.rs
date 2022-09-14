use super::server::db_connection::cell_value::CellType::*;
use super::server::db_connection::*;
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

    pub fn run_query(&self, id: String, query: String) -> QueryResult {
        // TODO: Make wrapper functions to make <Vec<String>>, Vec<<String>> into this format.
        QueryResult {
            column_names: vec![String::from("id"), String::from("name")],
            row_values: vec![RowValue {
                cell_values: vec![CellValue {
                    cell_type: Some(ColString {
                        0: String::from("hi"),
                    }),
                }],
            }],
        }
    }

    pub fn run_update(&self, id: String, query: String) -> UpdateResult {
        UpdateResult {
            success: true,
            message: query,
        }
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
