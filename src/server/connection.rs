use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct Connection {
    pub clients: Arc<Mutex<Vec<String>>>,
}

impl Connection {
    pub fn new() -> Self {
        Self::default()
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
