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

    pub fn new_client(&self) -> String {
        // Generate and add a new unique client ID.
        let id = rand::random::<i64>().to_string();
        self.clients.lock().unwrap().push(id.clone());
        id
    }

    pub fn remove_client(&self, id: String) {
        self.clients.lock().unwrap().retain(|x| x != &id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let connection = Connection::new();
        let id = connection.new_client();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0], id);
    }

    #[test]
    fn test_new_clients() {
        let connection = Connection::new();
        let id1 = connection.new_client();
        let id2 = connection.new_client();
        assert_eq!(connection.get_clients().len(), 2);
        assert_eq!(connection.get_clients()[0], id1);
        assert_eq!(connection.get_clients()[1], id2);
    }

    #[test]
    fn test_remove_client() {
        let connection = Connection::new();
        let id = connection.new_client();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0], id);
        connection.remove_client(id.clone());
        assert_eq!(connection.get_clients().len(), 0);
        assert_eq!(connection.get_clients().contains(&id), false);
    }

    #[test]
    fn test_remove_non_client() {
        let connection = Connection::new();
        let id = connection.new_client();
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients()[0], id);
        connection.remove_client("12345".to_string());
        assert_eq!(connection.get_clients().len(), 1);
        assert_eq!(connection.get_clients().contains(&id), true);
    }
}