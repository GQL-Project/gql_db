use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use connection::database_server::Database;
use connection::*;

pub mod connection {
    tonic::include_proto!("connection");
}

// Shared fields across all instances go here.
#[derive(Debug, Default)]
pub struct Connection {
    clients: Mutex<Vec<String>>,
}

#[tonic::async_trait]
impl Database for Connection {
    async fn connect_db(&self, _: Request<()>) -> Result<Response<ConnectResult>, Status> {
        let id = rand::random::<i64>().to_string();
        self.clients.lock().await.push(String::from(&id));
        let message = ConnectResult { id };
        Ok(Response::new(message))
    }

    async fn disconnect_db(&self, request: Request<ConnectResult>) -> Result<Response<()>, Status> {
        let id = request.into_inner().id;
        self.clients.lock().await.retain(|x| x != &id);
        Ok(Response::new(()))
    }

    async fn run_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResult>, Status> {
        let payload = request.into_inner();
        let message = QueryResult {
            column_names: [payload.id, payload.query].to_vec(),
            row_values: [].to_vec(),
        };
        Ok(Response::new(message))
    }

    async fn run_update(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<UpdateResult>, Status> {
        let payload = request.into_inner();
        let message = UpdateResult {
            success: true,
            message: payload.id,
        };
        Ok(Response::new(message))
    }
}
