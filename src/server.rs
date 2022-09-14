use tonic::{Request, Response, Status};

use connection::database_server::Database;
use connection::*;

pub mod connection {
    tonic::include_proto!("connection");
}

// Shared fields across all instances go here.
#[derive(Debug, Default)]
pub struct Connection {}

#[tonic::async_trait]
impl Database for Connection {
    async fn connect_db(&self, _: Request<()>) -> Result<Response<ConnectResult>, Status> {
        let message = ConnectResult { id: 10.to_string() };
        Ok(Response::new(message))
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
