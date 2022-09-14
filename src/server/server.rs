use db_connection::database_connection_server::DatabaseConnection;
use db_connection::*;
use tonic::{Request, Response, Status};

use crate::server::connection::Connection;

pub mod db_connection {
    tonic::include_proto!("db_connection");
}

// Shared fields across all instances go here.
#[tonic::async_trait]
impl DatabaseConnection for Connection {
    async fn connect_db(&self, _: Request<()>) -> Result<Response<ConnectResult>, Status> {
        let id = self.connect_db();
        Ok(Response::new(ConnectResult { id }))
    }

    async fn disconnect_db(&self, request: Request<ConnectResult>) -> Result<Response<()>, Status> {
        self.disconnect_db(request.into_inner().id);
        Ok(Response::new(()))
    }

    async fn run_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResult>, Status> {
        let request = request.into_inner();
        let result = self.run_query(request.id, request.query);
        Ok(Response::new(result))
    }

    async fn run_update(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<UpdateResult>, Status> {
        let request = request.into_inner();
        let result = self.run_update(request.id, request.query);
        Ok(Response::new(result))
    }
}
