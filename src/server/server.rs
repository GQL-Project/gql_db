use db_connection::database_connection_server::DatabaseConnection;
use db_connection::*;
use tonic::{Request, Response, Status};

use crate::parser::parser;
use crate::server::connection::Connection;
use crate::util::convert::*;

pub mod db_connection {
    tonic::include_proto!("db_connection");
}

// Shared fields across all instances go here.
#[tonic::async_trait]
impl DatabaseConnection for Connection {
    async fn connect_db(&self, _: Request<()>) -> Result<Response<ConnectResult>, Status> {
        let id = self.new_client();
        Ok(Response::new(to_connect_result(id)))
    }

    async fn disconnect_db(&self, request: Request<ConnectResult>) -> Result<Response<()>, Status> {
        self.remove_client(request.into_inner().id);
        Ok(Response::new(()))
    }

    async fn run_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResult>, Status> {
        let request = request.into_inner();
        /* SQL Pipeline Begins Here */
        // Instead of having the result type be checked each time, it's checked once here.
        // Hence, future functions will get a Result<T, String> argument, but accessing the
        // value inside Ok is just a simple (and safe!) ?.
        let result = parser::parse(&request.query, false);
        /* Creating Result */
        match result {
            Ok(tree) => Ok(Response::new(to_query_result(vec![tree], vec![]))),
            Err(err) => Err(Status::cancelled(&err)),
        }
    }

    async fn run_update(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<UpdateResult>, Status> {
        let request = request.into_inner();
        /* SQL Pipeline Begins Here */
        let result = parser::parse(&request.query, true);
        /* Creating Result */
        match result {
            Ok(tree) => Ok(Response::new(to_update_result(tree))),
            Err(err) => Err(Status::cancelled(&err)),
        }
    }
}

// Note: unit-testing servers is tricky, so it's done in the integration tests.