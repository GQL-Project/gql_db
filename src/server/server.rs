use db_connection::database_connection_server::DatabaseConnection;
use db_connection::*;
use tonic::{Request, Response, Status};

use crate::parser::parser;
use crate::server::connection::Connection;
use crate::util::convert::*;
use crate::executor::query;

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
            Ok(tree) => {
                // Execute the query represented by the AST.
                query::execute(&tree.clone(), false).map_err(|e| Status::internal(e))?;

                Ok(Response::new(to_query_result(vec![tree], vec![])))
            },
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

    /// This is a method that gets run every time the client runs a version control command.
    /// It automatically parses the command, executes it, and returns the result.
    async fn run_version_control_command(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<VersionControlResult>, Status> {
        let request = request.into_inner();
        /* VC Command Pipeline Begins Here */
        let result = parser::parse_vc_cmd(&request.query);

        /* Creating Result */
        match result {
            Ok(tree) => Ok(Response::new(to_vc_cmd_result(tree))),
            Err(err) => Err(Status::cancelled(&err)),
        }
    }
}

// Integration tests go here.
#[cfg(test)]
mod tests {
    // This import's needed, probably a bug in the language server.
    use super::*;
    // Tests to test async functions
    #[tokio::test]
    async fn connect_db() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn disconnect_db() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let result = conn
            .disconnect_db(Request::new(result.unwrap().into_inner()))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_query() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let id = result.unwrap().into_inner().id;
        let result = conn
            .run_query(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "SELECT * FROM test_table;".to_string(),
            }))
            .await;
        assert!(result.is_ok());
        let request = ConnectResult { id };
        let result = conn.disconnect_db(Request::new(request)).await;
        assert!(result.is_ok());
    }
}
