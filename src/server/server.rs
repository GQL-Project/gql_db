use db_connection::database_connection_server::DatabaseConnection;
use db_connection::*;
use tonic::{Request, Response, Status};

use crate::executor::query;
use crate::fileio::databaseio::get_db_instance;
use crate::parser::parser;
use crate::server::connection::Connection;
use crate::user::userdata::*;
use crate::util::convert::*;

pub mod db_connection {
    tonic::include_proto!("db_connection");
}

// Shared fields across all instances go here.
#[tonic::async_trait]
impl DatabaseConnection for Connection {
    async fn connect_db(&self, _: Request<()>) -> Result<Response<ConnectResult>, Status> {
        let id = self.new_client().map_err(|e| Status::internal(e))?;
        Ok(Response::new(to_connect_result(id)))
    }

    async fn disconnect_db(&self, request: Request<ConnectResult>) -> Result<Response<()>, Status> {
        let connect_res: ConnectResult = request.into_inner();

        // Delete the temp branch directory in it's own scope to prevent issues when removing the client
        {
            // Get the user that is disconnecting
            let user: &mut User = self
                .get_client(&connect_res.id)
                .map_err(|e| Status::internal(e))?;

            // If the user is on a temp branch, then we need to delete it.
            if user.is_on_temp_commit() {
                get_db_instance()
                    .map_err(|e| Status::internal(e))?
                    .delete_temp_branch_directory(user)
                    .map_err(|e| Status::internal(e))?;
            }
        }
        self.remove_client(connect_res.id)
            .map_err(|e| Status::internal(e))?;
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
                // Get the user that is running the query
                let user: &mut User = self
                    .get_client(&request.id)
                    .map_err(|e| Status::internal(e))?;

                // Execute the query represented by the AST.
                let data = query::execute_query(&tree, user).map_err(|e| Status::internal(e))?;
                Ok(Response::new(to_query_result(data.0, data.1)))
            }
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
            Ok(tree) => {
                // Get the user that is running the query
                let mut user: &mut User = self
                    .get_client(&request.id)
                    .map_err(|e| Status::internal(e))?;

                // If the user is not on a temp branch, then we need to create a new one.
                if user.is_on_temp_commit() == false {
                    get_db_instance()
                        .map_err(|e| Status::internal(e))?
                        .create_temp_branch_directory(user)
                        .map_err(|e| Status::internal(e))?;
                }

                let resp = query::execute_update(&tree, user).map_err(|e| Status::internal(e))?;
                Ok(Response::new(to_update_result(resp)))
            }
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

        // Get the user that is running the query
        let mut user: &mut User = self
            .get_client(&request.id)
            .map_err(|e| Status::internal(e))?;

        /* VC Command Pipeline Begins Here */
        let result = parser::parse_vc_cmd(&request.query, &user);

        /* Creating Result */
        match result {
            Ok(value) => Ok(Response::new(to_vc_cmd_result(value))),
            Err(err) => Err(Status::cancelled(&err)),
        }
    }
}

// Integration tests go here.
#[cfg(test)]
mod tests {
    use serial_test::serial;

    // This import's needed, probably a bug in the language server.
    use super::*;
    // Tests to test async functions
    #[tokio::test]
    #[serial]
    async fn connect_db() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn disconnect_db() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        result.as_ref().unwrap();
        let result = conn
            .disconnect_db(Request::new(result.unwrap().into_inner()))
            .await;
        result.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn run_query() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let id = result.unwrap().into_inner().id;
        let result = conn
            .run_query(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "ABCD INCORRECT QUERY;".to_string(),
            }))
            .await;
        assert!(result.is_err());
        let request = ConnectResult { id };
        let result = conn.disconnect_db(Request::new(request)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn run_update() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let id = result.unwrap().into_inner().id;
        let result = conn
            .run_update(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "ABCD INCORRECT QUERY;".to_string(),
            }))
            .await;
        assert!(result.is_err());
        let request = ConnectResult { id };
        let result = conn.disconnect_db(Request::new(request)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn run_update_success() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let id = result.unwrap().into_inner().id;
        let result = conn
            .run_update(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "CREATE TABLE test (id INT);".to_string(),
            }))
            .await;
        assert!(result.is_ok());
        let request = ConnectResult { id };
        let result = conn.disconnect_db(Request::new(request)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn run_query_success() {
        let conn = Connection::new();
        let result = conn.connect_db(Request::new(())).await;
        assert!(result.is_ok());
        let id = result.unwrap().into_inner().id;
        let result = conn
            .run_update(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "CREATE TABLE test (id INT);".to_string(),
            }))
            .await;
        assert!(result.is_ok());
        let result = conn
            .run_update(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "INSERT INTO test VALUES (88);".to_string(),
            }))
            .await;
        assert!(result.is_ok());
        let result = conn
            .run_query(Request::new(super::QueryRequest {
                id: id.clone(),
                query: "SELECT * FROM test;".to_string(),
            }))
            .await;
        assert!(result.is_ok());
        let request = ConnectResult { id };
        let result = conn.disconnect_db(Request::new(request)).await;
        assert!(result.is_ok());
    }
}
