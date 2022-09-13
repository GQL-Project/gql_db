use crate::server::connection::database_server::DatabaseServer;
use server::Connection;
use tonic::transport::Server;

mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let db_service = Connection::default();

    Server::builder()
        .add_service(DatabaseServer::new(db_service))
        .serve(addr)
        .await?;

    Ok(())
}
