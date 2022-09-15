use server::connection::Connection;
use server::server::db_connection::database_connection_server::DatabaseConnectionServer;
use tonic::transport::Server;

mod server;
mod util;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let db_service = Connection::default();

    Server::builder()
        .add_service(DatabaseConnectionServer::new(db_service))
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl C");
            println!("Shutting down GQL Server");
        })
        .await?;

    Ok(())
}
