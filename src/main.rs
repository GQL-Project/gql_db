use server::connection::Connection;
use server::server::db_connection::database_connection_server::DatabaseConnectionServer;
use tonic::transport::Server;
use clap::Parser;

mod server;
mod util;
mod parser;
mod fileio;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "None")]
struct Args {
    // IP Address 
    #[clap(short, long, default_value = "[::1]")]
    ip: String,

    /// Port Number
    #[clap(short, long, default_value = "50051")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.ip, args.port).parse().unwrap();
    let db_service = Connection::default();

    println!("GQL Server Started on address: {}", addr);
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
