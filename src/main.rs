use clap::Parser;
use server::connection::Connection;
use server::server::db_connection::database_connection_server::DatabaseConnectionServer;
use tonic::transport::Server;

mod client;
mod executor;
mod fileio;
mod parser;
mod server;
mod util;
mod version_control;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "None")]
struct Args {
    #[clap(short, long)]
    client: bool,

    #[clap(short, long)]
    gui: bool,

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
    if args.client {
        client::client::main().await?;
    } else {
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
    }
    Ok(())
}
