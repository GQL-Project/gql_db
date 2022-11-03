use clap::Parser;
use server::connection::Connection;
use server::server::db_connection::database_connection_server::DatabaseConnectionServer;
use tonic::transport::Server;

use crate::util::bench;

mod client;
mod executor;
mod fileio;
mod parser;
mod server;
mod user;
mod util;
mod version_control;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = "None")]
struct Args {
    // Run the terminal client
    #[clap(short, long)]
    client: bool,

    // IP Address
    #[clap(short, long, default_value = "[::1]")]
    ip: String,

    /// Port Number
    #[clap(short, long, default_value = "50051")]
    port: u16,

    /// Use Demo Database
    #[clap(short, long)]
    demo: bool,
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
        if args.demo {
            bench::create_demo_db("demo");
        }
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
