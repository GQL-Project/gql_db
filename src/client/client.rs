use colored::Colorize;
use tonic::transport::Channel;
use std::io::{self, Write};
use std::string::String;
use tonic::Request;

use crate::client::result_parse;
use crate::server::server::db_connection::database_connection_client::DatabaseConnectionClient;
use crate::server::server::db_connection::{ConnectResult, QueryRequest};

const GQL_PROMPT: &str = "GQL> ";
const DEFAULT_IP: &str = "[::1]";
const DEFAULT_PORT: &str = "50051";

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Query for IP address and port of server
    let mut client: DatabaseConnectionClient<Channel> = query_for_ip_port().await?;

    let request: Request<()> = tonic::Request::new(());
    let response: ConnectResult = client.connect_db(request).await?.into_inner();

    loop {
        print!("{}", &GQL_PROMPT.to_string());
        io::stdout().flush()?;

        let mut command: String = String::new();

        loop {
            // add a new line once user_input starts storing user input
            let mut last_input = String::new();
            if command.len() > 0 {
                command.push_str("\n");
            }

            // store user input
            io::stdin().read_line(&mut last_input)?;
            command.push_str(&last_input);

            // makes sure these are in the first line (VC commands and exit)
            if command.starts_with("exit") || command.starts_with("GQL ") {
                break;
            }

            // stop reading if there's a semi colon
            if last_input.contains(";") {
                break;
            }

            print!("{}", "   > ");
            io::stdout().flush()?;
        }

        // string manipulation to get rid of \n and ;
        command = command.replace(";", "");
        command = command.replace("\n", " ");

        let request = QueryRequest {
            id: String::from(&response.id),
            query: String::from(&command),
        };

        // need to type "exit" to exit
        if command.to_lowercase().starts_with("exit") {
            client
                .disconnect_db(Request::new(ConnectResult { id: request.id }))
                .await?;
            break;
        }

        // GQL
        let success = format!("{}", GQL_PROMPT.to_string().green());
        let error = format!("{}", GQL_PROMPT.to_string().red());
        if command.to_lowercase().starts_with("gql ") {
            let result = client
                .run_version_control_command(Request::new(request))
                .await;
            if result.is_ok() {
                let get_response = result.unwrap().into_inner();
                println!("{}{}", success, get_response.message);
            } else {
                println!("{}{}", error, result.unwrap_err().message());
            }
        } else if command.to_lowercase().starts_with("select ") {
            let result = client.run_query(Request::new(request)).await;
            if result.is_ok() {
                // parses through the result and prints the table
                result_parse::result_parse(result.unwrap().into_inner())?;
            } else {
                println!("{}{}", error, result.unwrap_err().message());
            }
        } else {
            let result = client.run_update(Request::new(request)).await;
            if result.is_ok() {
                let get_response = result.unwrap().into_inner();
                println!("{}{}", success, get_response.message);
            } else {
                println!("{}{}", error, result.unwrap_err().message());
            }
        }
    }
    Ok(())
}

// Indefinitely loops until a successful connection is made
async fn query_for_ip_port() -> Result<DatabaseConnectionClient<Channel>, Box<dyn std::error::Error>> {
    let client: DatabaseConnectionClient<Channel>;

    // Loop until we successfully connect
    loop {
        /* Query for IP address */
        print!("{}", "Enter IP Address of Database> ");
        io::stdout().flush()?;
        let mut ip_address: String = String::new();
        // store user input
        io::stdin().read_line(&mut ip_address)?;
        ip_address = ip_address.replace("\n", "").trim().to_string();
        // Set default IP address if none is given
        if ip_address.is_empty() {
            ip_address = DEFAULT_IP.to_string();
        }

        /* Query for port */
        print!("{}", "Enter Port of Database> ");
        io::stdout().flush()?;
        let mut port: String = String::new();
        // store user input
        io::stdin().read_line(&mut port)?;
        port = port.replace("\n", "").trim().to_string();
        // Set default port if none is given
        if port.is_empty() {
            port = DEFAULT_PORT.to_string();
        }

        // Attempt to connect to server
        let conn_str: String = format!("http://{}:{}", ip_address, port);
        match DatabaseConnectionClient::connect(conn_str.clone()).await {
            Ok(db_client) => {
                client = db_client;
                // Print a success msg
                print!(
                    "{}", 
                    format!(
                        "Successfully Connected to Database at {}\n", 
                        conn_str
                    ).green().to_string()
                );
                io::stdout().flush()?;
                break;
            }
            Err(_) => {
                // Start over if connection fails
                print!(
                    "{}",
                    format!(
                        "Error: Unable to connect to database at: {}\n",
                        &conn_str.clone()
                    ).red().to_string()
                );
                io::stdout().flush()?;
            }
        }
    }
    Ok(client)
}
