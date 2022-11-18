use colored::Colorize;
use rpassword::read_password;
use std::io::{self, Write};
use std::string::String;
use tonic::transport::Channel;
use tonic::Request;

use crate::client::result_parse;
use crate::server::server::db_connection::database_connection_client::DatabaseConnectionClient;
use crate::server::server::db_connection::{ConnectResult, LoginRequest, QueryRequest};

const GQL_PROMPT: &str = "GQL> ";
const DEFAULT_IP: &str = "[::1]";
const DEFAULT_PORT: &str = "50051";
const DEFAULT_USERNAME: &str = "admin";
const DEFAULT_PASSWORD: &str = "admin";
const DEFAULT_REGISTER: bool = false;

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Query for IP address and port of server
    let mut connection = attempt_connection().await?;
    let mut copy = connection.clone();

    // Ctrl-C handler, using the copy of the connection
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        copy.0
            .disconnect_db(Request::new(copy.1.clone()))
            .await
            .unwrap();
        println!(
            "{}",
            format!("\nSuccessfully disconnected from database").green()
        );
        std::process::exit(0);
    });

    let client: &mut DatabaseConnectionClient<Channel> = &mut connection.0;
    let response: &mut ConnectResult = &mut connection.1;

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
            if command.starts_with("exit")
                || command.starts_with("GQL ")
                || command.starts_with("gql ")
            {
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
        } else if command
            .to_lowercase()
            .replace("(", "") // Ignore parenthesis while checking for keywords
            .starts_with("select ")
        {
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

// Wrapper, to automatically handle disconnecting from the server when the program exits
#[derive(Clone)]
struct Connection(DatabaseConnectionClient<Channel>, ConnectResult);

// Indefinitely loops until a successful connection is made
async fn attempt_connection() -> Result<Connection, Box<dyn std::error::Error>> {
    let mut client: DatabaseConnectionClient<Channel>;
    let mut request: LoginRequest;

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

        /* Query for register */
        print!("{}", "Register? (y/n)> ");
        let create;
        loop {
            io::stdout().flush()?;
            let mut register: String = String::new();
            // store user input
            io::stdin().read_line(&mut register)?;
            register = register.replace("\n", "").trim().to_string();
            if register.is_empty() {
                create = DEFAULT_REGISTER;
                break;
            } else if register.to_lowercase() == "y" {
                create = true;
                break;
            } else if register.to_lowercase() == "n" {
                create = false;
                break;
            } else {
                println!("Invalid input. Please enter 'y' or 'n'.");
            }
        }

        /* Query for username */
        print!("{}", "Enter Username> ");
        io::stdout().flush()?;
        let mut username: String = String::new();
        // store user input
        io::stdin().read_line(&mut username)?;
        username = username.replace("\n", "").trim().to_string();
        if username.is_empty() {
            username = DEFAULT_USERNAME.to_string();
        }

        /* Query for password */
        print!("{}", "Enter Password> ");
        io::stdout().flush()?;
        // store user input
        let mut password: String = read_password().unwrap();
        password = password.replace("\n", "").trim().to_string();
        if password.is_empty() {
            password = DEFAULT_PASSWORD.to_string();
        }

        request = LoginRequest {
            username,
            password,
            create,
        };

        // Attempt to connect to server
        let conn_str: String = format!("http://{}:{}", ip_address, port);
        match DatabaseConnectionClient::connect(conn_str.clone()).await {
            Ok(db_client) => {
                client = db_client;
                let response = client.connect_db(request).await;
                if response.is_ok() {
                    // Print a success msg
                    print!(
                        "{}",
                        format!("Successfully Connected to Database at {}\n", conn_str)
                            .green()
                            .to_string()
                    );
                    io::stdout().flush()?;
                    return Ok(Connection {
                        0: client,
                        1: response?.into_inner(),
                    });
                } else {
                    println!(
                        "{}",
                        format!("Error logging in: {}", response.unwrap_err().message())
                            .red()
                            .to_string()
                    );
                }
            }
            Err(_) => {
                // Start over if connection fails
                print!(
                    "{}",
                    format!(
                        "Error: Unable to connect to database at: {}\n",
                        &conn_str.clone()
                    )
                    .red()
                    .to_string()
                );
                io::stdout().flush()?;
            }
        }
    }
}
