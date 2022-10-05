use std::io::{self, BufRead, Write};
use std::string::String;
use colored::Colorize;
use tonic::Request;

use crate::client::result_parse;
use crate::server::server::db_connection::database_connection_client::DatabaseConnectionClient;
use crate::server::server::db_connection::{ConnectResult, QueryRequest};

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(());
    let response = client.connect_db(request).await?.into_inner();

    loop {
        print!("{}", "GQL> ");
        io::stdout().flush()?;

        let mut command = String::new();

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
        let success = format!("{} ", "GQL>".green());
        let error = format!("{} ", "GQL>".red());
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
