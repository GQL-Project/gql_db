use connection::database_connection_client::DatabaseConnectionClient;
use connection::{ConnectResult, QueryRequest};
use std::io::{self, BufRead, Write};
use std::string::String;
use tonic::Request;

use crate::client::client::connection::QueryResult;
use crate::util::dbtype::Value;

pub mod connection {
    tonic::include_proto!("db_connection");
}

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(());
    let response = client.connect_db(request).await?.into_inner();

    loop {
        print!("{}", "GQL> ");
        io::stdout().flush()?;

        let mut lines = io::stdin().lock().lines();
        let mut command = String::new();

        while let Some(line) = lines.next() {
            let last_input = line.unwrap();

            // add a new line once user_input starts storing user input
            if command.len() > 0 {
                command.push_str("\n");
            }

            // store user input
            command.push_str(&last_input);

            // stop reading if there's a semi colon
            if last_input.contains(";")
            {
                break;
            }

            // makes sure these are in the first line (VC commands and exit)
            if (last_input.contains("exit") || last_input.starts_with("GQL ")) && command == last_input {
                break;
            }

            print!("{}", ">");
            io::stdout().flush()?;
        }

        // string manipulation to get rid of \n and ;
        command = command.replace(";", "");
        command = command.replace("\n", " ");

        let request = QueryRequest {
            id: String::from(response.id.clone()),
            query: String::from(command.clone()),
        };

        // need to type "exit" to exit
        if command == "exit" {
            client
                .disconnect_db(Request::new(ConnectResult { id: request.id }))
                .await?;
            break;
        }

        // GQL
        if command.starts_with("GQL ") {
            client.run_version_control_command(Request::new(request.clone())).await?;
        } else {
            client.run_query(Request::new(request.clone())).await?;
        }

        let result = QueryResult {
            column_names: vec!["Name".to_string(), "Age".to_string(), "Height".to_string(), "Weight".to_string()],
            row_values: vec![],
        };
    }
    Ok(())
}