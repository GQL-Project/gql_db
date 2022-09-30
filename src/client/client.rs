use std::io::{self, BufRead, Write};
use std::string::String;
use tonic::Request;

use crate::server::server::db_connection::database_connection_client::DatabaseConnectionClient;
use crate::server::server::db_connection::{ConnectResult, QueryRequest, QueryResult};
use crate::util::convert::to_row_value;
use crate::util::dbtype::Value;

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
            if last_input.contains(";") {
                break;
            }

            // makes sure these are in the first line (VC commands and exit)
            if (last_input.contains("exit") || last_input.starts_with("GQL "))
                && command == last_input
            {
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
            client
                .run_version_control_command(Request::new(request.clone()))
                .await?;
        } else {
            client.run_query(Request::new(request.clone())).await?;
        }

        let result = QueryResult {
            column_names: vec![
                "Name".to_string(),
                "Age".to_string(),
                "Height".to_string(),
                "Weight".to_string(),
                "Location".to_string(),
            ],
            row_values: vec![
                to_row_value(vec![
                    Value::String("John Adams".to_string()),
                    Value::I32(20),
                    Value::Float(5.5),
                    Value::Float(150.0),
                    Value::String("New York".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Jane Washington".to_string()),
                    Value::I32(21),
                    Value::Float(5.3),
                    Value::Float(130.0),
                    Value::String("Boston".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("George Jefferson".to_string()),
                    Value::I32(22),
                    Value::Float(5.7),
                    Value::Float(160.0),
                    Value::String("San Francisco".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Thomas Jefferson".to_string()),
                    Value::I32(23),
                    Value::Float(5.7),
                    Value::Float(160.0),
                    Value::String("New York".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Abraham Lincoln".to_string()),
                    Value::I32(24),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Chicago".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Andrew Jackson".to_string()),
                    Value::I32(25),
                    Value::Float(5.8),
                    Value::Float(170.0),
                    Value::String("Charleston".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Ulysses S. Grant".to_string()),
                    Value::I32(26),
                    Value::Float(6.0),
                    Value::Float(190.0),
                    Value::String("Washington DC".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Rutherford B. Hayes".to_string()),
                    Value::I32(27),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Indianapolis".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("James Garfield".to_string()),
                    Value::I32(28),
                    Value::Float(5.9),
                    Value::Float(180.0),
                    Value::String("Cleveland".to_string()),
                ]),
                to_row_value(vec![
                    Value::String("Chester A. Arthur".to_string()),
                    Value::I32(29),
                    Value::Float(5.8),
                    Value::Float(170.0),
                    Value::String("El Paso".to_string()),
                ]),
            ],
        };
    }
    Ok(())
}
