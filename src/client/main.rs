use connection::database_connection_client::DatabaseConnectionClient;
use connection::{ConnectResult, QueryRequest};
use std::io::{self, BufRead, Write};
use std::string::String;
use tonic::Request;

pub mod connection {
    tonic::include_proto!("db_connection");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

        //let get_response;
        // GQL
        if command.starts_with("GQL ") {
            client.run_version_control_command(Request::new(request.clone())).await?;
        } else {
            client.run_query(Request::new(request.clone())).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use connection::database_connection_client::DatabaseConnectionClient;
    use connection::{ConnectResult, QueryRequest};
    use std::string::String;
    use tonic::Request;

    #[tokio::test]
    async fn connect_db() {
        let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await.unwrap();
        let request = tonic::Request::new(());
        let response = client.connect_db(request).await.unwrap().into_inner();
        let request = ConnectResult { id: response.id };
        let _response = client.disconnect_db(Request::new(request)).await.unwrap();
    }

    #[tokio::test]
    async fn run_query() {
        let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await.unwrap();
        let request = tonic::Request::new(());
        let response = client.connect_db(request).await.unwrap().into_inner();
        let request = QueryRequest {
            id: String::from(response.id.clone()),
            query: String::from("SELECT * FROM test_table;".to_string()),
        };
        let _response = client.run_query(Request::new(request.clone())).await.unwrap();
        let request = ConnectResult { id: request.id };
        let __response = client.disconnect_db(Request::new(request)).await.unwrap();
    }

    #[tokio::test]
    async fn run_version_control_command() {
        let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await.unwrap();
        let request = tonic::Request::new(());
        let response = client.connect_db(request).await.unwrap().into_inner();
        let request = QueryRequest {
            id: String::from(response.id.clone()),
            query: String::from("GQL log".to_string()),
        };
        let _response = client
            .run_version_control_command(Request::new(request.clone()))
            .await
            .unwrap();
        let request = ConnectResult { id: request.id };
        let __response = client.disconnect_db(Request::new(request)).await.unwrap();
    }
    
}