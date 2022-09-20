use connection::database_connection_client::DatabaseConnectionClient;
use connection::{ConnectResult, QueryRequest};
use tonic::Request;
use std::io::{self, BufRead, Write};

pub mod connection {
    tonic::include_proto!("db_connection");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(());
    let response = client.connect_db(request).await?.into_inner();

    print!("{}", "GQL> ");
    io::stdout().flush(); // Yellow line under is a little sketch, but it works

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

        print!("{}", ">");
        io::stdout().flush();
    }

    command = command.replace(";", "");
    command = command.replace("\n", " ");

    //println!("This is the command {}", command);

    let request = QueryRequest {
        id: String::from(response.id.clone()),
        query: String::from(command),
    };

    let get_response = client.run_query(Request::new(request.clone())).await?;
    println!("{:?}", get_response.into_inner().column_names);

    //println!("{:?}", &response.id);
    //println!("{:?}", get_response.into_inner().row_values);

    client
        .disconnect_db(Request::new(ConnectResult { id: request.id }))
        .await?;
    Ok(())
}
