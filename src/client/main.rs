use connection::database_connection_client::DatabaseConnectionClient;
use connection::{ConnectResult, QueryRequest};
use tonic::Request;

pub mod connection {
    tonic::include_proto!("db_connection");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(());
    let response = client.connect_db(request).await?.into_inner();

    println!("{:?}", &response.id);
    let request = QueryRequest {
        id: String::from(response.id.clone()),
        query: String::from("select * from tables"),
    };
    
    let get_response = client.run_query(Request::new(request.clone())).await?;
    println!("{:?}", get_response.into_inner().column_names);

    client
        .disconnect_db(Request::new(ConnectResult { id: request.id }))
        .await?;
    Ok(())
}
