use connection::database_connection_client::DatabaseConnectionClient;
use connection::QueryRequest;

pub mod connection {
    tonic::include_proto!("db_connection");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseConnectionClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(());

    let response = client.connect_db(request).await?;

    println!("{:?}", response.into_inner().id);

    let create_request = tonic::Request::new(QueryRequest {
        id: String::from("10"),
        query: String::from("select * from tables"),
    });

    let create_response = client.run_query(create_request).await?;

    println!("{:?}", create_response.into_inner().column_names);

    Ok(())
}
