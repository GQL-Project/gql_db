fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile(&["proto/connection.proto"], &["proto"])
        .unwrap();
    Ok(())
}
