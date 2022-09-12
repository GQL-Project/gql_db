// This helps us to build the proto files for the rust code.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile(&["proto/todo.proto"], &["proto"])
        .unwrap();
    Ok(())
}