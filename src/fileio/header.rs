use crate::util::dbtype::ColumnType;
pub struct Header {
    pub num_pages: u32,
    pub schema: Schema,
}

pub type SchemaCol = (String, ColumnType);
pub type Schema = Vec<SchemaCol>;

pub fn read_schema(file: String) -> Schema {
    let mut schema = Schema::new();
    schema
}

pub fn read_header(file: String) -> Header {
    todo!("read_header")
}