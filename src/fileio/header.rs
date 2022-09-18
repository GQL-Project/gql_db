use super::pageio::*;
use crate::util::dbtype::ColumnType;
pub struct Header {
    pub num_pages: u32,
    pub schema: Schema,
}

pub type SchemaCol = (String, ColumnType);
pub type Schema = Vec<SchemaCol>;

pub fn read_schema(page: &Page) -> Schema {
    let mut schema = Schema::new();
    let num_cols = read_type::<u8>(&page, 4);
    schema
}

// Header reads must not fail, so we use unwrap() to panic on error
pub fn read_header(file: String) -> Header {
    let buf = read_page(0, &file).unwrap();
    let num_pages = read_type::<u32>(&buf, 0);
    let schema = read_schema(&buf);
    Header { num_pages, schema }
}
