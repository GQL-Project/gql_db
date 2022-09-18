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
    // Rather than doing ::<u8>, this is cleaner
    let num_cols: u8 = read_type(&page, 4);
    let mut offset = 5;
    for _ in 0..num_cols {
        let typeid: u16 = read_type(&page, offset);
        let colname = read_string(&page, offset + 2, 50);
        schema.push((colname, ColumnType::from(typeid)));
        offset += 34;
    }
    schema
}

pub fn write_schema(page: &mut Page, schema: &Schema) {
    write_type::<u8>(page, 4, schema.len() as u8);
    let mut offset = 5;
    for (colname, coltype) in schema {
        write_type::<u16>(page, offset, coltype.to_u16());
        write_string(page, offset + 2, colname, 50);
        offset += 34;
    }
}

// Header reads must not fail, so we use unwrap() to panic on error
// Not sure if it's better to have a file or the page passed in, 
// this can be changed later on.
pub fn read_header(file: String) -> Header {
    let buf = read_page(0, &file).unwrap();
    let num_pages = read_type::<u32>(&buf, 0);
    let schema = read_schema(&buf);
    Header { num_pages, schema }
}

pub fn write_header(file: String, header: &Header) {
    let mut buf = [0u8; PAGE_SIZE];
    write_type::<u32>(&mut buf, 0, header.num_pages);
    write_schema(&mut buf, &header.schema);
    write_page(0, &file, &buf).unwrap();
}