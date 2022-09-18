use std::io::Error;

use super::pageio::*;
use crate::util::dbtype::Column;
pub struct Header {
    pub num_pages: u32,
    pub schema: Schema,
}

pub type SchemaCol = (String, Column);
pub type Schema = Vec<SchemaCol>;

pub fn read_schema(page: &Page) -> Result<Schema, String> {
    let mut schema = Schema::new();
    // Rather than doing ::<u8>, this is cleaner
    let num_cols: u8 = read_type(&page, 4)?;
    let mut offset = 5;
    for _ in 0..num_cols {
        let typeid: u16 = read_type(&page, offset)?;
        let colname = read_string(&page, offset + 2, 50)?;
        schema.push((colname, Column::decode_type(typeid)));
        offset += 34;
    }
    Ok(schema)
}

pub fn write_schema(page: &mut Page, schema: &Schema) -> Result<(), String> {
    write_type::<u8>(page, 4, schema.len() as u8)?;
    let mut offset = 5;
    for (colname, coltype) in schema {
        write_type::<u16>(page, offset, coltype.encode_type())?;
        write_string(page, offset + 2, colname, 50)?;
        offset += 34;
    }
    Ok(())
}


// Not sure if it's better to have a file or the page passed in,
// this can be changed later on.
pub fn read_header(file: String) -> Result<Header, String> {
    let buf = read_page(0, &file).map_err(map_error)?;
    let num_pages = read_type::<u32>(&buf, 0)?;
    let schema = read_schema(&buf)?;
    Ok(Header { num_pages, schema })
}

pub fn write_header(file: String, header: &Header) -> Result<(), String> {
    let mut buf = [0u8; PAGE_SIZE];
    write_type::<u32>(&mut buf, 0, header.num_pages)?;
    write_schema(&mut buf, &header.schema)?;
    write_page(0, &file, &buf).map_err(map_error)?;
    Ok(())
}

pub fn schema_size(schema: &Schema) -> usize {
    let mut size = 1; // 1 byte for the check byte
    for (_, coltype) in schema {
        size += coltype.size();
    }
    size
}

fn map_error(err: Error) -> String {
    format!("IO Error: {}", err)
}
