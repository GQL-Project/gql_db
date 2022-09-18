use crate::util::dbtype::ValueType;

use super::{header::*, pageio::*};

pub type Row = Vec<ValueType>;

pub fn read_row(schema: &Schema, page: &Page, rownum: u16) -> Resut<Row, String> {
    let mut row = Row::new();
    let size = schema_size(schema) + 1;
    let mut offset = (rownum as usize) * size + 1;
    let check: u8 = read_type(page, offset)?;
    if check == 0 {
        return None;
    }
    for (_, celltype) in schema {
        row.push(celltype.read(page, offset)?);
    }
    Some(row)
}

pub fn write_row(schema: &Schema, page: &Page, row: Row, rownum: u16) {
    
}