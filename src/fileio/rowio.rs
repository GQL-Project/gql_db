use crate::util::dbtype::ValueType;

use super::{header::*, pageio::*};

pub type Row = Vec<ValueType>;

// Note that read and write rows and write to the page, not the file.
pub fn read_row(schema: &Schema, page: &Page, rownum: u16) -> Result<Option<Row>, String> {
    let mut row = Row::new();
    let mut offset = (rownum as usize) * schema_size(schema);
    let check: u8 = read_type(page, offset)?;
    if check == 0 {
        return Ok(None);
    }
    offset += 1;
    for (_, celltype) in schema {
        row.push(celltype.read(page, offset)?);
        offset += celltype.size();
    }
    Ok(Some(row))
}

pub fn write_row(schema: &Schema, page: &mut Page, row: &Row, rownum: u16) -> Result<(), String> {
    let mut offset = (rownum as usize) * schema_size(schema);
    write_type::<u8>(page, offset, 1)?;
    offset += 1;
    schema
        .iter()
        .zip(row.iter())
        .try_for_each(|((_, celltype), cell)| {
            celltype.write(cell, page, offset)?;
            offset += celltype.size();
            Ok(())
        })
}
