use crate::util::dbtype::Value;

use super::{header::*, pageio::*};

pub type Row = Vec<Value>;

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
    // This looks complicated, but all it's doing is just zipping
    // the schema with the row, and then writing each cell.
    schema
        .iter()
        .zip(row.iter())
        .try_for_each(|((_, celltype), cell)| {
            celltype.write(cell, page, offset)?;
            offset += celltype.size();
            Ok(())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::dbtype::{Column, Value};

    #[test]
    fn test_page_rows() {
        let schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let mut page = [0u8; PAGE_SIZE];
        let row1 = vec![
            Value::I32(1),
            Value::String("John".to_string()),
            Value::I32(20),
        ];
        let row2 = vec![
            Value::I32(2),
            Value::String("Jane".to_string()),
            Value::I32(21),
        ];
        write_row(&schema, &mut page, &row1, 0).unwrap();
        write_row(&schema, &mut page, &row2, 1).unwrap();
        assert_eq!(read_row(&schema, &page, 0).unwrap(), Some(row1));
        assert_eq!(read_row(&schema, &page, 1).unwrap(), Some(row2));
        assert_eq!(read_row(&schema, &page, 2).unwrap(), None);
    }
}
