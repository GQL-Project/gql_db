use super::{header::*, pageio::*};
use crate::util::row::Row;

// Here, instead of returning an Error if the row is not found, we return None
pub fn read_row(schema: &Schema, page: &Page, rownum: u16) -> Option<Row> {
    let mut row = Row::new();
    let mut offset = (rownum as usize) * schema_size(schema);
    let check: u8 = read_type(page, offset).ok()?;
    if check == 0 {
        return None;
    }
    assert!(check == 1, "Malformed Row");
    offset += 1;
    for (_, celltype) in schema {
        row.push(celltype.read(page, offset).ok()?);
        offset += celltype.size();
    }
    Some(row)
}

/// This checks if a row is present in the page.
/// If it is, it returns true.
/// If it is not, it returns false.
/// If the rownum is beyond the end of the page, it returns an error.
pub fn is_row_present(schema: &Schema, page: &Page, rownum: u16) -> Result<bool, String> {
    let offset = (rownum as usize) * schema_size(schema);
    // Check if rownum is beyond the end of the page
    check_bounds(offset, schema_size(schema))?;
    // Check if the row is present
    let check: u8 = read_type(page, offset)?;
    if check == 0 {
        return Ok(false);
    }
    Ok(true)
}

/// This needs to have an error if the row is too big to fit in the page.
pub fn write_row(schema: &Schema, page: &mut Page, row: &Row, rownum: u16) -> Result<(), String> {
    let offset = (rownum as usize) * schema_size(schema);
    write_row_at_offset(schema, page, row, offset)
}

/// This function writes a row to the page at the given offset
pub fn write_row_at_offset(schema: &Schema, page: &mut Page, row: &Row, offset: usize) -> Result<(), String> {
    let mut temp_offset: usize = offset;
    write_type::<u8>(page, temp_offset, 1)?;
    temp_offset += 1;
    // This looks complicated, but all it's doing is just zipping
    // the schema with the row, and then writing each cell.
    schema
        .iter()
        .zip(row.iter())
        .try_for_each(|((_, celltype), cell)| {
            celltype.write(cell, page, temp_offset)?;
            temp_offset += celltype.size();
            Ok(())
        })
}

// Locates the first free row in the page, and returns the row number, or None if the page is full.
pub fn insert_row(schema: &Schema, page: &mut Page, row: &Row) -> Result<Option<u16>, String> {
    let mut rownum = 0;
    let mut offset = 0;
    let size = schema_size(schema);
    while check_bounds(offset, size).is_ok() {
        let check: u8 = read_type(page, offset)?;
        if check == 0 {
            write_row(schema, page, row, rownum)?;
            return Ok(Some(rownum));
        }
        rownum += 1;
        offset += size;
    }
    Ok(None)
}

// We could get away with just marking the byte, but this is safer.
pub fn clear_row(schema: &Schema, page: &mut Page, rownum: u16) -> Result<(), String> {
    let size = schema_size(schema);
    let offset = (rownum as usize) * size;
    let clear = (0 as char).to_string().repeat(size);
    write_string(page, offset, &clear, size)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::util::dbtype::{Column, Value};

    #[test]
    #[serial]
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
        assert_eq!(read_row(&schema, &page, 0), Some(row1));
        assert_eq!(read_row(&schema, &page, 1), Some(row2));
        assert_eq!(read_row(&schema, &page, 2), None);
    }

    #[test]
    #[serial]
    fn test_insert() {
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
        // Fill up the page with rows.
        for i in 0..(PAGE_SIZE / schema_size(&schema)) {
            assert_eq!(
                insert_row(&schema, &mut page, &row1).unwrap(),
                Some(i as u16)
            );
        }
        // Attempting to allocate another row here now should fail.
        assert_eq!(insert_row(&schema, &mut page, &row1).unwrap(), None);
        assert_eq!(read_row(&schema, &page, 10), Some(row1));
    }

    #[test]
    #[serial]
    fn test_clear_row() {
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
        clear_row(&schema, &mut page, 0).unwrap();
        assert_eq!(read_row(&schema, &page, 0), None);
        assert_eq!(read_row(&schema, &page, 1), Some(row2));
    }

    #[test]
    #[serial]
    fn test_out_of_bounds() {
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
        // 59 * 68 = 4012, which is just under the page size.
        write_row(&schema, &mut page, &row1, 68).unwrap();
        // 59 * 69 > 4096, which should fail
        assert!(write_row(&schema, &mut page, &row2, 69).is_err());
        assert_eq!(read_row(&schema, &page, 68), Some(row1));
        assert_eq!(read_row(&schema, &page, 69), None);
    }
}
