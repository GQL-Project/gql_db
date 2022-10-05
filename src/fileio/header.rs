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
        offset += 54;
    }
    Ok(schema)
}

pub fn write_schema(page: &mut Page, schema: &Schema) -> Result<(), String> {
    write_type(page, 4, schema.len() as u8)?;
    let mut offset = 5;
    for (colname, coltype) in schema {
        write_type::<u16>(page, offset, coltype.encode_type())?;
        write_string(page, offset + 2, colname, 50)?;
        offset += 54;
    }
    Ok(())
}

// Not sure if it's better to have a file or the page passed in,
// this can be changed later on.
pub fn read_header(file: &String) -> Result<Header, String> {
    let buf = read_page(0, &file)?;
    let num_pages = read_type(&buf, 0)?;
    let schema = read_schema(&buf)?;
    Ok(Header { num_pages, schema })
}

pub fn write_header(file: &String, header: &Header) -> Result<(), String> {
    let mut buf = Box::new([0u8; PAGE_SIZE]);
    write_type(buf.as_mut(), 0, header.num_pages)?;
    write_schema(buf.as_mut(), &header.schema)?;
    write_page(0, &file, buf.as_ref())?;
    Ok(())
}

pub fn schema_size(schema: &Schema) -> usize {
    let mut size = 1; // 1 byte for the check byte
    for (_, coltype) in schema {
        size += coltype.size();
    }
    size
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::util::dbtype::Column;

    #[test]
    #[serial]
    fn test_schema_size() {
        let schema = vec![
            ("col1".to_string(), Column::I32),
            ("col2".to_string(), Column::String(50)),
            ("col3".to_string(), Column::Float),
        ];
        assert_eq!(schema_size(&schema), 1 + 4 + 50 + 4);
    }

    #[test]
    #[serial]
    fn test_read_write_header() {
        let schema = vec![
            ("col1".to_string(), Column::I32),
            ("col2".to_string(), Column::String(50)),
            ("col3".to_string(), Column::Float),
        ];
        let header = Header {
            num_pages: 10,
            schema,
        };
        let path = "test.db".to_string();
        create_file(&path).unwrap();
        write_header(&path, &header).unwrap();
        let header2 = read_header(&path).unwrap();
        assert_eq!(header.num_pages, header2.num_pages);
        assert_eq!(header.schema, header2.schema);
        // Clean up
        std::fs::remove_file("test.db").unwrap();
    }

    #[test]
    #[serial]
    fn test_read_write_large_header() {
        let schema = vec![
            ("column name 345".to_string(), Column::I32),
            ("a string column of 50".to_string(), Column::String(50)),
            ("a float column of 32 bytes".to_string(), Column::Float),
            ("a small boolean column".to_string(), Column::Bool),
            ("a huge timestamp column".to_string(), Column::Timestamp),
        ];
        let header = Header {
            num_pages: 245,
            schema,
        };
        let path = "test1.db".to_string();
        create_file(&path).unwrap();
        write_header(&path, &header).unwrap();
        let header2 = read_header(&path).unwrap();
        assert_eq!(header.num_pages, header2.num_pages);
        assert_eq!(header.schema, header2.schema);
        // Clean up
        std::fs::remove_file("test1.db").unwrap();
    }
}
