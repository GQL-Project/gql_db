use std::collections::HashMap;

use super::{pageio::*, index::*};
use crate::util::dbtype::Column;
pub struct Header {
    pub num_pages: u32,
    pub schema: Schema,
    pub indexes: HashMap<IndexKey, u32>,
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
    let (buf, page_type) = read_page(0, &file)?;
    if page_type != PageType::Header {
        return Err(format!("Error page 0 is not a header page in {}", file));
    }
    let num_pages: u32 = read_type(&buf, 0)?;
    let schema: Schema = read_schema(&buf)?;

    // Read the number of indexes from page
    let mut index_offset: usize = 5 + (schema.len() * 54);
    let num_indexes: u32 = read_type::<u32>(&buf, index_offset)?;
    index_offset += 4;

    // Read indexes from page
    let mut indexes: HashMap<IndexKey, u32> = HashMap::new();
    for _ in 0..num_indexes {
        // Read the number of columns that compose this specific index
        let num_cols_in_idx: u16 = read_type::<u16>(&buf, index_offset)?;
        index_offset += 2;

        let mut index_key: IndexKey = Vec::new();
        for _ in 0..num_cols_in_idx {
            // Read the column index
            let col_idx: u8 = read_type::<u8>(&buf, index_offset)?;
            index_offset += 1;
            index_key.push(col_idx);
        }

        let index_pagenum: u32 = read_type(&buf, index_offset)?;
        index_offset += 4;
        indexes.insert(index_key, index_pagenum);
    }
    
    Ok(Header { num_pages, schema, indexes })
}

pub fn write_header(file: &String, header: &Header) -> Result<(), String> {
    let mut buf = Box::new([0u8; PAGE_SIZE]);
    write_type(buf.as_mut(), 0, header.num_pages)?;
    write_schema(buf.as_mut(), &header.schema)?;

    // Write number of indexes to header
    let mut index_offset: usize = 5 + (header.schema.len() * 54);
    write_type(buf.as_mut(), index_offset, header.indexes.len() as u32)?;
    index_offset += 4;

    // Write indexes to header
    for (index_cols, pagenum) in &header.indexes {
        // Write the number of columns that compose this specific index
        write_type(buf.as_mut(), index_offset, index_cols.len() as u16)?;
        index_offset += 2;

        // Write the column indices that compose this index
        for index_col in index_cols {
            write_type(buf.as_mut(), index_offset, *index_col as u8)?;
            index_offset += 1;
        }

        // Write the pagenum that the first page of this index is stored on
        write_type(buf.as_mut(), index_offset, *pagenum as u32)?;
        index_offset += 4;
    }

    write_page(0, &file, buf.as_ref(), PageType::Header)?;
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
            indexes: HashMap::new(),
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
            indexes: HashMap::new(),
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

    #[test]
    #[serial]
    fn test_writing_header_with_indexes() {
        let schema: Schema = vec![
            ("col1".to_string(), Column::I32),
            ("col2".to_string(), Column::String(50)),
            ("col3".to_string(), Column::Float),
        ];
        let mut indexes: HashMap<IndexKey, u32> = HashMap::new();
        indexes.insert(vec![0], 1);
        indexes.insert(vec![1], 2);
        indexes.insert(vec![2], 3);
        indexes.insert(vec![0, 1], 4);
        indexes.insert(vec![0, 2], 5);
        indexes.insert(vec![1, 2], 6);
        indexes.insert(vec![0, 1, 2], 7);
        let header: Header = Header {
            num_pages: 10,
            schema,
            indexes,
        };
        let path: String = "test2.db".to_string();
        create_file(&path).unwrap();
        write_header(&path, &header).unwrap();
        let header2: Header = read_header(&path).unwrap();
        assert_eq!(header.num_pages, header2.num_pages);
        assert_eq!(header.schema, header2.schema);
        assert_eq!(header.indexes, header2.indexes);
        // Clean up
        std::fs::remove_file("test2.db").unwrap();
    }
}
