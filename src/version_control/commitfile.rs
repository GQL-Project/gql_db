use crate::{
    fileio::{
        header::{schema_size, Schema},
        pageio::{read_page, read_string, read_type, Page, PAGE_SIZE},
        tableio::Table,
    },
    util::{dbtype::Column, row::Row},
};

#[derive(Clone)]
pub struct CommitFile {
    pub header_path: String,
    pub delta_path: String,
    pub header_table: Table,
}

impl CommitFile {
    // Safe reads: These functions will read from the page, and if the offset is past the end of the
    // page, it will read from the next page. This is useful for reading strings and other types
    // that are not guaranteed to be on a page boundary.
    pub fn sread_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        size: u32,
    ) -> Result<String, String> {
        // If offset is greater than the page size, read the next page and reset the offset
        if *offset + size >= PAGE_SIZE as u32 {
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?;
        }
        let string = read_string(page, *offset as usize, size as usize)?;
        *offset = *offset + size;
        Ok(string)
    }

    // Safe read - dynamic string size
    pub fn sdread_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
    ) -> Result<String, String> {
        let size: u32 = self.sread_type(page, pagenum, offset)?;
        self.sread_string(page, pagenum, offset, size)
    }

    pub fn sread_type<T: Sized>(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
    ) -> Result<T, String> {
        // If offset is greater than the page size, read the next page and reset the offset
        let size = std::mem::size_of::<T>() as u32;
        if *offset + size >= PAGE_SIZE as u32 {
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?;
        }
        let t = read_type(page, *offset as usize)?;
        *offset = *offset + size;
        Ok(t)
    }

    pub fn sread_row(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        schema: &Schema,
    ) -> Result<Row, String> {
        let size = schema_size(schema) as u32; // Ensure the row is not split across pages
        if *offset + size >= PAGE_SIZE as u32 {
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?;
        }
        let mut row = Row::new();
        let check: u8 = self.sread_type(page, pagenum, offset)?;
        assert!(check == 1, "Malformed Row");
        for (_, celltype) in schema {
            row.push(celltype.read(page, *offset as usize)?);
            *offset = *offset + celltype.size() as u32;
        }
        Ok(row)
    }

    pub fn sread_schema(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
    ) -> Result<Schema, String> {
        let mut schema = Schema::new();
        // Rather than doing ::<u8>, this is cleaner
        let num_cols: u8 = self.sread_type(page, pagenum, offset)?;
        for _ in 0..num_cols {
            let typeid: u16 = self.sread_type(page, pagenum, offset)?;
            let colname = self.sread_string(page, pagenum, offset, 50)?;
            schema.push((colname, Column::decode_type(typeid)));
        }
        Ok(schema)
    }
}
