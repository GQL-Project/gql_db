use crate::{
    fileio::{
        header::{schema_size, Schema},
        pageio::*,
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
    // Safe reads and writes: These functions will read from the page, and if the offset is past the end of the
    // page, it will read from the next page. This is useful for reading strings and other types
    // that are not guaranteed to be on a page boundary.

    // Allocates new pages when failing to read from a page.
    pub fn sread_page(&self, pagenum: u32) -> Result<Box<Page>, String> {
        let page = read_page(pagenum, &self.delta_path);
        if page.is_err() {
            Ok(Box::new([0; PAGE_SIZE]))
        } else {
            Ok(page.unwrap().0)
        }
    }

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
            *page = *read_page(*pagenum, &self.delta_path)?.0;
        }
        let string = read_string(page, *offset as usize, size as usize)?;
        *offset = *offset + size;
        Ok(string)
    }

    pub fn swrite_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        string: &str,
        size: u32,
    ) -> Result<(), String> {
        // If offset is greater than the page size, read the next page and reset the offset
        if *offset + size >= PAGE_SIZE as u32 {
            write_page(*pagenum, &self.delta_path, page, PageType::Data)?;
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *self.sread_page(*pagenum)?
        }
        write_string(page, *offset as usize, string, size as usize)?;
        *offset = *offset + size;
        Ok(())
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

    // Safe write - dynamic string size
    pub fn sdwrite_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        string: &str,
    ) -> Result<(), String> {
        let size: u32 = string.len() as u32;
        self.swrite_type(page, pagenum, offset, size)?;
        self.swrite_string(page, pagenum, offset, string, size)
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
            write_page(*pagenum, &self.delta_path, page, PageType::Data)?;
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?.0;
        }
        let t = read_type(page, *offset as usize)?;
        *offset = *offset + size;
        Ok(t)
    }

    pub fn swrite_type<T: Sized>(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        t: T,
    ) -> Result<(), String> {
        // If offset is greater than the page size, read the next page and reset the offset
        let size = std::mem::size_of::<T>() as u32;
        if *offset + size >= PAGE_SIZE as u32 {
            write_page(*pagenum, &self.delta_path, page, PageType::Data)?;
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *self.sread_page(*pagenum)?;
        }
        write_type(page, *offset as usize, t)?;
        *offset = *offset + size;
        Ok(())
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
            *page = *read_page(*pagenum, &self.delta_path)?.0;
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

    pub fn swrite_row(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        row: &Row,
        schema: &Schema,
    ) -> Result<(), String> {
        let size = schema_size(schema) as u32; // Ensure the row is not split across pages
        if *offset + size >= PAGE_SIZE as u32 {
            write_page(*pagenum, &self.delta_path, page, PageType::Data)?;
            *offset = 0;
            *pagenum = *pagenum + 1;
            *page = *self.sread_page(*pagenum)?
        }
        self.swrite_type::<u8>(page, pagenum, offset, 1u8)?;
        schema
            .iter()
            .zip(row.iter())
            .try_for_each(|((_, celltype), cell)| {
                celltype.write(cell, page, *offset as usize)?;
                *offset = *offset + celltype.size() as u32;
                Ok(())
            })
    }

    pub fn sread_schema(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
    ) -> Result<Schema, String> {
        let mut schema = Schema::new();
        let num_cols: u8 = self.sread_type(page, pagenum, offset)?;
        for _ in 0..num_cols {
            let typeid: u16 = self.sread_type(page, pagenum, offset)?;
            let colname = self.sread_string(page, pagenum, offset, 50)?;
            schema.push((colname, Column::decode_type(typeid)));
        }
        Ok(schema)
    }

    pub fn swrite_schema(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        schema: &Schema,
    ) -> Result<(), String> {
        self.swrite_type::<u8>(page, pagenum, offset, schema.len() as u8)?;
        schema.iter().try_for_each(|(colname, celltype)| {
            self.swrite_type::<u16>(page, pagenum, offset, celltype.encode_type())?;
            self.swrite_string(page, pagenum, offset, colname, 50)?;
            Ok(())
        })
    }
}
