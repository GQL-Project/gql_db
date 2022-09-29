use crate::{
    fileio::{
        databaseio,
        header::{read_schema, write_header, Header, Schema},
        pageio::*,
        tableio::{get_row, insert_rows, Table},
    },
    util::{
        dbtype::{Column, Value},
        row::{Row, RowInfo, RowLocation},
    },
};

use super::diff::{Diff, TableCreateDiff, TableRemoveDiff};

// Commit Header: A struct with a commit hash, a page number, and a row number.
pub struct CommitHeader {
    commit_hash: String,
    pagenum: u32,
    offset: u32,
}

pub struct Commit {
    hash: String,
    timestamp: String, // TODO: Change to a DateTime object
    message: String,
    command: String, // Command that was run to create this commit
    diffs: Vec<Diff>,
}

#[derive(Clone)]
pub struct CommitFile {
    header_path: String,
    delta_path: String,
    header_table: Table,
}

impl Commit {
    /// Creates a new Commit object.
    pub fn new(
        commit_hash: String,
        timestamp: String,
        message: String,
        command: String,
        diffs: Vec<Diff>,
    ) -> Self {
        Self {
            hash: commit_hash,
            timestamp,
            message,
            command,
            diffs,
        }
    }
}

impl CommitHeader {
    /// Creates a new CommitHeader object.
    pub fn new(commit_hash: String, pagenum: u32, rownum: u32) -> Self {
        Self {
            commit_hash,
            pagenum: pagenum as u32,
            offset: rownum as u32,
        }
    }

    pub fn from_row(row: Row) -> Result<Self, String> {
        let commit_hash: String = match row.get(0) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid commit hash".to_string()),
        };
        let page_num: i32 = match row.get(1) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string()),
        };
        let row_num: i32 = match row.get(2) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string()),
        };

        Ok(Self {
            commit_hash,
            pagenum: page_num as u32,
            offset: row_num as u32,
        })
    }
}

impl CommitFile {
    /// Creates a new BranchesFile object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: String, create_file: bool) -> Result<CommitFile, String> {
        // Create Header File
        let header_name: String = format!(
            "{}{}",
            databaseio::COMMIT_HEADERS_FILE_NAME.to_string(),
            databaseio::COMMIT_HEADERS_FILE_EXTENSION.to_string()
        );
        let header_path: String =
            format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, header_name);
        let delta_name = format!(
            "{}{}",
            databaseio::DELTAS_FILE_NAME.to_string(),
            databaseio::DELTAS_FILE_NAME.to_string()
        );
        let delta_path: String = format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, delta_name);

        if create_file {
            // Header File
            std::fs::File::create(header_path.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("commit_hash".to_string(), Column::String(32)),
                ("page_num".to_string(), Column::I32),
                ("row_num".to_string(), Column::I32),
            ];
            let header = Header {
                num_pages: 2,
                schema,
            };
            write_header(&header_path, &header)?;

            // Write a blank page to the table
            let page = [0u8; PAGE_SIZE];
            write_page(1, &header_path, &page)?;

            // Delta File
            std::fs::File::create(delta_path.clone()).map_err(|e| e.to_string())?;
        }

        Ok(CommitFile {
            header_path,
            delta_path,
            header_table: Table::new(
                &dir_path.clone(),
                &databaseio::COMMIT_HEADERS_FILE_NAME.to_string(),
                Some(&databaseio::COMMIT_HEADERS_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    /// Search for a commit header by its commit hash.
    /// Returns the page number and row number of the commit header.
    pub fn find_header(&self, commit_hash: String) -> Result<Option<CommitHeader>, String> {
        let hash = Value::String(commit_hash);
        for RowInfo { row, .. } in self.header_table.clone() {
            if row[0] == hash {
                let header = CommitHeader::from_row(row)?;
                return Ok(Some(header));
            }
        }
        Ok(None)
    }

    pub fn read_commit(&self, mut pagenum: u32, mut offset: u32) -> Result<Option<Commit>, String> {
        // Read the commit information first
        let page = &mut read_page(pagenum, &self.delta_path)?;
        let pagenum = &mut pagenum;
        let offset = &mut offset;
        let commit_hash = self.sread_string(page, pagenum, offset, 32)?;
        let timestamp = self.sread_string(page, pagenum, offset, 32)?;
        let message = self.sdread_string(page, pagenum, offset)?;
        let command = self.sdread_string(page, pagenum, offset)?;

        // Parsing the diffs
        let num_diffs: u32 = self.sread_type(page, pagenum, offset)?;
        let mut diffs: Vec<Diff> = Vec::new();
        for _ in 0..num_diffs {
            let size: u32 = self.sread_type(page, pagenum, offset)?;
            let difftype: u32 = self.sread_type(page, pagenum, offset)?;
            let table_name = self.sdread_string(page, pagenum, offset)?;
            let diff: Diff = match difftype {
                0 => {
                    // Update
                    let old_value = self.sread_string(page, pagenum, offset, size)?;
                    let new_value = self.sread_string(page, pagenum, offset, size)?;
                    todo!()
                }
                1 => {
                    // Insert
                    let value = self.sread_string(page, pagenum, offset, size)?;
                    todo!()
                }
                2 => {
                    // Delete
                    let value = self.sread_string(page, pagenum, offset, size)?;
                    todo!()
                }
                3 => {
                    // Create Table
                    let schema = self.sread_schema(page, pagenum, offset)?;
                    Diff::TableCreate(TableCreateDiff { table_name, schema })
                }
                4 => {
                    // Remove Table
                    Diff::TableRemove(TableRemoveDiff { table_name })
                }
                _ => return Err("Invalid diff type".to_string()),
            };
            diffs.push(diff);
        }
        Ok(Some(Commit::new(
            commit_hash,
            timestamp,
            message,
            command,
            diffs,
        )))
    }

    pub fn insert_header(&mut self, header: CommitHeader) -> Result<(), String> {
        let row = vec![
            Value::String(header.commit_hash.clone()),
            Value::I32(header.pagenum as i32),
            Value::I32(header.offset as i32),
        ];
        insert_rows(&mut self.header_table, vec![row])?;
        Ok(())
    }

    // Safe reads (with page and offset changes when needed)
    fn sread_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32,
        size: u32,
    ) -> Result<String, String> {
        // If offset is greater than the page size, read the next page and reset the offset
        if *offset >= PAGE_SIZE as u32 {
            *offset = *offset - PAGE_SIZE as u32;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?;
        }
        let string = read_string(page, *offset as usize, size as usize)?;
        *offset = *offset + size as u32;
        Ok(string)
    }

    // Safe read - dynamic string size
    fn sdread_string(
        &self,
        page: &mut Page,
        pagenum: &mut u32,
        offset: &mut u32
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
        if *offset >= PAGE_SIZE as u32 {
            *offset = *offset - PAGE_SIZE as u32;
            *pagenum = *pagenum + 1;
            *page = *read_page(*pagenum, &self.delta_path)?;
        }
        let t = read_type(page, *offset as usize)?;
        *offset = *offset + std::mem::size_of::<T>() as u32;
        Ok(t)
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
