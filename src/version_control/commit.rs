use crate::{
    fileio::{
        databaseio,
        header::{read_schema, schema_size, write_header, Header, Schema},
        pageio::*,
        rowio::read_row,
        tableio::{get_row, insert_rows, Table},
    },
    util::{
        dbtype::{Column, Value},
        row::{Row, RowInfo, RowLocation},
    },
};

use super::diff::*;
use super::commitfile::*;

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

    pub fn fetch_commit(&self, commit_hash: String) -> Result<Commit, String> {
        let header = self.find_header(commit_hash)?;
        if let Some(header) = header {
            self.read_commit(header.pagenum, header.offset)
        } else {
            Err("Commit not found".to_string())
        }
    }

    pub fn read_commit(&self, mut pagenum: u32, mut offset: u32) -> Result<Commit, String> {
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
            let difftype: u32 = self.sread_type(page, pagenum, offset)?;
            let table_name = self.sdread_string(page, pagenum, offset)?;
            let diff: Diff = match difftype {
                0 | 1 => {
                    // Update or Insert
                    let num_rows: usize = self.sread_type(page, pagenum, offset)?;
                    let schema: Schema = self.sread_schema(page, pagenum, offset)?;
                    let mut rows: Vec<RowInfo> = Vec::new();
                    for _ in 0..num_rows {
                        let row = self.sread_row(page, pagenum, offset, &schema)?;
                        let row_info = RowInfo {
                            row,
                            pagenum: self.sread_type(page, pagenum, offset)?,
                            rownum: self.sread_type(page, pagenum, offset)?,
                        };
                        rows.push(row_info);
                    }
                    if difftype == 0 {
                        Diff::Update(UpdateDiff { table_name, rows })
                    } else {
                        Diff::Insert(InsertDiff { table_name, rows })
                    }
                }
                2 => {
                    // Delete
                    let num_rows: usize = self.sread_type(page, pagenum, offset)?;
                    let mut rows: Vec<RowLocation> = Vec::new();
                    for _ in 0..num_rows {
                        let page_num: u32 = self.sread_type(page, pagenum, offset)?;
                        let row_num: u16 = self.sread_type(page, pagenum, offset)?;
                        rows.push(RowLocation {
                            pagenum: page_num,
                            rownum: row_num,
                        });
                    }
                    Diff::Remove(RemoveDiff {
                        table_name,
                        row_locations: rows,
                    })
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
        Ok(Commit::new(commit_hash, timestamp, message, command, diffs))
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
}
