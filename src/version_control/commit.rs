use crate::{
    fileio::{
        databaseio,
        header::{write_header, Header},
        pageio::{write_page, PAGE_SIZE},
        tableio::{get_row, insert_rows, Table},
    },
    util::{
        dbtype::{Column, Value},
        row::{Row, RowInfo, RowLocation},
    },
};

use super::diff::Diff;

// Commit Header: A struct with a commit hash, a page number, and a row number.
pub struct CommitHeader {
    commit_hash: String,
    loc: RowLocation, // Note that RowLocation's rownum is a byte offset, not a row number.
}

pub struct Commit {
    header: CommitHeader,
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
        page_num: i32,
        row_num: i32,
        timestamp: String,
        message: String,
        command: String,
        diffs: Vec<Diff>,
    ) -> Self {
        Self {
            header: CommitHeader::new(commit_hash, page_num, row_num),
            timestamp,
            message,
            command,
            diffs,
        }
    }

    // TODO: Implement a function to write a commit to a file.

    // TODO: Implement a function to read and create a commit from a CommitHeader.
}

impl CommitHeader {
    /// Creates a new CommitHeader object.
    pub fn new(commit_hash: String, pagenum: i32, rownum: i32) -> Self {
        Self {
            commit_hash,
            loc: RowLocation {
                pagenum: pagenum as u32,
                rownum: rownum as u16,
            },
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
            loc: RowLocation {
                pagenum: page_num as u32,
                rownum: row_num as u16,
            },
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
            header_path: header_path,
            delta_path: delta_path,
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

    pub fn insert_header(&mut self, header: CommitHeader) -> Result<(), String> {
        let row = vec![
            Value::String(header.commit_hash.clone()),
            Value::I32(header.loc.pagenum as i32),
            Value::I32(header.loc.rownum as i32),
        ];
        insert_rows(&mut self.header_table, vec![row])?;
        Ok(())
    }
}
