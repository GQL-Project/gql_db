use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

use super::commitfile::*;
use super::diff::*;
use crate::{
    fileio::{
        databaseio,
        header::{write_header, Header, Schema},
        pageio::*,
        tableio::Table,
    },
    util::{
        dbtype::{Column, Value},
        row::{Row, RowInfo, RowLocation},
    },
};
use std::time::{SystemTime, UNIX_EPOCH};

// Commit Header: A struct with a commit hash, a page number, and a row number.
pub struct CommitHeader {
    commit_hash: String,
    pagenum: u32,
}

#[derive(Clone, PartialEq, Debug)]
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

    pub fn create_hash() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect()
    }
}

impl CommitHeader {
    /// Creates a new CommitHeader object.
    pub fn new(commit_hash: String, pagenum: u32) -> Self {
        Self {
            commit_hash,
            pagenum: pagenum as u32,
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

        Ok(Self {
            commit_hash,
            pagenum: page_num as u32,
        })
    }
}

impl CommitFile {
    /// Creates a new BranchesFile object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: &String, create_file: bool) -> Result<CommitFile, String> {
        // Create Header File
        let header_name: String = format!(
            "{}{}",
            databaseio::COMMIT_HEADERS_FILE_NAME.to_string(),
            databaseio::COMMIT_HEADERS_FILE_EXTENSION.to_string()
        );
        let mut header_path: String =
            format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, header_name);
        let delta_name = format!(
            "{}{}",
            databaseio::DELTAS_FILE_NAME.to_string(),
            databaseio::DELTAS_FILE_EXTENSION.to_string()
        );
        let mut delta_path: String =
            format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, delta_name);
        if dir_path.len() == 0 {
            header_path = header_name;
            delta_path = delta_name;
        }

        if create_file {
            // Header File
            std::fs::File::create(header_path.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("commit_hash".to_string(), Column::String(32)),
                ("page_num".to_string(), Column::I32),
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

    pub fn fetch_commit(&self, commit_hash: String) -> Result<Commit, String> {
        let header = self.find_header(commit_hash)?;
        if let Some(header) = header {
            self.read_commit(header.pagenum)
        } else {
            Err("Commit not found".to_string())
        }
    }

    pub fn create_commit(
        &mut self,
        message: String,
        command: String,
        diffs: Vec<Diff>,
    ) -> Result<Commit, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            .to_string();
        let hash = Commit::create_hash();
        let commit = Commit::new(hash, timestamp, message, command, diffs);
        self.store_commit(&commit)?;
        Ok(commit)
    }

    // Returns the commit header for a given commit hash.
    fn store_commit(&mut self, commit: &Commit) -> Result<(), String> {
        // Always needs to be written at the end. Traverse pages until we find a page marked as free.
        let mut pagenum = 1;
        let mut page = self.sread_page(pagenum)?;
        let mut read: u8 = read_type(&mut page, 0)?;
        while read != 0 {
            pagenum += 1;
            page = self.sread_page(pagenum)?; // If there is no page, this will create a new page
            read = read_type(&mut page, 0)?;
        }
        let hash = commit.hash.clone();
        self.write_commit(commit, pagenum)?;
        let header = CommitHeader {
            commit_hash: hash,
            pagenum,
        };
        self.insert_header(header)
    }

    fn find_header(&self, commit_hash: String) -> Result<Option<CommitHeader>, String> {
        let hash = Value::String(commit_hash);
        for RowInfo { row, .. } in self.header_table.clone() {
            if row[0] == hash {
                let header = CommitHeader::from_row(row)?;
                return Ok(Some(header));
            }
        }
        Ok(None)
    }

    fn insert_header(&mut self, header: CommitHeader) -> Result<(), String> {
        let row = vec![
            Value::String(header.commit_hash.clone()),
            Value::I32(header.pagenum as i32),
        ];
        self.header_table.insert_rows(vec![row])?;
        Ok(())
    }

    fn read_commit(&self, mut pagenum: u32) -> Result<Commit, String> {
        // Read the commit information first
        let page = &mut read_page(pagenum, &self.delta_path)?;
        let pagenum = &mut pagenum;
        let offset = &mut 0;
        let byte: u8 = self.sread_type(page, pagenum, offset)?;
        if byte != 1 {
            return Err("Invalid commit".to_string());
        }
        let commit_hash = self.sread_string(page, pagenum, offset, 32)?;
        let timestamp = self.sread_string(page, pagenum, offset, 128)?;
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
                        Diff::Update(UpdateDiff {
                            table_name,
                            schema,
                            rows,
                        })
                    } else {
                        Diff::Insert(InsertDiff {
                            table_name,
                            schema,
                            rows,
                        })
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

    fn write_commit(&self, commit: &Commit, mut pagenum: u32) -> Result<(), String> {
        let page = &mut self.sread_page(pagenum)?;
        let pagenum = &mut pagenum;
        let offset = &mut 0;
        self.swrite_type(page, pagenum, offset, 1u8)?;
        self.swrite_string(page, pagenum, offset, &commit.hash, 32)?;
        self.swrite_string(page, pagenum, offset, &commit.timestamp, 128)?;
        self.sdwrite_string(page, pagenum, offset, &commit.message)?;
        self.sdwrite_string(page, pagenum, offset, &commit.command)?;
        // Parsing the diffs
        self.swrite_type(page, pagenum, offset, commit.diffs.len() as u32)?;
        for diff in &commit.diffs {
            match diff {
                Diff::Update(update) => {
                    self.swrite_type(page, pagenum, offset, 0u32)?;
                    self.sdwrite_string(page, pagenum, offset, &update.table_name)?;
                    self.swrite_type(page, pagenum, offset, update.rows.len() as u32)?;
                    for row in &update.rows {
                        self.swrite_schema(page, pagenum, offset, &update.schema)?;
                        self.swrite_row(page, pagenum, offset, &row.row, &update.schema)?;
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::Insert(insert) => {
                    self.swrite_type(page, pagenum, offset, 1u32)?;
                    self.sdwrite_string(page, pagenum, offset, &insert.table_name)?;
                    self.swrite_type(page, pagenum, offset, insert.rows.len() as u32)?;
                    for row in &insert.rows {
                        self.swrite_schema(page, pagenum, offset, &insert.schema)?;
                        self.swrite_row(page, pagenum, offset, &row.row, &insert.schema)?;
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::Remove(remove) => {
                    self.swrite_type(page, pagenum, offset, 2u32)?;
                    self.sdwrite_string(page, pagenum, offset, &remove.table_name)?;
                    self.swrite_type(page, pagenum, offset, remove.row_locations.len() as u32)?;
                    for row in &remove.row_locations {
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::TableCreate(create) => {
                    self.swrite_type(page, pagenum, offset, 3u32)?;
                    self.sdwrite_string(page, pagenum, offset, &create.table_name)?;
                    self.swrite_schema(page, pagenum, offset, &create.schema)?;
                }
                Diff::TableRemove(remove) => {
                    self.swrite_type(page, pagenum, offset, 4u32)?;
                    self.sdwrite_string(page, pagenum, offset, &remove.table_name)?;
                }
            }
        }
        write_page(*pagenum, &self.delta_path, page)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_simple_commit() {
        let delta = CommitFile::new(&"".to_string(), true).unwrap();
        let schema = vec![
            ("t1".to_string(), Column::String(60)),
            ("t2".to_string(), Column::String(32)),
        ];
        let commit = Commit::new(
            "test_hash".to_string(),
            "test_timestamp".to_string(),
            "test_message".to_string(),
            "test_command".to_string(),
            vec![Diff::TableCreate(TableCreateDiff {
                table_name: "test_table".to_string(),
                schema: schema.clone(),
            })],
        );
        delta.write_commit(&commit, 0).unwrap();
        let commit2 = delta.read_commit(0).unwrap();
        assert_eq!(commit, commit2);

        // Delete the test files
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_simple_header() {
        let mut delta = CommitFile::new(&"".to_string(), true).unwrap();
        let schema = vec![
            ("t1".to_string(), Column::String(60)),
            ("t45".to_string(), Column::String(32)),
        ];
        let commit = Commit::new(
            "test_hash".to_string(),
            "test_timestamp".to_string(),
            "test_message".to_string(),
            "test_command".to_string(),
            vec![Diff::TableCreate(TableCreateDiff {
                table_name: "test_table".to_string(),
                schema: schema.clone(),
            })],
        );
        delta.store_commit(&commit).unwrap();
        let commit2 = delta.fetch_commit("test_hash".to_string()).unwrap();
        assert_eq!(commit, commit2);

        // Delete the test files
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }
}
