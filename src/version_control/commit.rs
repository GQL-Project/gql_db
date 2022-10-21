use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

use super::commitfile::*;
use super::diff;
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
        row::{Row, RowInfo},
    },
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Commit Header: A struct with a commit hash, a page number, and a row number.
pub struct CommitHeader {
    commit_hash: String,
    pagenum: u32,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Commit {
    pub hash: String,
    pub timestamp: String, // TODO: Change to a DateTime object
    pub message: String,
    pub command: String, // Command that was run to create this commit
    pub diffs: Vec<Diff>,
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

    pub fn fetch_commit(&self, commit_hash: &String) -> Result<Commit, String> {
        let header = self.find_header(commit_hash.clone())?;
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
            let difftype: i32 = self.sread_type::<u32>(page, pagenum, offset)? as i32;
            let table_name = self.sdread_string(page, pagenum, offset)?;
            let diff: Diff = match difftype {
                INSERT_TYPE | UPDATE_TYPE | REMOVE_TYPE => {
                    // Update or Insert or Remove
                    let num_rows: u32 = self.sread_type(page, pagenum, offset)?;
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
                    if difftype == INSERT_TYPE {
                        Diff::Insert(InsertDiff {
                            table_name,
                            schema,
                            rows,
                        })
                    } else if difftype == UPDATE_TYPE {
                        Diff::Update(UpdateDiff {
                            table_name,
                            schema,
                            rows,
                        })
                    } else {
                        Diff::Remove(RemoveDiff {
                            table_name,
                            schema,
                            rows,
                        })
                    }
                }

                TABLE_CREATE_TYPE => {
                    // Create Table
                    let schema = self.sread_schema(page, pagenum, offset)?;
                    Diff::TableCreate(TableCreateDiff { table_name, schema })
                }
                TABLE_REMOVE_TYPE => {
                    // Remove Table
                    let schema = self.sread_schema(page, pagenum, offset)?;
                    let num_rows: u32 = self.sread_type(page, pagenum, offset)?;
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
                    Diff::TableRemove(TableRemoveDiff {
                        table_name,
                        schema,
                        rows_removed: rows,
                    })
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
            self.swrite_type(page, pagenum, offset, diff.get_type() as u32)?;
            self.sdwrite_string(page, pagenum, offset, &diff.get_table_name())?;
            match diff {
                Diff::Insert(insert) => {
                    self.swrite_type(page, pagenum, offset, insert.rows.len() as u32)?;
                    self.swrite_schema(page, pagenum, offset, &insert.schema)?;
                    for row in &insert.rows {
                        self.swrite_row(page, pagenum, offset, &row.row, &insert.schema)?;
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::Update(update) => {
                    self.swrite_type(page, pagenum, offset, update.rows.len() as u32)?;
                    self.swrite_schema(page, pagenum, offset, &update.schema)?;
                    for row in &update.rows {
                        self.swrite_row(page, pagenum, offset, &row.row, &update.schema)?;
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::Remove(remove) => {
                    self.swrite_type(page, pagenum, offset, remove.rows.len() as u32)?;
                    self.swrite_schema(page, pagenum, offset, &remove.schema)?;
                    for row in &remove.rows {
                        self.swrite_row(page, pagenum, offset, &row.row, &remove.schema)?;
                        self.swrite_type(page, pagenum, offset, row.pagenum)?;
                        self.swrite_type(page, pagenum, offset, row.rownum)?;
                    }
                }
                Diff::TableCreate(create) => {
                    self.swrite_schema(page, pagenum, offset, &create.schema)?;
                }
                Diff::TableRemove(remove) => {
                    self.swrite_schema(page, pagenum, offset, &remove.schema)?;
                }
            }
        }
        write_page(*pagenum, &self.delta_path, page)?;
        Ok(())
    }

    pub fn squash_commits(&mut self, commits: &Vec<Commit>) -> Result<Commit, String> {
        if commits.len() == 0 {
            return Err("No commits to combine".to_string());
        } else if commits.len() == 1 {
            return Ok(commits[0].clone());
        }
        let msg = format!("Combined {} commits", commits.len());
        let cmd = format!(
            "GQL squash {} {}",
            commits[0].hash,
            commits[commits.len() - 1].hash
        );
        // Create a map of table names to a map of "Diff Type" to diff
        // TODO: It might be better to sort the rows in a diff by pagenum and rownum
        // We'd be able to do much quicker merges
        let mut map: HashMap<String, HashMap<i32, Diff>> = HashMap::new();
        for commit in commits {
            let mut diffs = commit.diffs.clone();
            diffs.sort_by(|x, y| x.partial_cmp(y).unwrap_or(Ordering::Equal));
            for diff in &commit.diffs {
                match diff {
                    Diff::Update(update) => {
                        let mut newrows = update.rows.clone();
                        // An Insert and an Update just become an Insert
                        if let Some(Diff::Insert(insert)) =
                            get_diff(&map, &update.table_name, INSERT_TYPE)
                        {
                            // If an update diff exists on a row, replace the insert diff with the update diff value
                            let rows: Vec<RowInfo> = insert
                                .rows
                                .iter()
                                .map(|row| {
                                    let mut row = row.clone();
                                    for newrow in &mut newrows {
                                        if row.rownum == newrow.rownum
                                            && row.pagenum == newrow.pagenum
                                        {
                                            // Replace the insert's row with the update's row
                                            row.row = newrow.row.clone();
                                            return row;
                                        }
                                    }
                                    row
                                })
                                .collect();
                            // Retain the rows that were not updated
                            newrows.retain(|x| {
                                !insert
                                    .rows
                                    .iter()
                                    .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                            });
                            // Update the insert diff with the new rows
                            add_diff(
                                &mut map,
                                Diff::Insert(InsertDiff {
                                    table_name: update.table_name.clone(),
                                    schema: update.schema.clone(),
                                    rows,
                                }),
                                update.table_name.clone(),
                            );
                        }
                        // Two Updates just become one update
                        if let Some(Diff::Update(existing)) =
                            get_diff(&map, &update.table_name, UPDATE_TYPE)
                        {
                            // Merge the diffs together, removing any rows that are in the new update
                            let rows = existing
                                .rows
                                .iter()
                                .filter(|x| {
                                    !newrows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .chain(newrows.iter())
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            // Update the update diff with the new rows
                            add_diff(
                                &mut map,
                                Diff::Update(UpdateDiff {
                                    table_name: update.table_name.clone(),
                                    schema: update.schema.clone(),
                                    rows,
                                }),
                                update.table_name.clone(),
                            );
                        } else {
                            add_diff(
                                &mut map,
                                Diff::Update(UpdateDiff {
                                    table_name: update.table_name.clone(),
                                    schema: update.schema.clone(),
                                    rows: newrows,
                                }),
                                update.table_name.clone(),
                            );
                        }
                    } // End of Update
                    Diff::Insert(insert) => {
                        // A Remove and an Insert just becomes an Insert
                        if let Some(Diff::Remove(remove)) =
                            get_diff(&map, &insert.table_name, REMOVE_TYPE)
                        {
                            let rows = remove
                                .rows
                                .iter()
                                .filter(|x| {
                                    !insert
                                        .rows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            add_diff(
                                &mut map,
                                Diff::Remove(RemoveDiff {
                                    table_name: insert.table_name.clone(),
                                    schema: insert.schema.clone(),
                                    rows,
                                }),
                                insert.table_name.clone(),
                            );
                        }
                        // Two Inserts just become one insert
                        if let Some(Diff::Insert(existing)) =
                            get_diff(&map, &insert.table_name, INSERT_TYPE)
                        {
                            // Merge the diffs
                            let rows = existing
                                .rows
                                .iter()
                                .filter(|x| {
                                    !insert
                                        .rows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .chain(insert.rows.iter())
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            add_diff(
                                &mut map,
                                Diff::Insert(InsertDiff {
                                    table_name: insert.table_name.clone(),
                                    schema: insert.schema.clone(),
                                    rows,
                                }),
                                insert.table_name.clone(),
                            );
                        } else {
                            add_diff(&mut map, diff.clone(), insert.table_name.clone());
                        }
                    } // End of Insert
                    Diff::Remove(remove) => {
                        let mut newrows = remove.rows.clone();
                        // An Insert and a Remove cancel each other out
                        if let Some(Diff::Insert(insert)) =
                            get_diff(&map, &remove.table_name, INSERT_TYPE)
                        {
                            // Remove rows from insert that are in remove
                            let rows = insert
                                .rows
                                .iter()
                                .filter(|x| {
                                    !newrows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            // Retain the rows that were not removed
                            newrows.retain(|x| {
                                !insert
                                    .rows
                                    .iter()
                                    .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                            });
                            // Update the insert diff with the new rows
                            add_diff(
                                &mut map,
                                Diff::Insert(InsertDiff {
                                    table_name: remove.table_name.clone(),
                                    schema: remove.schema.clone(),
                                    rows,
                                }),
                                remove.table_name.clone(),
                            );
                        }
                        // An Update and a Remove just becomes a Remove
                        if let Some(Diff::Update(update)) =
                            get_diff(&map, &remove.table_name, UPDATE_TYPE)
                        {
                            // Remove rows from update that are in remove
                            let rows = update
                                .rows
                                .iter()
                                .filter(|x| {
                                    !newrows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            // Update the update diff with the new rows
                            add_diff(
                                &mut map,
                                Diff::Update(UpdateDiff {
                                    table_name: remove.table_name.clone(),
                                    schema: remove.schema.clone(),
                                    rows,
                                }),
                                remove.table_name.clone(),
                            );
                        }
                        // Two Removes just become one Remove
                        if let Some(Diff::Remove(existing)) =
                            get_diff(&map, &remove.table_name, REMOVE_TYPE)
                        {
                            // Merge the diffs
                            let rows = existing
                                .rows
                                .iter()
                                .filter(|x| {
                                    !newrows
                                        .iter()
                                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                                })
                                .chain(newrows.iter())
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            add_diff(
                                &mut map,
                                Diff::Remove(RemoveDiff {
                                    table_name: remove.table_name.clone(),
                                    schema: remove.schema.clone(),
                                    rows,
                                }),
                                remove.table_name.clone(),
                            );
                        } else {
                            add_diff(
                                &mut map,
                                Diff::Remove(RemoveDiff {
                                    table_name: remove.table_name.clone(),
                                    schema: remove.schema.clone(),
                                    rows: newrows,
                                }),
                                remove.table_name.clone(),
                            );
                        }
                    } // End of Remove
                    Diff::TableCreate(create) => {
                        add_diff(&mut map, diff.clone(), create.table_name.clone());
                    }
                    // Table remove gets rid of previous diffs on that table
                    Diff::TableRemove(remove) => {
                        // We should check if there were other diffs for this table before the remove table
                        if map.contains_key(&remove.table_name) {
                            // It's fine to unwrap here because we just checked that the key exists
                            let value: &HashMap<i32, Diff> = map.get(&remove.table_name).unwrap();

                            // If there is a TableCreate, we can remove all the diffs for that table
                            // because we have both a TableCreate and a TableRemove for the same table
                            if value.contains_key(&diff::TABLE_CREATE_TYPE) {
                                map.remove(&remove.table_name);
                            }
                            // If there is not a TableCreate, we can remove all the diffs for that table,
                            // but we still need to add the TableRemove
                            else {
                                map.remove(&remove.table_name);
                                add_diff(&mut map, diff.clone(), remove.table_name.clone());
                            }
                        }
                        // Otherwise, just add the diff like normal
                        else {
                            add_diff(&mut map, diff.clone(), remove.table_name.clone());
                        }
                    } // End of TableRemove
                }
            }
        }
        let diffs: Vec<Diff> = map
            .into_values()
            .map(|y| y.into_values())
            .flatten()
            .filter(|x| !x.is_empty())
            .collect();
        self.create_commit(msg, cmd, diffs)
    }

    pub fn combine_commits(&mut self, commits: &Vec<Commit>) -> Result<Commit, String> {
        if commits.len() == 0 {
            return Err("No commits to combine".to_string());
        }
        let msg = format!("Combined {} commits", commits.len());
        let cmd = format!(
            "GQL squash {} {}",
            commits[0].hash,
            commits[commits.len() - 1].hash
        );
        // Create a map of table names to a map of "Diff Type" to diff
        // TODO: It might be better to sort the rows in a diff by pagenum and rownum
        // We'd be able to do much quicker merges
        let mut map: HashMap<String, HashMap<i32, Diff>> = HashMap::new();
        for commit in commits {
            for diff in &commit.diffs {
                match diff {
                    Diff::Update(_) | Diff::Insert(_) | Diff::Remove(_) => {
                        let table_name = diff.get_table_name();
                        let diff_type = diff.get_type();
                        if let Some(existing) = get_diff(&map, &table_name, diff_type) {
                            let curr_rows = diff.get_rows()?;
                            let schema = diff.get_schema();
                            // Merge the diffs together, removing any rows that are in the new update
                            let rows = existing
                                .get_rows()?
                                .iter()
                                .chain(curr_rows.iter())
                                .cloned()
                                .collect::<Vec<RowInfo>>();
                            // Update the update diff with the new rows
                            add_diff(
                                &mut map,
                                Diff::Update(UpdateDiff {
                                    table_name: table_name.clone(),
                                    schema,
                                    rows,
                                }),
                                table_name.clone(),
                            );
                        } else {
                            add_diff(&mut map, diff.clone(), table_name);
                        }
                    }
                    Diff::TableCreate(create) => {
                        add_diff(&mut map, diff.clone(), create.table_name.clone());
                    }
                    Diff::TableRemove(remove) => {
                        add_diff(&mut map, diff.clone(), remove.table_name.clone());
                    }
                }
            }
        }
        let diffs: Vec<Diff> = map
            .into_values()
            .map(|y| y.into_values())
            .flatten()
            .collect();
        self.create_commit(msg, cmd, diffs)
    }
}

// Here, <'a> means that the lifetime of the returned value is the same as the lifetime of the
// reference to the HashMap. This is a way to tell the compiler that the returned value will not
// outlive the HashMap.
fn get_diff<'a>(
    map: &'a HashMap<String, HashMap<i32, Diff>>,
    table_name: &'a String,
    diff_type: i32,
) -> Option<&'a Diff> {
    if let Some(table) = map.get(table_name) {
        if let Some(diff) = table.get(&diff_type) {
            return Some(diff);
        }
    }
    None
}

fn add_diff(map: &mut HashMap<String, HashMap<i32, Diff>>, diff: Diff, table_name: String) {
    let diff_type = diff.get_type();
    if let Some(table) = map.get_mut(&table_name) {
        if let Some(existing) = table.get_mut(&diff_type) {
            *existing = diff;
        } else {
            table.insert(diff_type, diff);
        }
    } else {
        let mut table = HashMap::new();
        table.insert(diff_type, diff);
        map.insert(table_name, table);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        executor::query::{create_table, insert},
        fileio::databaseio::{create_db_instance, delete_db_instance, get_db_instance, Database},
        user::userdata::User,
        util::row::RowLocation,
    };

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
    fn test_remove_commits() {
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
            vec![
                Diff::TableCreate(TableCreateDiff {
                    table_name: "test_table".to_string(),
                    schema: schema.clone(),
                }),
                Diff::TableRemove(TableRemoveDiff {
                    table_name: "test_table".to_string(),
                    schema: schema.clone(),
                    rows_removed: vec![],
                }),
            ],
        );

        let commit2 = Commit::new(
            "hasher2".to_string(),
            "test_timestamp".to_string(),
            "test_message".to_string(),
            "test_command".to_string(),
            vec![Diff::Remove(RemoveDiff {
                table_name: "test_table".to_string(),
                rows: vec![RowInfo {
                    row: vec![
                        Value::String("test".to_string()),
                        Value::String("122".to_string()),
                    ],
                    pagenum: 0,
                    rownum: 0,
                }],
                schema: schema.clone(),
            })],
        );
        delta.store_commit(&commit).unwrap();
        delta.store_commit(&commit2).unwrap();
        let commit4 = delta.fetch_commit(&"hasher2".to_string()).unwrap();
        assert_eq!(commit2, commit4);
        let commit3 = delta.fetch_commit(&"test_hash".to_string()).unwrap();
        assert_eq!(commit, commit3);

        // Delete the test files
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_insert_commits() {
        let new_db: Database = Database::new("commit_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(225)),
            ("age".to_string(), Column::I32),
        ];

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let mut rows = vec![
            vec!["1".to_string(), "Iron Man".to_string(), "40".to_string()],
            vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "35".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "100".to_string(),
            ],
            vec!["5".to_string(), "Thor".to_string(), "1000".to_string()],
        ];
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        let (x, y) = rows.split_at(21); // 40 rows
        let (_, diff1) = insert(x.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let (_, diff2) = insert(y.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let commit1 = Commit::new(
            "hash1".to_string(),
            "timestamp1".to_string(),
            "message1".to_string(),
            "cmd1".to_string(),
            vec![Diff::TableCreate(TableCreateDiff {
                table_name: "test_table".to_string(),
                schema: schema.clone(),
            })],
        );
        let commit2 = Commit::new(
            "hash2".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff1)],
        );
        let commit3 = Commit::new(
            "hash3".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff2)],
        );

        new_db.delete_database().unwrap();

        let mut delta = CommitFile::new(&"".to_string(), true).unwrap();
        delta.store_commit(&commit1).unwrap();
        delta.store_commit(&commit2).unwrap();
        delta.store_commit(&commit3).unwrap();
        let commit13 = delta.fetch_commit(&"hash3".to_string()).unwrap();
        assert_eq!(commit3, commit13);
        let commit11 = delta.fetch_commit(&"hash1".to_string()).unwrap();
        assert_eq!(commit1, commit11);
        let commit12 = delta.fetch_commit(&"hash2".to_string()).unwrap();
        assert_eq!(commit2, commit12);

        // Delete the test database
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_commit_commands() {
        let new_db: Database = Database::new("commit_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(225)),
            ("age".to_string(), Column::I32),
        ];

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let mut rows = vec![
            vec!["1".to_string(), "Nick Fury".to_string(), "40".to_string()],
            vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "35".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "100".to_string(),
            ],
            vec!["5".to_string(), "Thor".to_string(), "1000".to_string()],
        ];
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        let (x, y) = rows.split_at(21); // 40 rows
        let (_, diff1) = insert(x.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let (_, diff2) = insert(y.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let commit1 = Commit::new(
            "hash1".to_string(),
            "timestamp1".to_string(),
            "message1".to_string(),
            "cmd1".to_string(),
            vec![Diff::TableCreate(TableCreateDiff {
                table_name: "test_table".to_string(),
                schema: schema.clone(),
            })],
        );
        let commit2 = Commit::new(
            "hash2".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff1)],
        );
        let commit3 = Commit::new(
            "hash3".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff2)],
        );
        new_db.delete_database().unwrap();
        let mut delta = CommitFile::new(&"".to_string(), true).unwrap();
        delta.store_commit(&commit1).unwrap();
        delta.store_commit(&commit2).unwrap();
        delta.store_commit(&commit3).unwrap();
        let commit13 = delta.fetch_commit(&"hash3".to_string()).unwrap();
        assert_eq!(commit3.command, commit13.command);
        let commit11 = delta.fetch_commit(&"hash1".to_string()).unwrap();
        assert_eq!(commit1.command, commit11.command);
        let commit12 = delta.fetch_commit(&"hash2".to_string()).unwrap();
        assert_eq!(commit2.command, commit12.command);

        // Delete the test database
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_commit_message() {
        let new_db: Database = Database::new("commit_test_db".to_string()).unwrap();
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(225)),
            ("age".to_string(), Column::I32),
        ];

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        create_table(&"test_table1".to_string(), &schema, &new_db, &mut user).unwrap();
        let mut rows = vec![
            vec!["1".to_string(), "Nick Fury".to_string(), "40".to_string()],
            vec!["2".to_string(), "Spiderman".to_string(), "20".to_string()],
            vec![
                "3".to_string(),
                "Doctor Strange".to_string(),
                "35".to_string(),
            ],
            vec![
                "4".to_string(),
                "Captain America".to_string(),
                "100".to_string(),
            ],
            vec!["5".to_string(), "Thor".to_string(), "1000".to_string()],
        ];
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        rows.extend_from_within(0..);
        let (x, y) = rows.split_at(21); // 40 rows
        let (_, diff1) = insert(x.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let (_, diff2) = insert(y.to_vec(), "test_table1".to_string(), &new_db, &mut user).unwrap();
        let commit1 = Commit::new(
            "hash1".to_string(),
            "timestamp1".to_string(),
            "message1".to_string(),
            "cmd1".to_string(),
            vec![Diff::TableCreate(TableCreateDiff {
                table_name: "test_table".to_string(),
                schema: schema.clone(),
            })],
        );
        let commit2 = Commit::new(
            "hash2".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff1)],
        );
        let commit3 = Commit::new(
            "hash3".to_string(),
            "timestamp2".to_string(),
            "message2".to_string(),
            "cmd2".to_string(),
            vec![Diff::Insert(diff2)],
        );
        new_db.delete_database().unwrap();
        let mut delta = CommitFile::new(&"".to_string(), true).unwrap();
        delta.store_commit(&commit1).unwrap();
        delta.store_commit(&commit2).unwrap();
        delta.store_commit(&commit3).unwrap();
        let commit13 = delta.fetch_commit(&"hash3".to_string()).unwrap();
        assert_eq!(commit3.message, commit13.message);
        let commit11 = delta.fetch_commit(&"hash1".to_string()).unwrap();
        assert_eq!(commit1.message, commit11.message);
        let commit12 = delta.fetch_commit(&"hash2".to_string()).unwrap();
        assert_eq!(commit2.message, commit12.message);

        // Delete the test database
        std::fs::remove_file(delta.delta_path).unwrap();
        std::fs::remove_file(delta.header_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_squash() {
        // This will test squashing 2 commits together
        let db_name: String = "test_squash_db".to_string();
        let table_name1: String = "table1".to_string();

        // Create a new database
        create_db_instance(&db_name).unwrap();

        // Create a new user
        let mut user: User = User::new("test_user".to_string());

        // Create the table on the main branch
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table1_diff) =
            create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();
        user.append_diff(&Diff::TableCreate(table1_diff));

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![Value::I32(1), Value::String("FirstRow".to_string())]);
        rows.push(vec![Value::I32(2), Value::String("SecondRow".to_string())]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Create a commit on main branch
        let (_, commit1) = get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on Main Branch".to_string(),
                &"Create Table;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![Value::I32(3), Value::String("ThirdRow".to_string())]);
        rows.push(vec![Value::I32(4), Value::String("FourthRow".to_string())]);
        rows.push(vec![Value::I32(5), Value::String("FifthRow".to_string())]);
        rows.push(vec![Value::I32(6), Value::String("SixthRow".to_string())]);
        rows.push(vec![Value::I32(7), Value::String("SeventhRow".to_string())]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Update some of those rows
        let table1_update_diff: UpdateDiff = table1
            .rewrite_rows(vec![
                RowInfo {
                    pagenum: 1,
                    rownum: 2,
                    row: vec![Value::I32(3), Value::String("ThirdRowUpdated".to_string())],
                },
                RowInfo {
                    pagenum: 1,
                    rownum: 3,
                    row: vec![Value::I32(4), Value::String("FourthRowUpdated".to_string())],
                },
            ])
            .unwrap();
        user.append_diff(&Diff::Update(table1_update_diff));

        // Remove the fourth row
        let table1_remove_diff: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 3,
            }])
            .unwrap();
        user.append_diff(&Diff::Remove(table1_remove_diff));

        // Create a commit on main branch
        let (_, commit2) = get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"Second Commit on Main Branch".to_string(),
                &"Insert, Update, and Remove Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        let squash: Commit = get_db_instance()
            .unwrap()
            .get_commit_file_mut()
            .squash_commits(&vec![commit1, commit2])
            .unwrap();

        let diffs: Vec<Diff> = squash.diffs;

        // At this point, we've:
        // 1. Created a table
        // 2. Inserted 2 rows
        // 3. Committed
        // 4. Inserted 5 rows
        // 5. Updated 2 rows (the 3rd and 4th rows)
        // 6. Removed 1 row (the 4th row)
        // 7. Committed
        // We should only have 2 diffs where there is a table creation, and an insert of 6 rows

        // Assert that the diffs are corrects
        // Should only contain tablecreate and insert diffs
        assert_eq!(diffs.len(), 2);

        // Get the tablecreate diff from diffs
        let tablecreate_diff: &Diff = diffs
            .iter()
            .find(|diff| match diff {
                Diff::TableCreate(_) => true,
                _ => false,
            })
            .unwrap();

        // Assert that the tablecreate diff is correct
        assert_eq!(tablecreate_diff.get_schema(), table1.schema);
        assert_eq!(tablecreate_diff.get_table_name(), table1.name);

        // Get the insert diff from diffs
        let insert_diff: &Diff = diffs
            .iter()
            .find(|diff| match diff {
                Diff::Insert(_) => true,
                _ => false,
            })
            .unwrap();
        if let Diff::Insert(insert_diff) = insert_diff.clone() {
            // Assert that the insert diff is correct
            assert_eq!(insert_diff.table_name, table1.name);

            // Assert there are only 6 rows to insert
            assert_eq!(insert_diff.rows.len(), 6);

            // Assert that the first row is correct
            assert_eq!(
                insert_diff.rows[0].row,
                vec![Value::I32(1), Value::String("FirstRow".to_string())]
            );

            // Assert that the second row is correct
            assert_eq!(
                insert_diff.rows[1].row,
                vec![Value::I32(2), Value::String("SecondRow".to_string())]
            );

            // Assert that the third row is correct
            assert_eq!(
                insert_diff.rows[2].row,
                vec![Value::I32(3), Value::String("ThirdRowUpdated".to_string())]
            );

            // The fourth row was removed, so it should not be in the insert diff

            // Assert that the fifth row is correct
            assert_eq!(
                insert_diff.rows[3].row,
                vec![Value::I32(5), Value::String("FifthRow".to_string())]
            );

            // Assert that the sixth row is correct
            assert_eq!(
                insert_diff.rows[4].row,
                vec![Value::I32(6), Value::String("SixthRow".to_string())]
            );

            // Assert that the seventh row is correct
            assert_eq!(
                insert_diff.rows[5].row,
                vec![Value::I32(7), Value::String("SeventhRow".to_string())]
            );
        } else {
            panic!("Insert diff was not found");
        }

        // Delete the database
        delete_db_instance().unwrap();
    }
}
