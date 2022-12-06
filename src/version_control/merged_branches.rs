use std::collections::HashMap;

use crate::fileio::{header::*, pageio::*, tableio::*, *};
use crate::util::{dbtype::*, row::*};

/// This represents a merged branch. It is a single row in the `merged_branches.gql` table.
#[derive(Clone, Debug)]
pub struct MergedBranch {
    pub branch_name: String,        // The name of the branch that was merged.
    pub source_commit: String, // The commit hash that this branch was originally branched from.
    pub destination_commit: String, // The commit hash that this branch was merged into.
}

/// This is designed to represent the merged_branches.gql file for a database.
#[derive(Clone)]
pub struct MergedBranchesFile {
    filepath: String,
    merged_branches_table: Table,
}

impl MergedBranchesFile {
    /// Creates a new MergedBranchesFile object to store the merged branches for the database.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: &String, create_file: bool) -> Result<MergedBranchesFile, String> {
        // Get filepath info
        let branch_filename: String = format!(
            "{}{}",
            databaseio::MERGED_BRANCHES_FILE_NAME.to_string(),
            databaseio::MERGED_BRANCHES_FILE_EXTENSION.to_string()
        );
        let filepath: String;
        if dir_path.len() == 0 {
            filepath = branch_filename.clone();
        } else {
            filepath = format!(
                "{}{}{}",
                dir_path,
                std::path::MAIN_SEPARATOR,
                branch_filename
            );
        }

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("branch_name".to_string(), Column::String(60)),
                ("source_commit".to_string(), Column::String(50)),
                ("destination_commit".to_string(), Column::String(50)),
            ];
            let header = Header {
                num_pages: 2,
                schema,
                index_top_level_pages: HashMap::new(),
            };
            write_header(&filepath, &header)?;

            // Write a blank page to the table
            let page: Page = [0u8; PAGE_SIZE];
            write_page(1, &filepath, &page, PageType::Data)?;
        }

        Ok(MergedBranchesFile {
            filepath: filepath.clone(),
            merged_branches_table: Table::new(
                &dir_path.clone(),
                &databaseio::MERGED_BRANCHES_FILE_NAME.to_string(),
                Some(&databaseio::MERGED_BRANCHES_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    /// Inserts a new merged branch into the merged_branches.gql file.
    pub fn insert_merged_branch(
        &mut self,
        branch_name: &String,
        source_commit: &String,
        destination_commit: &String,
    ) -> Result<(), String> {
        let row: Row = vec![
            Value::String(branch_name.clone()),
            Value::String(source_commit.clone()),
            Value::String(destination_commit.clone()),
        ];
        self.merged_branches_table.insert_rows(vec![row])?;
        Ok(())
    }

    /// Returns a list of all merged branches.
    pub fn get_merged_branches(&self) -> Result<Vec<MergedBranch>, String> {
        let mut merged_branches: Vec<MergedBranch> = Vec::new();
        let rows: Vec<RowInfo> = self.merged_branches_table.clone().into_iter().collect();
        for rowinfo in rows {
            let branch_name: String = match rowinfo.row.get(0) {
                Some(Value::String(val)) => val.clone(),
                _ => return Err("get_merged_branches: Could not get index 0".to_string()),
            };
            let source_commit: String = match rowinfo.row.get(1) {
                Some(Value::String(val)) => val.clone(),
                _ => return Err("get_merged_branches: Could not get index 1".to_string()),
            };
            let destination_commit: String = match rowinfo.row.get(2) {
                Some(Value::String(val)) => val.clone(),
                _ => return Err("get_merged_branches: Could not get index 2".to_string()),
            };
            merged_branches.push(MergedBranch {
                branch_name,
                source_commit,
                destination_commit,
            });
        }
        Ok(merged_branches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileio::databaseio::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_creating_branch_heads() {
        let mut merged_branches_file: MergedBranchesFile =
            MergedBranchesFile::new(&"".to_string(), true).unwrap();

        let merged_branch: MergedBranch = MergedBranch {
            branch_name: "main".to_string(),
            source_commit: "abc".to_string(),
            destination_commit: "def".to_string(),
        };

        merged_branches_file
            .insert_merged_branch(
                &merged_branch.branch_name,
                &merged_branch.source_commit,
                &merged_branch.destination_commit,
            )
            .unwrap();

        let merged_branches: Vec<MergedBranch> =
            merged_branches_file.get_merged_branches().unwrap();

        assert_eq!(merged_branches.len(), 1);
        assert_eq!(merged_branches[0].branch_name, "main");
        assert_eq!(merged_branches[0].source_commit, "abc");
        assert_eq!(merged_branches[0].destination_commit, "def");

        // Delete the test file
        std::fs::remove_file(format!(
            "{}{}",
            MERGED_BRANCHES_FILE_NAME, MERGED_BRANCHES_FILE_EXTENSION
        ))
        .unwrap();
    }
}
