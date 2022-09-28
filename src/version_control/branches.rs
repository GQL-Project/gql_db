use crate::fileio::{
    databaseio::{self, *},
    header::*,
    pageio::*,
    rowio::*,
    tableio::*,
};
use crate::util::{dbtype::*, row::*};

#[derive(Clone)]
pub struct BranchNode {
    pub branch_name: String, // The name of the branch this node is on.
    pub commit_hash: String, // The commit hash that this node is associated with.
    pub prev_pagenum: i32,   // The page number of the previous branch node.
    pub prev_rownum: i32,    // The row number of the previous branch node.
}

/// This is designed to represent the branches.gql file for a database.
#[derive(Clone)]
pub struct BranchesFile {
    filepath: String,
    branches_table: Table,
}

impl BranchNode {
    /// Create a new BranchNode from a row of data read from the branches.gql table
    pub fn new(row: Row) -> Result<Self, String> {
        let branch_name: String = match row.get(0) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid branch name".to_string()),
        };
        let commit_hash: String = match row.get(1) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid commit hash".to_string()),
        };
        let page_num: i32 = match row.get(2) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string()),
        };
        let row_num: i32 = match row.get(3) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string()),
        };

        Ok(Self {
            branch_name,
            commit_hash,
            prev_pagenum: page_num,
            prev_rownum: row_num,
        })
    }
}

impl BranchesFile {
    /// Creates a new BranchesFile object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: String, create_file: bool) -> Result<BranchesFile, String> {
        // Get filepath info
        let branch_filename: String = format!(
            "{}{}",
            databaseio::BRANCHES_FILE_NAME.to_string(),
            databaseio::BRANCHES_FILE_EXTENSION.to_string()
        );
        let filepath: String = format!(
            "{}{}{}",
            dir_path,
            std::path::MAIN_SEPARATOR,
            branch_filename
        );

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("branch_name".to_string(), Column::String(60)),
                ("commit_hash".to_string(), Column::String(32)),
                ("page_num".to_string(), Column::I32),
                ("row_num".to_string(), Column::I32),
            ];
            let header = Header {
                num_pages: 2,
                schema,
            };
            write_header(&filepath, &header)?;

            // Write a blank page to the table
            let page = [0u8; PAGE_SIZE];
            write_page(1, &filepath, &page)?;
        }

        Ok(BranchesFile {
            filepath: filepath.clone(),
            branches_table: Table::new(
                &dir_path.clone(),
                &databaseio::BRANCHES_FILE_NAME.to_string(),
                Some(&databaseio::BRANCHES_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    /// Returns a branch node from the given page row and number.
    pub fn get_branch_node(&self, row_location: RowLocation) -> Result<BranchNode, String> {
        let row = get_row(&self.branches_table, row_location)?;
        Ok(BranchNode::new(row)?)
    }

    /// Returns the previous branch node from the given branch node
    pub fn get_prev_branch_node(&self, branch_node: &BranchNode) -> Result<BranchNode, String> {
        let row_location = RowLocation {
            pagenum: branch_node.prev_pagenum as u32,
            rownum: branch_node.prev_rownum as u16,
        };
        self.get_branch_node(row_location)
    }
}
