use crate::fileio::{databaseio::{*, self}, header::*, pageio::*, rowio::*, tableio::*};
use crate::util::{dbtype::*, row::*};

#[derive(Clone)]
pub struct BranchNode {
    pub commit_hash: String,
    pub page_num: i32,
    pub row_num: i32,
    pub branch_name: String
}

/// This is designed to represent the branches.gql file for a database.
#[derive(Clone)]
pub struct BranchesFile {
    filepath: String,
    branches_table: Table
}


impl BranchesFile {
    /// Creates a new BranchesFile object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(
        dir_path: String, 
        create_file: bool
    ) -> Result<BranchesFile, String> {
        // Get filepath info
        let branch_filename: String = format!("{}{}", databaseio::BRANCHES_FILE_NAME.to_string(), databaseio::BRANCHES_FILE_EXTENSION.to_string());
        let filepath: String = format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, branch_filename);

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("commit_hash".to_string(), Column::String(32)),
                ("page_num".to_string(), Column::I32),
                ("row_num".to_string(), Column::I32),
                ("branch_name".to_string(), Column::String(60))
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
            branches_table: Table::new(&dir_path.clone(), 
                &databaseio::BRANCHES_FILE_NAME.to_string(),
                Some(&databaseio::BRANCHES_FILE_EXTENSION.to_string()))?
        })
    }
}
