use crate::util::{dbtype::*, row::*};
use crate::fileio::{tableio::*, header::*, pageio::*, *};

#[derive(Clone)]
pub struct BranchHead {
    pub branch_name: String,
    pub page_num: i32,
    pub row_num: i32,
}

/// This is designed to represent the branch_heads.gql file for a database.
#[derive(Clone)]
pub struct BranchHEADs {
    filepath: String,
    branch_heads_table: Table
}


impl BranchHEADs {
    /// Creates a new BranchHEADs object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(
        dir_path: &String, 
        create_file: bool
    ) -> Result<BranchHEADs, String> {
        // Get filepath info
        let branch_filename: String = format!("{}{}", databaseio::BRANCH_HEADS_FILE_NAME.to_string(), databaseio::BRANCH_HEADS_FILE_EXTENSION.to_string());
        let filepath: String;
        if dir_path.len() == 0 {
            filepath = branch_filename.clone();
        } else {
            filepath = format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, branch_filename);
        }

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("branch_name".to_string(), Column::String(60)),
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
        
        Ok(BranchHEADs {
            filepath: filepath.clone(),
            branch_heads_table: Table::new(&dir_path.clone(), 
                &databaseio::BRANCH_HEADS_FILE_NAME.to_string(),
                Some(&databaseio::BRANCH_HEADS_FILE_EXTENSION.to_string()))?
        })
    }
    
    
    /// Takes in a branch name and returns the corresponding branch head
    pub fn get_branch_head(
        &mut self, 
        branch_name: &String
    ) -> Result<BranchHead, String> {
        let branch_heads: Vec<BranchHead> = self.get_all_branch_heads()?;

        for branch_head in branch_heads {
            if branch_head.branch_name == *branch_name {
                return Ok(branch_head);
            }
        }

        return Err("Branch name not present in branch HEADs file".to_string())
    }


    /// Read the branch heads file and return a vector of BranchHead structs
    pub fn get_all_branch_heads(
        &mut self
    ) -> Result<Vec<BranchHead>, String> {
        let mut branch_heads: Vec<BranchHead> = Vec::new();
    
        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.row;
    
            let branch_name: String;
            let page_num: i32;
            let row_num: i32;
    
            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string())
            }
    
            // Get the page number
            match row.get(1) {
                Some(Value::I32(p_num)) => page_num = p_num.clone(),
                _ => return Err("Error: Page number not found".to_string())
            }
    
            // Get the row number
            match row.get(2) {
                Some(Value::I32(r_num)) => row_num = r_num.clone(),
                _ => return Err("Error: Row number not found".to_string())
            }
    
            let branch_head: BranchHead = BranchHead {
                branch_name: branch_name,
                page_num: page_num,
                row_num: row_num,
            };
    
            branch_heads.push(branch_head);
        }
    
        Ok(branch_heads)
    }


    /// Writes a new branch head to the branch heads file
    pub fn write_new_branch_head(
        &mut self, 
        branch_head: &BranchHead
    ) -> Result<(), String> {
        let rows: Vec<Vec<Value>> = vec![
            // Just make one new row
            vec![
                Value::String(branch_head.branch_name.clone()),
                Value::I32(branch_head.page_num),
                Value::I32(branch_head.row_num)
            ],
        ];
        insert_rows(&mut self.branch_heads_table, rows)?;
        Ok(())
    }

    /// Takes in a BranchHead object and updates the branch head in the branch heads file
    /// that corresponds to the branch name in the BranchHead object
    pub fn update_branch_head(
        &mut self, 
        branch_head: &BranchHead
    ) -> Result<(), String> {
        // Iterate through all the rows in the branch heads file and check to see if there is a row that has
        // the same branch name as the branch head we are trying to update
        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.clone().row;
    
            let row_branch_name: String;
    
            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => row_branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string())
            }
    
            // If the branch name matches
            if row_branch_name == branch_head.branch_name.clone() {
                // Create a new row with the updated values
                let updated_row_info: RowInfo = RowInfo {
                    row: vec![
                        Value::String(branch_head.branch_name.clone()),
                        Value::I32(branch_head.page_num),
                        Value::I32(branch_head.row_num)
                    ],
                    pagenum: row_info.pagenum,
                    rownum: row_info.rownum,
                };
        
                // Update the row in the branch heads file
                rewrite_rows(&mut self.branch_heads_table, vec![updated_row_info])?;
    
                return Ok(())
            }
        }
    
        // The branch name was not present in the branch heads file
        Err("Error: Branch name was not present".to_string())
    }


    /// Deletes a branch head from the branch heads file
    /// Returns an error if the branch name is not present in the branch heads file
    pub fn delete_branch_head(
        &mut self, 
        branch_name: &String
    ) -> Result<(), String> {
        // Iterate through all the rows in the branch heads file and check to see if there is a row that has
        // the same branch name as the branch head we are trying to delete
        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.clone().row;
    
            let row_branch_name: String;
    
            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => row_branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string())
            }
    
            // If the branch name matches, delete the row
            if row_branch_name == *branch_name {
                remove_rows(&mut self.branch_heads_table, vec![(row_info.pagenum, row_info.rownum)])?;
                return Ok(())
            }
        }
    
        // The branch name was not present in the branch heads file
        Err("Error: Branch name was not present".to_string())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use crate::fileio::databaseio::*;

    #[test]
    #[serial]
    fn test_creating_branch_heads() {
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head = BranchHead {
            branch_name: "main".to_string(),
            page_num: 1,
            row_num: 1,
        };

        branch_heads.write_new_branch_head(&branch_head).unwrap();

        let branch_heads = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_heads.len(), 1);
        assert_eq!(branch_heads[0].branch_name, "main");
        assert_eq!(branch_heads[0].page_num, 1);
        assert_eq!(branch_heads[0].row_num, 1);

        // Delete the test file
        std::fs::remove_file(format!("{}{}", BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION)).unwrap();
    }

    #[test]
    #[serial]
    fn test_creating_multiple_branch_heads() {
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            page_num: 1,
            row_num: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            page_num: 2,
            row_num: 2,
        };

        branch_heads.write_new_branch_head(&branch_head1).unwrap();
        branch_heads.write_new_branch_head(&branch_head2).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].page_num, 1);
        assert_eq!(branch_head_list[0].row_num, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].page_num, 2);
        assert_eq!(branch_head_list[1].row_num, 2);

        let test_branch_head: BranchHead = branch_heads.get_branch_head(&"test".to_string()).unwrap();

        assert_eq!(test_branch_head.branch_name, "test");
        assert_eq!(test_branch_head.page_num, 2);
        assert_eq!(test_branch_head.row_num, 2);

        // Delete the test file
        std::fs::remove_file(format!("{}{}", BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION)).unwrap();
    }

    #[test]
    #[serial]
    fn test_updating_branch_head() {
        let branch_heads_file: String = "test_update_branch_heads_file".to_string();
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            page_num: 1,
            row_num: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            page_num: 2,
            row_num: 2,
        };

        branch_heads.write_new_branch_head(&branch_head1).unwrap();
        branch_heads.write_new_branch_head(&branch_head2).unwrap();

        let branch_head3 = BranchHead {
            branch_name: "test".to_string(),
            page_num: 5,
            row_num: 16,
        };

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].page_num, 1);
        assert_eq!(branch_head_list[0].row_num, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].page_num, 2);
        assert_eq!(branch_head_list[1].row_num, 2);

        branch_heads.update_branch_head(&branch_head3).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].page_num, 1);
        assert_eq!(branch_head_list[0].row_num, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].page_num, 5);
        assert_eq!(branch_head_list[1].row_num, 16);

        // Delete the test file
        std::fs::remove_file(format!("{}{}", BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION)).unwrap();
    }

    #[test]
    #[serial]
    fn test_deleting_branch_head() {
        let branch_heads_file: String = "test_delete_branch_heads_file".to_string();
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            page_num: 1,
            row_num: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            page_num: 2,
            row_num: 2,
        };

        branch_heads.write_new_branch_head(&branch_head1).unwrap();
        branch_heads.write_new_branch_head(&branch_head2).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].page_num, 1);
        assert_eq!(branch_head_list[0].row_num, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].page_num, 2);
        assert_eq!(branch_head_list[1].row_num, 2);

        branch_heads.delete_branch_head(&"test".to_string()).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 1);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].page_num, 1);
        assert_eq!(branch_head_list[0].row_num, 1);

        // Delete the test file
        std::fs::remove_file(format!("{}{}", BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION)).unwrap();
    }

}