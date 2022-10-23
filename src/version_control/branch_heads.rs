use crate::fileio::{header::*, pageio::*, tableio::*, *};
use crate::util::{dbtype::*, row::*};
use crate::version_control::branches::*;

/// This represents a branch head. It is a single row in the `branch_heads.gql` table.
/// It points to a branch node in the `branches.gql` table.
#[derive(Clone)]
pub struct BranchHead {
    pub branch_name: String, // The name of the branch that this head points to.
    pub pagenum: i32,        // The page number in `branches.gql` where the branch node is located.
    pub rownum: i32,         // The row number in `branches.gql` where the branch node is located.
}

/// This is designed to represent the branch_heads.gql file for a database.
#[derive(Clone)]
pub struct BranchHEADs {
    filepath: String,
    branch_heads_table: Table,
}

impl BranchHead {
    /// Gets the branch node location within `branches.gql` from a given branch HEAD
    pub fn get_branch_node_location(&self) -> RowLocation {
        RowLocation {
            pagenum: self.pagenum as u32,
            rownum: self.rownum as u16,
        }
    }
}

impl BranchHEADs {
    /// Creates a new BranchHEADs object to store the branch heads for the database.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: &String, create_file: bool) -> Result<BranchHEADs, String> {
        // Get filepath info
        let branch_filename: String = format!(
            "{}{}",
            databaseio::BRANCH_HEADS_FILE_NAME.to_string(),
            databaseio::BRANCH_HEADS_FILE_EXTENSION.to_string()
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
            branch_heads_table: Table::new(
                &dir_path.clone(),
                &databaseio::BRANCH_HEADS_FILE_NAME.to_string(),
                Some(&databaseio::BRANCH_HEADS_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    // Immutable getter access to filepath.
    pub fn filepath(&self) -> &str {
        &self.filepath
    }

    /// Takes in a branch name and returns the corresponding branch HEAD.
    /// If the branch name does not exist, returns an error.
    pub fn get_branch_head(&mut self, branch_name: &String) -> Result<BranchHead, String> {
        let branch_heads: Vec<BranchHead> = self.get_all_branch_heads()?;

        for branch_head in branch_heads {
            if branch_head.branch_name == *branch_name {
                return Ok(branch_head);
            }
        }

        return Err(format!(
            "Branch name '{}' not present in branch HEADs file",
            branch_name
        )
        .to_string());
    }

    /// Returns the branch node for the HEAD of the given branch.
    pub fn get_branch_node_from_head(
        &mut self,
        branch_name: &String,
        branches: &Branches,
    ) -> Result<BranchNode, String> {
        let branch_head: &mut BranchHead = &mut self.get_branch_head(branch_name)?;
        Ok(branches.get_branch_node(&RowLocation {
            pagenum: branch_head.pagenum as u32,
            rownum: branch_head.rownum as u16,
        })?)
    }

    /// Returns a list of all branches on the database
    pub fn get_all_branch_names(&mut self) -> Result<Vec<String>, String> {
        let mut branch_names: Vec<String> = Vec::new();

        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.row;

            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => branch_names.push(br_name.to_string()),
                _ => return Err("Error: Branch name not found".to_string()),
            }
        }
        Ok(branch_names)
    }

    /// Returns if the branch exists in the database
    pub fn does_branch_exist(&mut self, name: String) -> Result<bool, String> {
        let vec_name = self.get_all_branch_names()?;
        for branch_name in vec_name {
            if branch_name == name {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Read the branch heads file and return a vector of BranchHead objects
    pub fn get_all_branch_heads(&mut self) -> Result<Vec<BranchHead>, String> {
        let mut branch_heads: Vec<BranchHead> = Vec::new();

        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.row;

            let branch_name: String;
            let page_num: i32;
            let row_num: i32;

            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string()),
            }

            // Get the page number
            match row.get(1) {
                Some(Value::I32(p_num)) => page_num = p_num.clone(),
                _ => return Err("Error: Page number not found".to_string()),
            }

            // Get the row number
            match row.get(2) {
                Some(Value::I32(r_num)) => row_num = r_num.clone(),
                _ => return Err("Error: Row number not found".to_string()),
            }

            let branch_head: BranchHead = BranchHead {
                branch_name: branch_name,
                pagenum: page_num,
                rownum: row_num,
            };

            branch_heads.push(branch_head);
        }

        Ok(branch_heads)
    }

    /// Writes a new branch head to the branch heads file.
    /// Returns an error if a branch head with the given name already exists.
    pub fn create_branch_head(&mut self, branch_head: &BranchHead) -> Result<(), String> {
        // Make sure that a branch head doesn't already have the same branch name
        let branch_heads: Vec<BranchHead> = self.get_all_branch_heads()?;
        for branch in branch_heads {
            if branch.branch_name == branch_head.branch_name {
                return Err("Error: Branch name already exists".to_string());
            }
        }

        let rows: Vec<Vec<Value>> = vec![
            // Just make one new row
            vec![
                Value::String(branch_head.branch_name.clone()),
                Value::I32(branch_head.pagenum),
                Value::I32(branch_head.rownum),
            ],
        ];
        self.branch_heads_table.insert_rows(rows)?;
        Ok(())
    }

    /// Takes in a BranchHead object and updates the branch head in the branch heads file
    /// that corresponds to the branch name in the BranchHead object.
    pub fn update_branch_head(&mut self, branch_head: &BranchHead) -> Result<(), String> {
        // Iterate through all the rows in the branch heads file and check to see if there is a row that has
        // the same branch name as the branch head we are trying to update
        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.clone().row;

            let row_branch_name: String;

            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => row_branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string()),
            }

            // If the branch name matches
            if row_branch_name == branch_head.branch_name.clone() {
                // Create a new row with the updated values
                let updated_row_info: RowInfo = RowInfo {
                    row: vec![
                        Value::String(branch_head.branch_name.clone()),
                        Value::I32(branch_head.pagenum),
                        Value::I32(branch_head.rownum),
                    ],
                    pagenum: row_info.pagenum,
                    rownum: row_info.rownum,
                };

                // Update the row in the branch heads file
                self.branch_heads_table
                    .rewrite_rows(vec![updated_row_info])?;

                return Ok(());
            }
        }

        // The branch name was not present in the branch heads file
        Err("Error: Branch name was not present".to_string())
    }

    /// Set branch head object to point to a new branch node within `branches.gql`
    /// This function is used when an existing branch gets a new branch node appended to it.
    /// Note that the Branch Node itself needs to be updated to ensure it is also
    /// marked as a branch head
    pub fn set_branch_head(
        &mut self,
        branch_name: &String,
        new_branch_node_loc: &RowLocation,
    ) -> Result<(), String> {
        // Get the branch head
        let mut branch_head: BranchHead = self.get_branch_head(branch_name)?;

        // Update the branch head
        branch_head.pagenum = new_branch_node_loc.pagenum as i32;
        branch_head.rownum = new_branch_node_loc.rownum as i32;

        // Update the branch head in the branch heads file
        self.update_branch_head(&branch_head)?;
        Ok(())
    }

    /// Deletes a branch head from the branch heads file
    /// Returns an error if the branch name is not present in the branch heads file
    pub fn delete_branch_head(&mut self, branch_name: &String) -> Result<(), String> {
        // Iterate through all the rows in the branch heads file and check to see if there is a row that has
        // the same branch name as the branch head we are trying to delete
        for row_info in self.branch_heads_table.by_ref().into_iter().clone() {
            let row: Row = row_info.clone().row;

            let row_branch_name: String;

            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => row_branch_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string()),
            }

            // If the branch name matches, delete the row
            if row_branch_name == *branch_name {
                self.branch_heads_table.remove_rows(vec![RowLocation {
                    pagenum: row_info.pagenum,
                    rownum: row_info.rownum,
                }])?;
                return Ok(());
            }
        }

        // The branch name was not present in the branch heads file
        Err("Error: Branch name was not present".to_string())
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
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head = BranchHead {
            branch_name: "main".to_string(),
            pagenum: 1,
            rownum: 1,
        };

        branch_heads.create_branch_head(&branch_head).unwrap();

        let branch_heads = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_heads.len(), 1);
        assert_eq!(branch_heads[0].branch_name, "main");
        assert_eq!(branch_heads[0].pagenum, 1);
        assert_eq!(branch_heads[0].rownum, 1);

        // Delete the test file
        std::fs::remove_file(format!(
            "{}{}",
            BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION
        ))
        .unwrap();
    }

    #[test]
    #[serial]
    fn test_creating_multiple_branch_heads() {
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            pagenum: 1,
            rownum: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            pagenum: 2,
            rownum: 2,
        };

        branch_heads.create_branch_head(&branch_head1).unwrap();
        branch_heads.create_branch_head(&branch_head2).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].pagenum, 1);
        assert_eq!(branch_head_list[0].rownum, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].pagenum, 2);
        assert_eq!(branch_head_list[1].rownum, 2);

        let test_branch_head: BranchHead =
            branch_heads.get_branch_head(&"test".to_string()).unwrap();

        assert_eq!(test_branch_head.branch_name, "test");
        assert_eq!(test_branch_head.pagenum, 2);
        assert_eq!(test_branch_head.rownum, 2);

        // Delete the test file
        std::fs::remove_file(format!(
            "{}{}",
            BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION
        ))
        .unwrap();
    }

    #[test]
    #[serial]
    fn test_updating_branch_head() {
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            pagenum: 1,
            rownum: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            pagenum: 2,
            rownum: 2,
        };

        branch_heads.create_branch_head(&branch_head1).unwrap();
        branch_heads.create_branch_head(&branch_head2).unwrap();

        let branch_head3 = BranchHead {
            branch_name: "test".to_string(),
            pagenum: 5,
            rownum: 16,
        };

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].pagenum, 1);
        assert_eq!(branch_head_list[0].rownum, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].pagenum, 2);
        assert_eq!(branch_head_list[1].rownum, 2);

        branch_heads.update_branch_head(&branch_head3).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].pagenum, 1);
        assert_eq!(branch_head_list[0].rownum, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].pagenum, 5);
        assert_eq!(branch_head_list[1].rownum, 16);

        // Delete the test file
        std::fs::remove_file(format!(
            "{}{}",
            BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION
        ))
        .unwrap();
    }

    #[test]
    #[serial]
    fn test_deleting_branch_head() {
        let mut branch_heads: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();

        let branch_head1 = BranchHead {
            branch_name: "main".to_string(),
            pagenum: 1,
            rownum: 1,
        };

        let branch_head2 = BranchHead {
            branch_name: "test".to_string(),
            pagenum: 2,
            rownum: 2,
        };

        branch_heads.create_branch_head(&branch_head1).unwrap();
        branch_heads.create_branch_head(&branch_head2).unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 2);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].pagenum, 1);
        assert_eq!(branch_head_list[0].rownum, 1);
        assert_eq!(branch_head_list[1].branch_name, "test");
        assert_eq!(branch_head_list[1].pagenum, 2);
        assert_eq!(branch_head_list[1].rownum, 2);

        branch_heads
            .delete_branch_head(&"test".to_string())
            .unwrap();

        let branch_head_list = branch_heads.get_all_branch_heads().unwrap();

        assert_eq!(branch_head_list.len(), 1);
        assert_eq!(branch_head_list[0].branch_name, "main");
        assert_eq!(branch_head_list[0].pagenum, 1);
        assert_eq!(branch_head_list[0].rownum, 1);

        // Delete the test file
        std::fs::remove_file(format!(
            "{}{}",
            BRANCH_HEADS_FILE_NAME, BRANCH_HEADS_FILE_EXTENSION
        ))
        .unwrap();
    }
}
