use crate::fileio::{databaseio::{*, self}, header::*, pageio::*, rowio::*, tableio::*};
use crate::util::{dbtype::*, row::*};
use super::{diff::*, branch_heads::*};

#[derive(Clone)]
pub struct BranchNode {
    pub branch_name: String, // The name of the branch this node is on.
    pub commit_hash: String, // The commit hash that this node is associated with.
    pub prev_pagenum: i32,   // The page number of the previous branch node. Will be -1 if this is the first node.
    pub prev_rownum: i32,    // The row number of the previous branch node. Will be -1 if this is the first node.
    pub curr_pagenum: i32,   // The page number of the current branch node.
    pub curr_rownum: i32,    // The row number of the current branch node.
    pub is_head: bool        // Whether or not this node is the head of the branch.
}

/// This is designed to represent the branches.gql file for a database.
#[derive(Clone)]
pub struct Branches {
    pub filepath: String, // This is only public to allow file cleanup duing testing
    branches_table: Table
}

impl BranchNode {
    /// Create a new BranchNode from a row of data read from the branches.gql table
    pub fn new(row: &Row) -> Result<Self, String> {
        let branch_name: String = match row.get(0) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid branch name".to_string())
        };
        let commit_hash: String = match row.get(1) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid commit hash".to_string())
        };
        let prev_pagenum: i32 = match row.get(2) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string())
        };
        let prev_rownum: i32 = match row.get(3) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string())
        };
        let curr_pagenum: i32 = match row.get(4) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string())
        };
        let curr_rownum: i32 = match row.get(5) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string())
        };
        let is_head: bool = match row.get(6) {
            Some(Value::Bool(i)) => *i,
            _ => return Err("Invalid is_head value".to_string())
        };

        Ok(Self {
            branch_name,
            commit_hash,
            prev_pagenum,
            prev_rownum,
            curr_pagenum,
            curr_rownum,
            is_head
        })
    }
}


impl Branches {
    /// Creates a new BranchesFile object.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(
        dir_path: &String, 
        create_file: bool
    ) -> Result<Branches, String> {
        // Get filepath info
        let branch_filename: String = format!("{}{}", databaseio::BRANCHES_FILE_NAME.to_string(), databaseio::BRANCHES_FILE_EXTENSION.to_string());
        let mut filepath: String = format!("{}{}{}", dir_path, std::path::MAIN_SEPARATOR, branch_filename);
        // If the directory path is not given, use the current directory
        if dir_path.len() == 0 {
            filepath = branch_filename;
        }

        if create_file {
            std::fs::File::create(filepath.clone()).map_err(|e| e.to_string())?;

            let schema = vec![
                ("branch_name".to_string(), Column::String(60)),
                ("commit_hash".to_string(), Column::String(32)),
                ("prev_page_num".to_string(), Column::I32),
                ("prev_row_num".to_string(), Column::I32),
                ("curr_page_num".to_string(), Column::I32),
                ("curr_row_num".to_string(), Column::I32),
                ("is_head".to_string(), Column::Bool)
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

        Ok(Branches {
            filepath: filepath.clone(),
            branches_table: Table::new(&dir_path.clone(), 
                &databaseio::BRANCHES_FILE_NAME.to_string(),
                Some(&databaseio::BRANCHES_FILE_EXTENSION.to_string()))?
        })
    }


    /// Returns a branch node from the given page row and number.
    pub fn get_branch_node(&self, row_location: &RowLocation) -> Result<BranchNode, String> {
        let row = get_row(&self.branches_table, &row_location)?;
        Ok(BranchNode::new(&row)?)
    }


    /// Returns the previous branch node from the given branch node
    pub fn get_prev_branch_node(&self, branch_node: &BranchNode) -> Result<BranchNode, String> {
        let row_location = RowLocation {
            pagenum: branch_node.prev_pagenum as u32,
            rownum: branch_node.prev_rownum as u16
        };
        self.get_branch_node(&row_location)
    }


    /// Traverse backwards through the nodes starting from the given branch node.
    /// Returns a list of all nodes in the branch in reverse order. 
    /// The first node returned in the list is the origin and the last is the branch node given.
    pub fn traverse_branch_nodes(&self, branch_node: &BranchNode) -> Result<Vec<BranchNode>, String> {
        let mut branch_nodes: Vec<BranchNode> = Vec::new();
        let mut current_branch_node: BranchNode = branch_node.clone();
        loop {
            branch_nodes.push(current_branch_node.clone());
            if current_branch_node.prev_pagenum == -1 {
                break;
            }
            current_branch_node = self.get_prev_branch_node(&current_branch_node)?;
        }
        branch_nodes.reverse();
        Ok(branch_nodes)
    }


    /// Creates a new branch node and adds it to the branches table with the given branch name and commit hash.
    /// Also updates the branch HEADs table appropriately.
    pub fn create_branch_node(
        &mut self, 
        branch_heads_table: &mut BranchHEADs, 
        prev_node: Option<&BranchNode>, 
        branch_name: &String, 
        commit_hash: &String
    ) -> Result<(), String> {
        // Determine if we are creating the first commit on the database, or if this commit is on an existing branch
        let mut prev_pagenum: i32 = -1;                    // Default value for first commit
        let mut prev_rownum: i32 = -1;                     // Default value for first commit
        let mut prev_is_head: bool = true;                 // Default value for first commit
        let mut prev_branch_name: String = "".to_string(); // Default value for first commit
        match prev_node {
            Some(prev_node_values) => {
                prev_pagenum = prev_node_values.curr_pagenum;
                prev_rownum = prev_node_values.curr_rownum;
                prev_is_head = prev_node_values.is_head;
                prev_branch_name = prev_node_values.branch_name.clone();
            },
            None => { /* Do nothing */ }
        }
        
        // Create the new branch node
        let mut new_node: Vec<Value> = Vec::new();
        new_node.push(Value::String(branch_name.clone()));
        new_node.push(Value::String(commit_hash.clone()));
        new_node.push(Value::I32(prev_pagenum));
        new_node.push(Value::I32(prev_rownum));
        new_node.push(Value::I32(-1)); // Default value until after we write the row to the table
        new_node.push(Value::I32(-1)); // Default value until after we write the row to the table
        new_node.push(Value::Bool(false));
        
        // Insert the new branch node
        let insert_diff: InsertDiff = insert_rows(&mut self.branches_table, vec![new_node])?;

        // Verify that the insert was successful
        match insert_diff.rows.get(0) {
            Some(row) => {
                // This determines if we are going to update the is_head value of this branch node
                let mut set_is_head: bool = false;
                let mut rows_to_rewrite: Vec<RowInfo> = Vec::new();

                // If the previous node was the head, set the new node to be the head only if the branch names match
                if prev_is_head && prev_branch_name == branch_name.clone() {
                    // Set the new node to be the head using the location where the new node was inserted
                    branch_heads_table.set_branch_head(
                        branch_name, 
                        &RowLocation { 
                            pagenum: row.pagenum, 
                            rownum: row.rownum 
                        }
                    )?;
                    set_is_head = true;

                    // We need to update the previous node to no longer be the head in the table
                    let prev_row_location: RowLocation = RowLocation {
                        pagenum: prev_pagenum as u32,
                        rownum: prev_rownum as u16
                    };
                    let mut prev_row: Row = get_row(&self.branches_table, &prev_row_location)?;
                    prev_row[6] = Value::Bool(false);
                    let prev_row_info: RowInfo = RowInfo {
                        pagenum: prev_row_location.pagenum as u32,
                        rownum: prev_row_location.rownum as u16,
                        row: prev_row
                    };
                    rows_to_rewrite.push(prev_row_info);
                }
                // If the previous node was a different branch name, this node needs to be a new HEAD node
                else if prev_branch_name != branch_name.clone() {
                    // Create a new branch HEAD node
                    branch_heads_table.create_branch_head(
                        &BranchHead { 
                            branch_name: branch_name.clone(), 
                            pagenum: row.pagenum as i32, 
                            rownum: row.rownum as i32 
                        }
                    )?;
                    set_is_head = true;
                }

                // Write the updated values to the table
                let mut new_row: RowInfo = row.clone();
                new_row.row[4] = Value::I32(row.pagenum as i32); // curr_pagenum column
                new_row.row[5] = Value::I32(row.rownum as i32);  // curr_rownum column
                new_row.row[6] = Value::Bool(set_is_head);       // is_head column
                rows_to_rewrite.push(new_row);
                rewrite_rows(&mut self.branches_table, rows_to_rewrite)?;

                Ok(())
            },
            None => return Err("Branch node was not created correctly".to_string())
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_creating_branch_node() {
        let mut branches_file: Branches = Branches::new(&"".to_string(), true).unwrap();
        let mut branch_heads_table: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();
        let commit_hash: String = "12345678901234567890123456789012".to_string();
        let branch_name: String = "test_branch".to_string();
        branches_file.create_branch_node(&mut branch_heads_table, None, &branch_name, &commit_hash).unwrap();
        let branch_head: BranchHead = branch_heads_table.get_branch_head(&branch_name).unwrap();
        let branch_node: BranchNode = branches_file.get_branch_node(&branch_head.get_branch_node_location()).unwrap();
        assert_eq!(branch_node.branch_name, branch_name);
        assert_eq!(branch_node.commit_hash, commit_hash);
        assert_eq!(branch_node.prev_pagenum, -1);
        assert_eq!(branch_node.prev_rownum, -1);
        assert_eq!(branch_node.is_head, true);

        // Delete the test files
        std::fs::remove_file(branches_file.filepath).unwrap();
        std::fs::remove_file(branch_heads_table.filepath).unwrap();
    }

    #[test]
    #[serial]
    fn test_creating_multiple_branch_nodes() {
        let mut branches_file: Branches = Branches::new(&"".to_string(), true).unwrap();
        let mut branch_heads_table: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();
        let commit_hash1: String = "12345678901234567890123456789012".to_string();
        let branch_name: String = "test_branch".to_string();
        branches_file.create_branch_node(&mut branch_heads_table, None, &branch_name, &commit_hash1).unwrap();
        let branch_head: BranchHead = branch_heads_table.get_branch_head(&branch_name).unwrap();
        let branch_node: BranchNode = branches_file.get_branch_node(&branch_head.get_branch_node_location()).unwrap();
        assert_eq!(branch_node.branch_name, branch_name);
        assert_eq!(branch_node.commit_hash, commit_hash1);
        assert_eq!(branch_node.prev_pagenum, -1);
        assert_eq!(branch_node.prev_rownum, -1);
        assert_eq!(branch_node.is_head, true);

        // Create a second branch node
        let commit_hash2: String = "23456789012345678901234567890123".to_string();
        let branch_name2: String = "test_branch".to_string();
        branches_file.create_branch_node(&mut branch_heads_table, Some(&branch_node), &branch_name2, &commit_hash2).unwrap();
        let branch_head2: BranchHead = branch_heads_table.get_branch_head(&branch_name2).unwrap();
        let branch_node2: BranchNode = branches_file.get_branch_node(&branch_head2.get_branch_node_location()).unwrap();
        assert_eq!(branch_node2.branch_name, branch_name2);
        assert_eq!(branch_node2.commit_hash, commit_hash2);
        assert_eq!(branch_node2.prev_pagenum, 1);
        assert_eq!(branch_node2.prev_rownum, 0);
        assert_eq!(branch_node2.is_head, true);

        // Verify that you can access first branch node from the second
        let branch_node3: BranchNode = branches_file.get_prev_branch_node(&branch_node2).unwrap();
        assert_eq!(branch_node3.branch_name, branch_name2);
        assert_eq!(branch_node3.commit_hash, commit_hash1);
        assert_eq!(branch_node3.prev_pagenum, -1);
        assert_eq!(branch_node3.prev_rownum, -1);
        assert_eq!(branch_node3.is_head, false);

        // Delete the test files
        std::fs::remove_file(branches_file.filepath).unwrap();
        std::fs::remove_file(branch_heads_table.filepath).unwrap();
    }
}