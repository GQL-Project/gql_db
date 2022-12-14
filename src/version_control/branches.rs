use std::collections::HashMap;

use super::{branch_heads::*, diff::*};
use crate::fileio::{
    databaseio::{self, get_db_instance},
    header::*,
    pageio::*,
    tableio::*,
};
use crate::util::{dbtype::*, row::*};

/// This represents a branch node in the database. It is a single row in the `branches.gql` table.
/// A branch node is in a linked list of other branch nodes. It is singly linked, pointing backwards.
#[derive(Clone, PartialEq, Debug)]
pub struct BranchNode {
    pub branch_name: String, // The name of the branch this node is on.
    pub commit_hash: String, // The commit hash that this node is associated with.
    pub prev_pagenum: i32, // The page number of the previous branch node. Will be -1 if this is the first node.
    pub prev_rownum: i32, // The row number of the previous branch node. Will be -1 if this is the first node.
    pub curr_pagenum: i32, // The page number of the current branch node.
    pub curr_rownum: i32, // The row number of the current branch node.
    pub num_kids: i32, // The number of children this branch node has. (0 if it is the last node in the list)
    pub is_head: bool, // Whether or not this branch node is the head of the branch.
}

impl BranchNode {
    pub fn can_squash(&self) -> bool {
        // If this node is the head of the branch, we can't squash it, if it has any children.
        // Alternatively, if it has more than one child, we can't squash it.
        (!self.is_head || self.num_kids == 0) && self.num_kids <= 1
    }
}

/// This is designed to represent the branches.gql file for a database.
/// It is a table that contains a list of all branch nodes across all branches in the database.
#[derive(Clone)]
pub struct Branches {
    filepath: String,
    branches_table: Table,
}

impl BranchNode {
    /// Create a new BranchNode from a row of data read from the branches.gql table
    pub fn new(row: &Row) -> Result<Self, String> {
        let branch_name: String = match row.get(0) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid branch name".to_string()),
        };
        let commit_hash: String = match row.get(1) {
            Some(Value::String(s)) => s.clone(),
            _ => return Err("Invalid commit hash".to_string()),
        };
        let prev_pagenum: i32 = match row.get(2) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string()),
        };
        let prev_rownum: i32 = match row.get(3) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string()),
        };
        let curr_pagenum: i32 = match row.get(4) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid page number".to_string()),
        };
        let curr_rownum: i32 = match row.get(5) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid row number".to_string()),
        };
        let num_kids = match row.get(6) {
            Some(Value::I32(i)) => *i,
            _ => return Err("Invalid num_kids value".to_string()),
        };
        let is_head = match row.get(7) {
            Some(Value::Bool(i)) => *i,
            _ => return Err("Invalid is_head value".to_string()),
        };

        Ok(Self {
            branch_name,
            commit_hash,
            prev_pagenum,
            prev_rownum,
            curr_pagenum,
            curr_rownum,
            num_kids,
            is_head,
        })
    }
}

impl Branches {
    /// Creates a new Branches object.
    /// This object is used to store all the branch nodes across all branches in the database.
    /// If create_file is true, the file and table will be created with a header.
    /// If create_file is false, the file and table will be opened.
    pub fn new(dir_path: &String, create_file: bool) -> Result<Branches, String> {
        // Get filepath info
        let branch_filename: String = format!(
            "{}{}",
            databaseio::BRANCHES_FILE_NAME.to_string(),
            databaseio::BRANCHES_FILE_EXTENSION.to_string()
        );
        let mut filepath: String = format!(
            "{}{}{}",
            dir_path,
            std::path::MAIN_SEPARATOR,
            branch_filename
        );
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
                ("num_kids".to_string(), Column::I32),
                ("is_head".to_string(), Column::Bool),
            ];
            let header = Header {
                num_pages: 2,
                schema,
                index_top_level_pages: HashMap::new(),
            };
            write_header(&filepath, &header)?;

            // Write a blank page to the table
            let page = [0u8; PAGE_SIZE];
            write_page(1, &filepath, &page, PageType::Data)?;
        }

        Ok(Branches {
            filepath: filepath.clone(),
            branches_table: Table::new(
                &dir_path.clone(),
                &databaseio::BRANCHES_FILE_NAME.to_string(),
                Some(&databaseio::BRANCHES_FILE_EXTENSION.to_string()),
            )?,
        })
    }

    // Immutable getter access to filepath.
    pub fn filepath(&self) -> &str {
        &self.filepath
    }

    /// Returns a branch node from `branches.gql` with the given page row and number.
    pub fn get_branch_node(&self, row_location: &RowLocation) -> Result<BranchNode, String> {
        let row = &self.branches_table.get_row(&row_location)?;
        Ok(BranchNode::new(&row)?)
    }

    /// Returns the previous branch node from the given branch node
    /// Returns None if the given branch node is the original node in the database
    pub fn get_prev_branch_node(
        &self,
        branch_node: &BranchNode,
    ) -> Result<Option<BranchNode>, String> {
        if branch_node.prev_pagenum == -1 || branch_node.prev_rownum == -1 {
            return Ok(None);
        }
        let row_location = RowLocation {
            pagenum: branch_node.prev_pagenum as u32,
            rownum: branch_node.prev_rownum as u16,
        };
        Ok(Some(self.get_branch_node(&row_location)?))
    }

    /// Traverse backwards through the nodes starting from the given branch node.
    /// Returns a list of all nodes in the branch in reverse order.
    /// The first node returned in the list is the origin and the last is the branch node given.
    pub fn traverse_branch_nodes(
        &self,
        branch_node: &BranchNode,
    ) -> Result<Vec<BranchNode>, String> {
        let mut branch_nodes: Vec<BranchNode> = Vec::new();
        let mut current_branch_node: BranchNode = branch_node.clone();
        loop {
            branch_nodes.push(current_branch_node.clone());
            current_branch_node = match self.get_prev_branch_node(&current_branch_node)? {
                Some(bn) => bn, // If Some, we have a previous node
                None => break,  // If None, that means we are trying to go before the original node
            };
        }
        Ok(branch_nodes)
    }

    /// Find the branch node with the given commit hash.
    /// Returns None if the commit hash is not found.
    /// Returns the branch node if the commit hash is found.
    pub fn traverse_for_commit(
        &self,
        branch_start: &BranchNode,
        commit_hash: &String,
    ) -> Result<Option<BranchNode>, String> {
        let mut current_node: BranchNode = branch_start.clone();
        let commit_hash = get_db_instance()?
            .get_commit_file_mut()
            .resolve_commit(commit_hash)?; // Resolve the commit hash to the actual commit hash (in case it is a branch name
        loop {
            if current_node.commit_hash == *commit_hash {
                return Ok(Some(current_node));
            }
            current_node = match self.get_prev_branch_node(&current_node)? {
                Some(bn) => bn, // If Some, we have a previous node
                None => break,  // If None, that means we are trying to go before the original node
            };
        }
        Ok(None)
    }

    /// Creates a new branch node and adds it to the branches table with the given branch name and commit hash.
    /// Also updates the branch HEADs table appropriately.
    /// It branches the node off the prev_node given. If prev_node is None, it becomes the original node.
    pub fn create_branch_node(
        &mut self,
        branch_heads: &mut BranchHEADs,
        prev_node: Option<&BranchNode>,
        branch_name: &String,
        commit_hash: &String,
    ) -> Result<BranchNode, String> {
        // Create the new branch node
        let mut new_node: Vec<Value> = Vec::new();
        new_node.push(Value::String(branch_name.clone()));
        new_node.push(Value::String(commit_hash.clone()));
        new_node.push(Value::I32(prev_node.map_or(-1, |n| n.curr_pagenum)));
        new_node.push(Value::I32(prev_node.map_or(-1, |n| n.curr_rownum)));
        new_node.push(Value::I32(-1)); // Default value until after we write the row to the table
        new_node.push(Value::I32(-1)); // Default value until after we write the row to the table
        new_node.push(Value::I32(0));
        new_node.push(Value::Bool(true));

        // Insert the new branch node
        let insert_diff: InsertDiff = self.branches_table.insert_rows(vec![new_node])?;

        // Verify that the insert was successful
        match insert_diff.rows.get(0) {
            Some(row) => {
                // This determines if we are going to update the is_head value of this branch node
                let mut rows_to_rewrite: Vec<RowInfo> = Vec::new();

                if let Some(prev_node) = prev_node {
                    let prev_row_location: RowLocation = RowLocation {
                        pagenum: prev_node.curr_pagenum as u32,
                        rownum: prev_node.curr_rownum as u16,
                    };
                    let mut prev_row: Row = self.branches_table.get_row(&prev_row_location)?;
                    prev_row[6] = Value::I32(prev_node.num_kids + 1);
                    if prev_node.branch_name == branch_name.clone() {
                        if !prev_node.is_head {
                            return Err(
                                "Cannot create a branch node with the same branch name as the previous node if the previous node is not the head of the branch.".to_string());
                        }
                        branch_heads.set_branch_head(
                            branch_name,
                            &RowLocation {
                                pagenum: row.pagenum,
                                rownum: row.rownum,
                            },
                        )?;
                        prev_row[7] = Value::Bool(false);
                    } else {
                        branch_heads.create_branch_head(&BranchHead {
                            branch_name: branch_name.clone(),
                            pagenum: row.pagenum as i32,
                            rownum: row.rownum as i32,
                        })?;
                        prev_row[7] = Value::Bool(true);
                    }
                    let prev_row_info: RowInfo = RowInfo {
                        pagenum: prev_row_location.pagenum as u32,
                        rownum: prev_row_location.rownum as u16,
                        row: prev_row,
                    };
                    rows_to_rewrite.push(prev_row_info);
                } else {
                    branch_heads.create_branch_head(&BranchHead {
                        branch_name: branch_name.clone(),
                        pagenum: row.pagenum as i32,
                        rownum: row.rownum as i32,
                    })?;
                }

                // Write the updated values to the table
                let mut new_row: RowInfo = row.clone();
                new_row.row[4] = Value::I32(row.pagenum as i32); // curr_pagenum column
                new_row.row[5] = Value::I32(row.rownum as i32); // curr_rownum column
                let node: BranchNode = BranchNode::new(&new_row.row)?;
                rows_to_rewrite.push(new_row);
                self.branches_table.rewrite_rows(rows_to_rewrite)?;
                Ok(node)
            }
            None => return Err("Branch node was not created correctly".to_string()),
        }
    }

    /// Deletes the branch node with the given branch name
    pub fn delete_branch_node(&mut self, branch_name: &String) -> Result<(), String> {
        // Get the branch node
        // let branch_node: BranchNode = self.get_branch_node(branch_name)?;
        for row_info in self.branches_table.by_ref().into_iter().clone() {
            let row: Row = row_info.clone().row;

            let row_node_name: String;

            // Get the branch name
            match row.get(0) {
                Some(Value::String(br_name)) => row_node_name = br_name.to_string(),
                _ => return Err("Error: Branch name not found".to_string()),
            }

            if row_node_name == *branch_name {
                self.branches_table.remove_rows(vec![RowLocation {
                    pagenum: row_info.pagenum,
                    rownum: row_info.rownum,
                }])?;
            }
        }
        Ok(())
    }

    /// Update an existing branch node, using the curr_pagenum and curr_rownum values to find the node.
    pub fn update_branch_node(&mut self, node: &BranchNode) -> Result<BranchNode, String> {
        // Create the new branch node
        let mut new_node: Vec<Value> = Vec::new();
        new_node.push(Value::String(node.branch_name.clone()));
        new_node.push(Value::String(node.commit_hash.clone()));
        new_node.push(Value::I32(node.prev_pagenum));
        new_node.push(Value::I32(node.prev_rownum));
        new_node.push(Value::I32(node.curr_pagenum));
        new_node.push(Value::I32(node.curr_rownum));
        new_node.push(Value::I32(node.num_kids));

        // Insert the new branch node
        let update_diff: UpdateDiff = self.branches_table.rewrite_rows(vec![RowInfo {
            row: new_node,
            pagenum: node.curr_pagenum as u32,
            rownum: node.curr_rownum as u16,
        }])?;

        // Verify that the update was successful
        match update_diff.rows.get(0) {
            Some(_) => Ok(node.clone()),
            None => return Err("Branch node was not updated correctly".to_string()),
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
        branches_file
            .create_branch_node(&mut branch_heads_table, None, &branch_name, &commit_hash)
            .unwrap();
        let branch_head: BranchHead = branch_heads_table.get_branch_head(&branch_name).unwrap();
        let branch_node: BranchNode = branches_file
            .get_branch_node(&branch_head.get_branch_node_location())
            .unwrap();
        assert_eq!(branch_node.branch_name, branch_name);
        assert_eq!(branch_node.commit_hash, commit_hash);
        assert_eq!(branch_node.prev_pagenum, -1);
        assert_eq!(branch_node.prev_rownum, -1);
        assert_eq!(branch_node.is_head, true);

        // Delete the test files
        std::fs::remove_file(branches_file.filepath()).unwrap();
        std::fs::remove_file(branch_heads_table.filepath()).unwrap();
    }

    #[test]
    #[serial]
    fn test_creating_multiple_branch_nodes() {
        let mut branches_file: Branches = Branches::new(&"".to_string(), true).unwrap();
        let mut branch_heads_table: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();
        let commit_hash1: String = "12345678901234567890123456789012".to_string();
        let branch_name: String = "test_branch".to_string();
        branches_file
            .create_branch_node(&mut branch_heads_table, None, &branch_name, &commit_hash1)
            .unwrap();
        let branch_head: BranchHead = branch_heads_table.get_branch_head(&branch_name).unwrap();
        let branch_node: BranchNode = branches_file
            .get_branch_node(&branch_head.get_branch_node_location())
            .unwrap();
        assert_eq!(branch_node.branch_name, branch_name);
        assert_eq!(branch_node.commit_hash, commit_hash1);
        assert_eq!(branch_node.prev_pagenum, -1);
        assert_eq!(branch_node.prev_rownum, -1);
        assert_eq!(branch_node.is_head, true);

        // Create a second branch node
        let commit_hash2: String = "23456789012345678901234567890123".to_string();
        let branch_name2: String = "test_branch".to_string();
        branches_file
            .create_branch_node(
                &mut branch_heads_table,
                Some(&branch_node),
                &branch_name2,
                &commit_hash2,
            )
            .unwrap();
        let branch_head2: BranchHead = branch_heads_table.get_branch_head(&branch_name2).unwrap();
        let branch_node2: BranchNode = branches_file
            .get_branch_node(&branch_head2.get_branch_node_location())
            .unwrap();
        assert_eq!(branch_node2.branch_name, branch_name2);
        assert_eq!(branch_node2.commit_hash, commit_hash2);
        assert_eq!(branch_node2.prev_pagenum, 1);
        assert_eq!(branch_node2.prev_rownum, 0);
        assert_eq!(branch_node2.is_head, true);

        // Verify that you can access first branch node from the second
        let branch_node3: BranchNode = branches_file
            .get_prev_branch_node(&branch_node2)
            .unwrap()
            .unwrap();
        assert_eq!(branch_node3.branch_name, branch_name2);
        assert_eq!(branch_node3.commit_hash, commit_hash1);
        assert_eq!(branch_node3.prev_pagenum, -1);
        assert_eq!(branch_node3.prev_rownum, -1);
        assert_eq!(branch_node3.is_head, false);

        // Delete the test files
        std::fs::remove_file(branches_file.filepath()).unwrap();
        std::fs::remove_file(branch_heads_table.filepath()).unwrap();
    }

    #[test]
    #[serial]
    fn test_get_branch_node_from_head() {
        let mut branches_file: Branches = Branches::new(&"".to_string(), true).unwrap();
        let mut branch_heads_table: BranchHEADs = BranchHEADs::new(&"".to_string(), true).unwrap();
        let commit_hash1: String = "12345678901234567890123456789012".to_string();
        let branch_name: String = "test_branch".to_string();
        branches_file
            .create_branch_node(&mut branch_heads_table, None, &branch_name, &commit_hash1)
            .unwrap();
        let branch_node: BranchNode = branch_heads_table
            .get_branch_node_from_head(&branch_name, &branches_file)
            .unwrap();
        assert_eq!(branch_node.branch_name, branch_name);
        assert_eq!(branch_node.commit_hash, commit_hash1);
        assert_eq!(branch_node.prev_pagenum, -1);
        assert_eq!(branch_node.prev_rownum, -1);
        assert_eq!(branch_node.is_head, true);

        // Delete the test files
        std::fs::remove_file(branches_file.filepath()).unwrap();
        std::fs::remove_file(branch_heads_table.filepath()).unwrap();
    }
}
