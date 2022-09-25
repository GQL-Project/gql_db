use crate::util::{dbtype::*, row::*};
use crate::fileio::{databaseio::*, tableio::*, header::*, rowio::*, pageio::*};

#[derive(Clone)]
pub struct BranchHead {
    pub branch_name: String,
    pub page_num: i32,
    pub row_num: i32,
}

pub fn write_branch_heads_file_header(branch_heads_file_path: &String) -> Result<Table, String> {
    let schema = vec![
        ("branch_name".to_string(), Column::String(60)),
        ("page_num".to_string(), Column::I32),
        ("row_num".to_string(), Column::I32),
    ];
    let header = Header {
        num_pages: 2,
        schema,
    };
    write_header(&branch_heads_file_path, &header)?;

    // Write a blank page to the table
    let page = [0u8; PAGE_SIZE];
    write_page(1, &branch_heads_file_path, &page)?;

    // Return the table
    Ok(Table::new(branch_heads_file_path.to_string())?)
}


/// Writes a new branch head to the branch heads file
pub fn write_new_branch_head(branch_head: &BranchHead, branch_heads_file: &mut Table) -> Result<(), String> {
    
    let rows: Vec<Vec<Value>> = vec![
        // Just make one new row
        vec![
            Value::String(branch_head.branch_name.clone()),
            Value::I32(branch_head.page_num),
            Value::I32(branch_head.row_num)
        ],
    ];
    insert_rows(branch_heads_file, rows)?;
    Ok(())
}


/// Read the branch heads file and return a vector of BranchHead structs
pub fn get_all_branch_heads(branch_heads_file: &mut Table) -> Result<Vec<BranchHead>, String> {
    let mut branch_heads: Vec<BranchHead> = Vec::new();

    for row_info in branch_heads_file {
        let row: Row = row_info.row;

        let branch_name: String;
        let page_num: i32;
        let row_num: i32;

        match row.get(0) {
            Some(Value::String(br_name)) => branch_name = br_name.to_string(),
            _ => return Err("Error: Branch name not found".to_string())
        }

        match row.get(1) {
            Some(Value::I32(p_num)) => page_num = p_num.clone(),
            _ => return Err("Error: Page number not found".to_string())
        }

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