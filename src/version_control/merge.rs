use crate::{user::userdata::User, fileio::databaseio::get_db_instance, util::row::EmptyRowLocation};

use super::diff::*;
use std::{collections::HashMap, vec};

pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
}

/// Merges a single diff to merge into the list of diffs to merge into using a merge conflict algorithm
/// Returns a new list of diffs that would be the result of applying diff_to_merge into target_diffs 
pub fn merge_diff_into_list(
    diff_to_merge: Diff,                              // The source diff to merge into the target diffs
    target_diffs: &Vec<Diff>,                         // The target diffs to merge the source diff into                    
    insert_map: &mut HashMap<(u32, u16), (u32, u16)>, // Maps (pagenum, rownum) in source to (pagenum, rownum) in the target
    user: &User,                                      // The user that is performing the merge (assumed to be on the target branch)
    conflict_res_algo: MergeConflictResolutionAlgo    // The merge conflict resolution algorithm to use
) -> Result<Vec<Diff>, String> {
    // We assume target_diffs_on_the_table only contains one diff of each type for that table
    verify_only_one_type_of_diff_per_table(target_diffs)?;

    // Get all the diffs that affect the same table as diff_to_merge
    let target_diffs_on_the_table: Vec<Diff> = target_diffs
        .iter()
        .filter(|diff| diff.get_table_name() == diff_to_merge.get_table_name())
        .cloned()
        .collect();

    let result_diffs: Vec<Diff> = Vec::new();

    match diff_to_merge {
        Diff::Insert(mut insert_diff_to_merge) => {
            // Get the insert diff from target_diffs_on_the_table if it exists
            let insert_diff_target_option = target_diffs_on_the_table
                .iter()
                .find_map(|diff| match diff {
                    Diff::Insert(ins_diff) => Some(ins_diff),
                    _ => None,
                });
            
            // If there is an insert diff in the target, we need to remove any duplicate row insertions.
            if let Some(insert_diff_target) = insert_diff_target_option {
                insert_diff_to_merge.rows
                    .retain(|x| {
                        !insert_diff_target
                            .rows
                            .iter()
                            .any(|y| 
                                x.pagenum == y.pagenum && 
                                x.rownum == y.rownum &&
                                x.row == y.row)
                    }
                );
            }

            // Now we need to map the rows in insert_diff_to_merge to open rows in the target
            // Find the open rows in the target
            let open_rows: Vec<EmptyRowLocation> = get_db_instance()?
                .get_open_rows_in_table(
                    &insert_diff_to_merge.table_name, 
                    insert_diff_to_merge.rows.len(),
                    user
                )?;

            // Map the rows in insert_diff_to_merge to the open rows
            for (i, row) in insert_diff_to_merge.rows.iter().enumerate() {
                insert_map.insert((row.pagenum, row.rownum), (open_rows[i].location.pagenum, open_rows[i].location.rownum));

                // Add the new mapped rows to the result_diffs

            }
        },
        Diff::Update(update_diff_to_merge) => {},
        Diff::Remove(remove_diff_to_merge) => {},
        Diff::TableCreate(table_create_diff_to_merge) => {},
        Diff::TableRemove(table_remove_diff_to_merge) => {},
    }



    Ok(vec![])
}

/// Creates a list of merge diffs that would result from merging the source diffs onto
/// the target diffs. 
/// Depending on the conflict resolution algorithm, the merge may fail if there are conflicts.
/// Otherwise, the merge will succeed, and the result will contain the diffs needed to apply
/// the source diffs to the target diffs.
pub fn create_merge_diffs(
    source_diffs: &Vec<Diff>, 
    target_diffs: &Vec<Diff>, 
    conflict_res_algo: MergeConflictResolutionAlgo
) -> Result<Vec<Diff>, String> {
    // A hashmap that maps a table name to a list of diffs for that table
    let mut result: HashMap<String, Vec<Diff>> = HashMap::new();

    match conflict_res_algo {
        MergeConflictResolutionAlgo::NoConflicts => {
            // Merge each of the source diffs into the target diffs
            for src_diff in source_diffs {
                
            }
        },
        MergeConflictResolutionAlgo::UseTarget => {
            // Check if there any conflicts between the two diffs
            for src_diff in source_diffs {
                for target_diff in target_diffs {
                    if src_diff.is_merge_conflict(target_diff) {
                        // There is a conflict, so we need to decompose both diffs down to the row level
                        // and then merge the rows, using the target's version of any conflicting rows.

                        // We know since they conflicted, they are for the same table, so we don't need to check that

                    }
                }
            }
        },
        MergeConflictResolutionAlgo::UseSource => {

        },
    }

    Ok(vec![])
}


/// Verifies that for each table within the diffs, there is at most 1 of each type of diff
fn verify_only_one_type_of_diff_per_table(diffs: &Vec<Diff>) -> Result<(), String> {
    // Get all the diffs organized into a hashmap of table name to a list of diffs for that table
    let table_to_diffs: HashMap<String, Vec<Diff>> = 
        diffs.iter().fold(HashMap::new(), |mut acc, diff| {
            let table_name = diff.get_table_name();
            let table_diffs = acc.entry(table_name).or_insert(Vec::new());
            table_diffs.push(diff.clone());
            acc
        });

    for table_name in table_to_diffs.keys() {
        if let Some(diffs_of_same_table) = table_to_diffs.get(table_name) {

            let mut contains_create_table: bool = false;
            let mut contains_remove_table: bool = false;
            let mut contains_insert: bool = false;
            let mut contains_remove: bool = false;
            let mut contains_update: bool = false;
            for diff in diffs_of_same_table {
                match diff {
                    Diff::TableCreate(_) => {
                        if contains_create_table {
                            return Err(format!("Multiple create table diffs for table {}", diff.get_table_name()));
                        }
                        contains_create_table = true;
                    },
                    Diff::TableRemove(_) => {
                        if contains_remove_table {
                            return Err(format!("Multiple remove table diffs for table {}", diff.get_table_name()));
                        }
                        contains_remove_table = true;
                    },
                    Diff::Insert(_) => {
                        if contains_insert {
                            return Err(format!("Multiple insert diffs for table {}", diff.get_table_name()));
                        }
                        contains_insert = true;
                    },
                    Diff::Remove(_) => {
                        if contains_remove {
                            return Err(format!("Multiple remove diffs for table {}", diff.get_table_name()));
                        }
                        contains_remove = true;
                    },
                    Diff::Update(_) => {
                        if contains_update {
                            return Err(format!("Multiple update diffs for table {}", diff.get_table_name()));
                        }
                        contains_update = true;
                    },
                }
            }
        }
    }

    Ok(())
}