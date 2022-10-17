use crate::{user::userdata::User, fileio::databaseio::get_db_instance, util::row::{EmptyRowLocation, RowInfo}};

use super::diff::*;
use std::{collections::HashMap, vec};

pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
}

/// Merges a single diff to merge into the list of diffs to merge into using a merge conflict algorithm
/// Returns a new list of diffs that would be the result of applying source_diffs into target_diffs 
pub fn merge_diff_into_list(
    source_diffs: &Vec<Diff>,                         // The source diffs to merge into the target diffs
    target_diffs: &Vec<Diff>,                         // The target diffs to merge the source diff into                    
    insert_map: &mut HashMap<(u32, u16), (u32, u16)>, // Maps (pagenum, rownum) in source to (pagenum, rownum) in the target
    user: &User,                                      // The user that is performing the merge (assumed to be on the target branch)
    conflict_res_algo: MergeConflictResolutionAlgo    // The merge conflict resolution algorithm to use
) -> Result<Vec<Diff>, String> {
    // We assume target_diffs_on_the_table only contains one diff of each type for that table
    verify_only_one_type_of_diff_per_table(target_diffs)?;

    let mut result_diffs: SquashDiffs = SquashDiffs::new();

    for source_diff in source_diffs {
        // Get all the diffs that affect the same table as the source_diff
        let target_diffs_on_the_table: Vec<Diff> = target_diffs
            .iter()
            .filter(|diff| diff.get_table_name() == source_diff.get_table_name())
            .cloned()
            .collect();

        match source_diff.clone() {
            Diff::Insert(mut insert_source_diff) => {
                // Get the insert diff from target_diffs_on_the_table if it exists
                let insert_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Insert(ins_diff) => Some(ins_diff),
                        _ => None,
                    });
                
                // If there is an insert diff in the target, we need to remove any duplicate row insertions.
                if let Some(insert_diff_target) = insert_diff_target_option {
                    insert_source_diff.rows
                        .retain(|x| {
                            !insert_diff_target
                                .rows
                                .iter()
                                .any(|y| 
                                    x.pagenum == y.pagenum && 
                                    x.rownum == y.rownum &&
                                    x.row == y.row
                                )
                        }
                    );
                }

                // Now we need to map the rows in insert_source_diff to open rows in the target
                // Find the open rows in the target
                let open_rows: Vec<EmptyRowLocation> = get_db_instance()?
                    .get_open_rows_in_table(
                        &insert_source_diff.table_name, 
                        insert_source_diff.rows.len(),
                        user
                    )?;

                // Map the rows in insert_source_diff to the open rows
                for (i, row) in insert_source_diff.rows.iter().enumerate() {
                    insert_map.insert((row.pagenum, row.rownum), (open_rows[i].location.pagenum, open_rows[i].location.rownum));

                    // Add the new mapped rows to the result_diffs
                    result_diffs.table_diffs
                        .entry(insert_source_diff.table_name.clone())
                        .or_insert_with(|| TableSquashDiff::new(&insert_source_diff.table_name, &insert_source_diff.schema))
                        .insert_diff.rows.push(RowInfo {
                            pagenum: open_rows[i].location.pagenum,
                            rownum: open_rows[i].location.rownum,
                            row: row.row.clone(),
                        });
                }
            },
            Diff::Update(mut update_source_diff) => {
                // Get the update diff from target_diffs_on_the_table if it exists
                let update_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Update(up_diff) => Some(up_diff),
                        _ => None,
                    });
                
                // If there is an update diff in the target, we need to remove any duplicate row updates.
                if let Some(update_diff_target) = update_diff_target_option {
                    update_source_diff.rows
                        .retain(|x| {
                            !update_diff_target
                                .rows
                                .iter()
                                .any(|y| 
                                    x.pagenum == y.pagenum && 
                                    x.rownum == y.rownum &&
                                    x.row == y.row
                                )
                        }
                    );
                }

                // Now we need to map the rows in update_source_diff to the rows in the target
                for row in update_source_diff.rows.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) = insert_map.get(&(row.pagenum, row.rownum)) {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }

                // Add the new mapped rows to the result_diffs
                result_diffs.table_diffs
                    .entry(update_source_diff.table_name.clone())
                    .or_insert_with(|| TableSquashDiff::new(&update_source_diff.table_name, &update_source_diff.schema))
                    .update_diff.rows.append(&mut update_source_diff.rows);
            },
            Diff::Remove(mut remove_source_diff) => {
                // Get the remove diff from target_diffs_on_the_table if it exists
                let remove_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Remove(rem_diff) => Some(rem_diff),
                        _ => None,
                    });
                
                // If there is a remove diff in the target, we need to remove any duplicate row removals.
                if let Some(remove_diff_target) = remove_diff_target_option {
                    remove_source_diff.rows_removed
                        .retain(|x| {
                            !remove_diff_target
                                .rows_removed
                                .iter()
                                .any(|y| 
                                    x.pagenum == y.pagenum && 
                                    x.rownum == y.rownum &&
                                    x.row == y.row
                                )
                        }
                    );
                }

                // Now we need to map the rows in remove_source_diff to the rows in the target
                for row in remove_source_diff.rows_removed.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) = insert_map.get(&(row.pagenum, row.rownum)) {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }

                // Add the new mapped rows to the result_diffs
                result_diffs.table_diffs
                    .entry(remove_source_diff.table_name.clone())
                    .or_insert_with(|| TableSquashDiff::new(&remove_source_diff.table_name, &remove_source_diff.schema))
                    .remove_diff.rows_removed.append(&mut remove_source_diff.rows_removed);
            },
            Diff::TableCreate(table_create_source_diff) => {
                // Get the table_create diff from target_diffs_on_the_table if it exists
                let table_create_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::TableCreate(table_create_diff) => Some(table_create_diff),
                        _ => None,
                    });
                
                // If there is a table_create diff in the target, we need to remove any duplicate table creations.
                if let Some(table_create_diff_target) = table_create_diff_target_option {
                    // If the schema aren't the same, we need to add the table_create diff to the result_diffs
                    if table_create_source_diff.schema != table_create_diff_target.schema {
                        // TODO: Handle merge conflict here

                        // Add the new table creation to the result_diffs
                        result_diffs.table_diffs
                            .entry(table_create_source_diff.table_name.clone())
                            .or_insert_with(|| TableSquashDiff::new(&table_create_source_diff.table_name, &table_create_source_diff.schema))
                            .table_create_diff = table_create_source_diff.clone();
                    }
                }
            },
            Diff::TableRemove(table_remove_source_diff) => {
                // Get the table_remove diff from target_diffs_on_the_table if it exists
                let table_remove_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::TableRemove(table_remove_diff) => Some(table_remove_diff),
                        _ => None,
                    });
                
                // If there is a table_remove diff in the target, we need to remove any duplicate table removals.
                if let Some(table_remove_diff_target) = table_remove_diff_target_option {
                    // If the schema aren't the same, we need to add the table_remove diff to the result_diffs
                    if table_remove_source_diff.schema != table_remove_diff_target.schema {
                        // TODO: Handle merge conflict here
                        // What rows should be in the table_remove diff?

                        // Add the new table creation to the result_diffs
                        result_diffs.table_diffs
                            .entry(table_remove_source_diff.table_name.clone())
                            .or_insert_with(|| TableSquashDiff::new(&table_remove_source_diff.table_name, &table_remove_source_diff.schema))
                            .table_remove_diff = table_remove_source_diff.clone();
                    }
                }
            },
        }
    }

    // Now the result_diffs contains all the diffs that need to be applied to the target to get the source merged in

    // We need to check for merge conflicts in the result_diffs now
    


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