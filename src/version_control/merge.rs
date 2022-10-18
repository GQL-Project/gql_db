use crate::{user::userdata::User, fileio::{databaseio::get_db_instance, tableio::Table}, util::row::{EmptyRowLocation, RowInfo, RowLocation, self}};

use super::diff::*;
use std::collections::HashMap;

pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
}

/// Merges a single diff to merge into the list of diffs to merge into using a merge conflict algorithm
/// Returns a new list of diffs that would be the result of applying source_diffs into target_diffs 
pub fn create_merge_diffs(
    source_diffs: &Vec<Diff>,                         // The source diffs to merge into the target diffs
    target_diffs: &Vec<Diff>,                         // The target diffs to merge the source diff into                    
    target_table_dir: &String,                        // The directory where the target branch tables are stored
    conflict_res_algo: MergeConflictResolutionAlgo    // The merge conflict resolution algorithm to use
) -> Result<Vec<Diff>, String> {
    // We assume target_diffs_on_the_table only contains one diff of each type for that table
    verify_only_one_type_of_diff_per_table(target_diffs)?;

    // The result of the merge without any conflicts resolved
    let mut result_diffs: SquashDiffs = SquashDiffs::new();

    // Maps (pagenum, rownum) in source to (pagenum, rownum) in the target
    let mut insert_map: HashMap<(u32, u16), (u32, u16)> = HashMap::new(); 

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
                
                // We need to map the rows in insert_source_diff to open rows in the target
                // Find the open rows in the target
                let open_rows: Vec<EmptyRowLocation> = get_db_instance()?
                    .get_open_rows_in_table(
                        &insert_source_diff.table_name, 
                        target_table_dir,
                        insert_source_diff.rows.len(),
                    )?;

                // Convert the EmptyRowLocations to a list of RowLocations
                let free_row_locations: Vec<RowLocation> = open_rows
                    .iter()
                    .map(|x| {
                        let mut rowlocations_free: Vec<RowLocation> = Vec::new();
                        
                        for row_count in 0..x.num_rows_empty {
                            rowlocations_free.push(RowLocation {
                                pagenum: x.location.pagenum,
                                rownum: x.location.rownum + row_count as u16,
                            });
                        }
                        return rowlocations_free;
                    })
                    .flatten()
                    .collect();

                // Map the rows in insert_source_diff to the open rows
                for (i, row) in insert_source_diff.rows.iter().enumerate() {
                    insert_map.insert((row.pagenum, row.rownum), (free_row_locations[i].pagenum, free_row_locations[i].rownum));

                    // Add the new mapped rows to the result_diffs
                    result_diffs.table_diffs
                        .entry(insert_source_diff.table_name.clone())
                        .or_insert_with(|| TableSquashDiff::new(&insert_source_diff.table_name, &insert_source_diff.schema))
                        .insert_diff.rows.push(RowInfo {
                            pagenum: free_row_locations[i].pagenum,
                            rownum: free_row_locations[i].rownum,
                            row: row.row.clone(),
                        });
                }
            },
            Diff::Update(mut update_source_diff) => {
                // We need to map the rows in update_source_diff to the rows in the target
                for row in update_source_diff.rows.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) = insert_map.get(&(row.pagenum, row.rownum)) {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }
                
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

                // Add the new mapped rows to the result_diffs
                result_diffs.table_diffs
                    .entry(update_source_diff.table_name.clone())
                    .or_insert_with(|| TableSquashDiff::new(&update_source_diff.table_name, &update_source_diff.schema))
                    .update_diff.rows.append(&mut update_source_diff.rows);
            },
            Diff::Remove(mut remove_source_diff) => {
                // We need to map the rows in remove_source_diff to the rows in the target
                for row in remove_source_diff.rows.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) = insert_map.get(&(row.pagenum, row.rownum)) {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }

                // Get the remove diff from target_diffs_on_the_table if it exists
                let remove_diff_target_option = target_diffs_on_the_table
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Remove(rem_diff) => Some(rem_diff),
                        _ => None,
                    });
                
                // If there is a remove diff in the target, we need to remove any duplicate row removals.
                if let Some(remove_diff_target) = remove_diff_target_option {
                    remove_source_diff.rows
                        .retain(|x| {
                            !remove_diff_target
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

                // Add the new mapped rows to the result_diffs
                result_diffs.table_diffs
                    .entry(remove_source_diff.table_name.clone())
                    .or_insert_with(|| TableSquashDiff::new(&remove_source_diff.table_name, &remove_source_diff.schema))
                    .remove_diff.rows.append(&mut remove_source_diff.rows);
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
                let mut is_duplicate_table_create: bool = false;
                if let Some(table_create_diff_target) = table_create_diff_target_option {
                    if table_create_source_diff.schema == table_create_diff_target.schema {
                        is_duplicate_table_create = true;
                    }
                }

                if !is_duplicate_table_create {
                    // Add the new table creation to the result_diffs
                    result_diffs.table_diffs
                        .entry(table_create_source_diff.table_name.clone())
                        .or_insert_with(|| TableSquashDiff::new(&table_create_source_diff.table_name, &table_create_source_diff.schema))
                        .table_create_diff = Some(table_create_source_diff.clone());
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
                let mut is_duplicate_table_remove: bool = false;
                if let Some(table_remove_diff_target) = table_remove_diff_target_option {
                    if table_remove_source_diff.schema == table_remove_diff_target.schema {
                        is_duplicate_table_remove = true;
                    }
                }

                if !is_duplicate_table_remove {
                    // Add the new table removal to the result_diffs
                        result_diffs.table_diffs
                        .entry(table_remove_source_diff.table_name.clone())
                        .or_insert_with(|| TableSquashDiff::new(&table_remove_source_diff.table_name, &table_remove_source_diff.schema))
                        .table_remove_diff = Some(table_remove_source_diff.clone());
                }
            },
        }
    }

    // Now the result_diffs contains all the diffs that need to be applied to the target to get the source merged in
    let (mut prereq_diffs, mut merge_diffs) = 
        handle_merge_conflicts(
            &mut result_diffs,
            target_diffs,
            target_table_dir, 
            conflict_res_algo
        )?;

    // Assemble the final diffs into a chronological list of diffs
    prereq_diffs.append(&mut merge_diffs);
    Ok(prereq_diffs)
}

/// Handles merge conflicts by applying the conflict resolution algorithm to the diffs.
/// Returns a tuple:
///    - The diffs that should be applied to the target as a prerequisite to get the source merged in
///    - The diffs that should be applied to the target to complete the merge
pub fn handle_merge_conflicts(
    processed_source_diffs: &mut SquashDiffs,         // The source diffs to merge into the target diffs
    target_diffs: &Vec<Diff>,                         // The target diffs to merge the source diff into
    target_table_dir: &String,                        // The directory where the target branch tables are located.
    conflict_res_algo: MergeConflictResolutionAlgo    // The merge conflict resolution algorithm to use
) -> Result<(Vec<Diff>, Vec<Diff>), String> {
    // Keep track of the diffs that need to be applied to the target before the source diffs can be applied
    let mut prereq_diffs: Vec<Diff> = Vec::new();

    // We need to check for merge conflicts in the processed_source_diffs now for each table
    let result_keys: Vec<String> = processed_source_diffs.table_diffs.keys().cloned().collect();
    for res_table_name in result_keys {
        // Get the result and target diffs for the same table name
        let res_table_diff = processed_source_diffs.table_diffs.get_mut(&res_table_name).unwrap();
        let target_table_diff: Vec<Diff> = target_diffs
            .iter()
            .filter(|diff| diff.get_table_name() == res_table_name)
            .cloned()
            .collect();

        
        /********** Insert **********/
        // These shouldn't really happen because we should have mapped the rows to the target
        // But we should check for them anyway
        let mut idx: usize = 0;
        'result_insert_loop: while idx < res_table_diff.insert_diff.rows.len() {
            let res_insert_row: RowInfo = res_table_diff.insert_diff.rows[idx].clone();
            
            // Get the target insert diff
            let target_insert_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Insert(ins_diff) => Some(ins_diff),
                    _ => None,
                });

            // If the target has a row inserted at the same location, we have a merge conflict
            if let Some(target_insert_diff) = target_insert_diff_opt {
                for target_insert_row in target_insert_diff.rows.iter() {
                    // If Merge Conflict
                    if res_insert_row.get_row_location() == target_insert_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, 
                                                   but row was also inserted at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            },
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Update(upd_diff) => Some(upd_diff),
                    _ => None,
                });

            // If the target has a row updated at the same location, we have a merge conflict
            if let Some(target_update_diff) = target_update_diff_opt {
                for target_update_row in target_update_diff.rows.iter() {
                    // If Merge Conflict
                    if res_insert_row.get_row_location() == target_update_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, 
                                                   but row was also updated at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            },
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Remove(del_diff) => Some(del_diff),
                    _ => None,
                });

            // If the target has a row removed at the same location, we have a merge conflict
            if let Some(target_remove_diff) = target_remove_diff_opt {
                for target_remove_row in target_remove_diff.rows.iter() {
                    // If Merge Conflict
                    if res_insert_row.get_row_location() == target_remove_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, 
                                                   but row was also removed at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            },
                        }
                    }
                }
            } // end target_remove_diff

            idx += 1;
        } // end of result insert loop

        
        /********** Update **********/
        let mut idx: usize = 0;
        'result_update_loop: while idx < res_table_diff.update_diff.rows.len() {
            let res_update_row: RowInfo = res_table_diff.update_diff.rows[idx].clone();
            
            // Get the target insert diff
            let target_insert_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Insert(ins_diff) => Some(ins_diff),
                    _ => None,
                });

            // If the target has a row inserted at the same location, we have a merge conflict
            if let Some(target_insert_diff) = target_insert_diff_opt {
                for target_insert_row in target_insert_diff.rows.iter() {
                    // If Merge Conflict
                    if res_update_row.get_row_location() == target_insert_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, 
                                                   but row was also inserted at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            },
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Update(upd_diff) => Some(upd_diff),
                    _ => None,
                });

            // If the target has a row updated at the same location, we have a merge conflict
            if let Some(target_update_diff) = target_update_diff_opt {
                for target_update_row in target_update_diff.rows.iter() {
                    // If Merge Conflict
                    if res_update_row.get_row_location() == target_update_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, 
                                                   but row was also updated at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            },
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Remove(del_diff) => Some(del_diff),
                    _ => None,
                });

            // If the target has a row removed at the same location, we have a merge conflict
            if let Some(target_remove_diff) = target_remove_diff_opt {
                for target_remove_row in target_remove_diff.rows.iter() {
                    // If Merge Conflict
                    if res_update_row.get_row_location() == target_remove_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, 
                                                   but row was also removed at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            },
                        }
                    }
                }
            } // end target_remove_diff

            idx += 1;
        } // end of result update loop


        /********** Remove **********/
        let mut idx: usize = 0;
        'result_remove_loop: while idx < res_table_diff.remove_diff.rows.len() {
            let res_remove_row: RowInfo = res_table_diff.remove_diff.rows[idx].clone();
            
            // Get the target insert diff
            let target_insert_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Insert(ins_diff) => Some(ins_diff),
                    _ => None,
                });

            // If the target has a row inserted at the same location, we have a merge conflict
            if let Some(target_insert_diff) = target_insert_diff_opt {
                for target_insert_row in target_insert_diff.rows.iter() {
                    // If Merge Conflict
                    if res_remove_row.get_row_location() == target_insert_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, 
                                                   but row was also inserted at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            },
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Update(upd_diff) => Some(upd_diff),
                    _ => None,
                });

            // If the target has a row updated at the same location, we have a merge conflict
            if let Some(target_update_diff) = target_update_diff_opt {
                for target_update_row in target_update_diff.rows.iter() {
                    // If Merge Conflict
                    if res_remove_row.get_row_location() == target_update_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, 
                                                   but row was also updated at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            },
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::Remove(del_diff) => Some(del_diff),
                    _ => None,
                });

            // If the target has a row removed at the same location, we have a merge conflict
            if let Some(target_remove_diff) = target_remove_diff_opt {
                for target_remove_row in target_remove_diff.rows.iter() {
                    // If Merge Conflict
                    if res_remove_row.get_row_location() == target_remove_row.get_row_location() {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, 
                                                   but row was also removed at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            },
                        }
                    }
                }
            } // end target_remove_diff

            idx += 1;
        } // end result_remove_loop


        /********** TableCreate **********/
        {
            // Get the target table create diff
            let target_table_create_diff_opt = target_table_diff
                .iter()
                .find_map(|diff| match diff {
                    Diff::TableCreate(table_create_diff) => Some(table_create_diff),
                    _ => None,
                });

            // If the target created a table
            if let Some(target_table_create_diff) = target_table_create_diff_opt {
                // If the result diffs also created a table
                if let Some(res_table_create_diff) = res_table_diff.table_create_diff.clone() {
                    // If they have different schema, that's a merge conflict
                    if target_table_create_diff.schema != res_table_create_diff.schema {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Table {} created in source, but table was also created in target with different schema", res_table_name));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the table from the target by keeping the source's table create
                                // and undoing the target's table create with a prerequisite remove table diff
                                let table: Table = Table::new(target_table_dir, &target_table_create_diff.table_name, None)?;
                                let table_rows: Vec<RowInfo> = table.into_iter().collect();

                                let table_remove_diff: Diff = Diff::TableRemove(TableRemoveDiff {
                                    table_name: target_table_create_diff.table_name.clone(),
                                    schema: target_table_create_diff.schema.clone(),
                                    rows_removed: table_rows,
                                });

                                prereq_diffs.push(table_remove_diff);
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table from the source by removing it from the res_table_diff
                                res_table_diff.table_create_diff = None;
                            },
                        }
                    }
                }
            } // end target_table_create_diff
        } // end result table create
        

        /********** TableRemove **********/
        // If the result diffs removed a table
        if let Some(res_table_remove_diff) = res_table_diff.table_remove_diff.clone() {
            // Boolean that keeps track of whether the res_table_remove_diff still exists after checking each merge conflict
            let mut res_table_remove_diff_exists: bool = true;
            
            if res_table_remove_diff_exists {
                // Get the target insert diff
                let target_insert_diff_opt = target_table_diff
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Insert(ins_diff) => Some(ins_diff),
                        _ => None,
                    });

                // If the target has a row inserted on the same table that the result removed, we have a merge conflict
                if let Some(target_insert_diff) = target_insert_diff_opt {
                    let target_inserted_rows: &Vec<RowInfo> = &target_insert_diff.rows;
                    // If Merge Conflict
                    if target_inserted_rows.len() > 0 {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed table {} in source, 
                                                    but rows were also inserted into the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the rows from the target by using a prerequisite remove row diff
                                // and keeping the source table remove diff.
                                // Each of the rows in the prereq remove row diff will only be the ones inserted in the target
                                let target_rows_remove_diff: Diff = Diff::Remove(RemoveDiff {
                                    table_name: target_insert_diff.table_name.clone(),
                                    schema: target_insert_diff.schema.clone(),
                                    rows: target_inserted_rows.clone(),
                                });

                                prereq_diffs.push(target_rows_remove_diff);
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            },
                        }
                    }
                } // end target_insert_diff
            }

            if res_table_remove_diff_exists {
                // Get the target update diff
                let target_update_diff_opt = target_table_diff
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Update(upd_diff) => Some(upd_diff),
                        _ => None,
                    });

                // If the target has a row updated on the same table that the result removed, we have a merge conflict
                if let Some(target_update_diff) = target_update_diff_opt {
                    let target_updated_rows: &Vec<RowInfo> = &target_update_diff.rows;
                    // If Merge Conflict
                    if target_updated_rows.len() > 0 {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed table {} in source, 
                                                    but rows were also updated in the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the rows from the target by using a prerequisite remove row diff
                                // and keeping the source table remove diff.
                                // Each of the rows in the prereq remove row diff will only be the ones updated in the target
                                let target_rows_remove_diff: Diff = Diff::Remove(RemoveDiff {
                                    table_name: target_update_diff.table_name.clone(),
                                    schema: target_update_diff.schema.clone(),
                                    rows: target_updated_rows.clone(),
                                });

                                prereq_diffs.push(target_rows_remove_diff);
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            },
                        }
                    }
                } // end target_update_diff
            }

            if res_table_remove_diff_exists {
                // Get the target remove diff
                let target_remove_diff_opt = target_table_diff
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::Remove(rem_diff) => Some(rem_diff),
                        _ => None,
                    });

                // If the target has a row removed on the same table that the result removed, we have a merge conflict
                if let Some(target_remove_diff) = target_remove_diff_opt {
                    let target_removed_rows: &Vec<RowInfo> = &target_remove_diff.rows;
                    // If Merge Conflict
                    if target_removed_rows.len() > 0 {
                        match conflict_res_algo {
                            MergeConflictResolutionAlgo::NoConflicts => {
                                // We don't want to handle merge conflicts, so just throw error
                                return Err(format!("Merge Conflict: Removed table {} in source, 
                                                    but rows were also removed from the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            },
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the rows from the target by using a prerequisite insert row diff
                                // and keep the source table remove diff.
                                // Each of the rows in the prereq insert row diff will only be the ones removed in the target
                                let target_rows_insert_diff: Diff = Diff::Insert(InsertDiff {
                                    table_name: target_remove_diff.table_name.clone(),
                                    schema: target_remove_diff.schema.clone(),
                                    rows: target_removed_rows.clone(),
                                });

                                prereq_diffs.push(target_rows_insert_diff);
                            },
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            },
                        }
                    }
                } // end target_remove_diff
            }

            if res_table_remove_diff_exists {
                // Get the target table remove diff
                let target_table_remove_diff_opt = target_table_diff
                    .iter()
                    .find_map(|diff| match diff {
                        Diff::TableRemove(table_remove_diff) => Some(table_remove_diff),
                        _ => None,
                    });

                // If the target also removed a table of the same name
                if let Some(target_table_remove_diff) = target_table_remove_diff_opt {
                    // If they have different schema, that's a merge conflict
                    if target_table_remove_diff.schema != res_table_remove_diff.schema {
                        // This should NEVER EVER happen, so throw an error
                        // That's because there are 2 scenarios where remove table can happen with different schemas:
                        // 1. Both source and target create the table with different schema and remove it.
                        //    When squashed, this would remove the table from the source and target, so this wouldn't happen here.
                        // 2. The common ancestor creates a table, and the target recreates the table with a different schema before
                        //    removing it. Then the source removes the original table. That won't cause a conflict because when the
                        //    target gets squashed, it would remove the recreated table from the target alltogether, so we would
                        //    be left with two remove tables for the same schema.
                        return Err(format!("Merge Conflict: Table {} removed in source, but table was also removed in target with different schema", res_table_name));
                    }
                } // end target_table_remove_diff
            }
        } // end result table remove
    } // end foreach table name key in source
    // Assemble the result diff into a vec of diffs
    let mut res_diffs: Vec<Diff> = Vec::new();
    for (_table_name, table_diff) in &processed_source_diffs.table_diffs {
        // Only add the diff if it affects at least one row or table
        if table_diff.insert_diff.rows.len() > 0 {
            res_diffs.push(Diff::Insert(table_diff.insert_diff.clone()));
        }
        if table_diff.update_diff.rows.len() > 0 {
            res_diffs.push(Diff::Update(table_diff.update_diff.clone()));
        }
        if table_diff.remove_diff.rows.len() > 0 {
            res_diffs.push(Diff::Remove(table_diff.remove_diff.clone()));
        }
        if table_diff.table_create_diff.is_some() {
            res_diffs.push(Diff::TableCreate(table_diff.table_create_diff.clone().unwrap()));
        }
        if table_diff.table_remove_diff.is_some() {
            res_diffs.push(Diff::TableRemove(table_diff.table_remove_diff.clone().unwrap()));
        }
    }

    Ok((prereq_diffs, res_diffs))
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


#[cfg(test)]
mod tests {
    use std::path::Path;

    use serial_test::serial;

    use crate::{
        fileio::{
            databaseio::{create_db_instance, delete_db_instance, get_db_instance, MAIN_BRANCH_NAME},
            header::Schema, tableio::create_table_in_dir,
        },
        util::{dbtype::*, row::Row},
        version_control::{commit::Commit, diff::Diff}, executor::query::create_table,
    };

    use super::*;

    #[test]
    #[serial]
    fn test_basic_insert_merge() {
        // Tests inserting rows into the source branch and merging them into the target branch

        // Create the database
        let (mut user, 
            src_branch, 
            target_branch, 
            src_branch_dir, 
            target_branch_dir,
            table_name1,
            table_name2
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Insert rows into the source branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("John".to_string())],
            vec![Value::I32(2), Value::String("Jane".to_string())],
            vec![Value::I32(3), Value::String("Joe".to_string())],
        ];
        let mut src_table1: Table =
            Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = src_table1.insert_rows(rows).unwrap();
        src_diffs.push(Diff::Insert(insert_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs, 
            &target_diffs, 
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts
        ).unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Insert(insert_diff) = &merge_diffs[0] {
            assert_eq!(insert_diff.rows.len(), 3);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(insert_diff.rows[0].row[1], Value::String("John".to_string()));
            assert_eq!(insert_diff.rows[1].row.len(), 2);
            assert_eq!(insert_diff.rows[1].row[0], Value::I32(2));
            assert_eq!(insert_diff.rows[1].row[1], Value::String("Jane".to_string()));
            assert_eq!(insert_diff.rows[2].row.len(), 2);
            assert_eq!(insert_diff.rows[2].row[0], Value::I32(3));
            assert_eq!(insert_diff.rows[2].row[1], Value::String("Joe".to_string()));
        } else {
            panic!("Expected insert diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_insert_merge() {
        // Tests inserting rows into both the source branch and the target branch then merging them into the target branch

        // Create the database
        let (mut user, 
            src_branch, 
            target_branch, 
            src_branch_dir, 
            target_branch_dir,
            table_name1,
            table_name2
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Insert rows into the source branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("John".to_string())],
            vec![Value::I32(2), Value::String("Jane".to_string())],
            vec![Value::I32(3), Value::String("Joe".to_string())],
        ];
        let mut src_table1: Table =
            Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = src_table1.insert_rows(rows).unwrap();
        src_diffs.push(Diff::Insert(insert_diff));

        // Insert rows into the target branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(4), Value::String("John".to_string())],
            vec![Value::I32(2), Value::String("Jane".to_string())], // Same as source branch
            vec![Value::I32(6), Value::String("Joe".to_string())],
        ];
        let mut target_table1: Table =
            Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = target_table1.insert_rows(rows).unwrap();
        target_diffs.push(Diff::Insert(insert_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs, 
            &target_diffs, 
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts
        ).unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Insert(insert_diff) = &merge_diffs[0] {
            assert_eq!(insert_diff.rows.len(), 2);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(insert_diff.rows[0].row[1], Value::String("John".to_string()));
            assert_eq!(insert_diff.rows[1].row.len(), 2);
            assert_eq!(insert_diff.rows[1].row[0], Value::I32(3));
            assert_eq!(insert_diff.rows[1].row[1], Value::String("Joe".to_string()));
        } else {
            panic!("Expected insert diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_basic_table_create_merge() {
        // Tests creating a table in the source branch then merging it into the target branch

        // Create the database
        let (mut user, 
            src_branch, 
            target_branch, 
            src_branch_dir, 
            target_branch_dir,
            table_name1,
            table_name2
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Create a table in the source branch
        let src_new_table_name: String = "new_table_123".to_string();
        let src_new_table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("score".to_string(), Column::Float),
        ];
        let (src_new_table, src_table_create_diff) = 
            create_table_in_dir(
                &src_new_table_name, 
                &src_new_table_schema, 
                &src_branch_dir
            ).unwrap();
        src_diffs.push(Diff::TableCreate(src_table_create_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs, 
            &target_diffs, 
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts
        ).unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), src_new_table_name);
        if let Diff::TableCreate(create_diff) = &merge_diffs[0] {
            assert_eq!(create_diff.table_name, src_new_table_name);
            assert_eq!(create_diff.schema, src_new_table_schema);
        } else {
            panic!("Expected create diff");
        }

        // Clean up the database
        delete_test_db();
    }

    // Sets up the database and creates 2 tables on the main branch for first commit.
    // Also creates a branch off of the first commit on the main branch.
    // Returns the a tuple:
    //   - The user
    //   - The source branch
    //   - The target branch
    //   - The source branch directory
    //   - The target branch directory
    //   - The name of the first table
    //   - The name of the second table
    fn setup_test_db() -> (User, String, String, String, String, String, String) {
        // delete database if it already exists
        let path = std::env::current_exe().unwrap();
        let mut db_dir: String = path
            .canonicalize()
            .expect("The current exe should exist")
            .parent()
            .unwrap()
            .to_string_lossy()
            .to_string();

        db_dir.push_str("/databases"); // Append the databases directory to the path
        db_dir = db_dir.replace("\\\\?\\", ""); // remove wonkiness on Windows
        if Path::new(&db_dir).exists() {
            std::fs::remove_dir_all(db_dir).unwrap();
        }

        // Create the database
        let db_name: String = "test_merge".to_string();
        create_db_instance(&db_name).unwrap();

        // Table names
        let table_name_1: String = "table1".to_string();
        let table_name_2: String = "table2".to_string();

        // Branch name
        let branch_name: String = "test_branch".to_string();

        // Diffs applied to the main branch
        let mut main_branch_diffs: Vec<Diff> = Vec::new();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (_, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Create a second table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let (_, table_create_diff2) = create_table(
            &table_name_2,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff2));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(), 
                &"Create 2 Tables".to_string(), 
                &mut user,
                None
            ).unwrap();

        // Create a branch off of the first commit
        get_db_instance().unwrap().create_branch(&branch_name, &mut user).unwrap();

        // Get the two branch directories
        let new_branch_dir: String = get_db_instance().unwrap().get_branch_path_from_name(&branch_name);
        let main_branch_dir: String = get_db_instance().unwrap().get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        user.set_diffs(&Vec::new());
        (user, branch_name, MAIN_BRANCH_NAME.to_string(), new_branch_dir, main_branch_dir, table_name_1, table_name_2)
    }

    // Cleans up the database
    fn delete_test_db() {
        delete_db_instance().unwrap();
    }
}