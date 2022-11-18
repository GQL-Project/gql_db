use crate::{
    fileio::{databaseio::get_db_instance, tableio::Table},
    util::row::{EmptyRowLocation, RowInfo, RowLocation},
};

use super::diff::*;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
}

/// Merges a single diff to merge into the list of diffs to merge into using a merge conflict algorithm
/// Returns a new list of diffs that would be the result of applying source_diffs into target_diffs
pub fn create_merge_diffs(
    source_diffs: &Vec<Diff>,  // The source diffs to merge into the target diffs
    target_diffs: &Vec<Diff>,  // The target diffs to merge the source diff into
    target_table_dir: &String, // The directory where the target branch tables are stored
    conflict_res_algo: MergeConflictResolutionAlgo, // The merge conflict resolution algorithm to use
) -> Result<Vec<Diff>, String> {
    // We assume target_diffs_on_the_table only contains one diff of each type for that table
    verify_only_one_type_of_diff_per_table(target_diffs)?;

    // Keep track of anything we need to do before applying the merge diffs
    let mut prev_merge_diffs: Vec<Diff> = Vec::new();

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
                let insert_diff_target_option =
                    target_diffs_on_the_table
                        .iter()
                        .find_map(|diff| match diff {
                            Diff::Insert(ins_diff) => Some(ins_diff),
                            _ => None,
                        });

                // If there is an insert diff in the target, we need to remove any duplicate row insertions.
                if let Some(insert_diff_target) = insert_diff_target_option {
                    insert_source_diff.rows.retain(|x| {
                        !insert_diff_target.rows.iter().any(|y| {
                            x.pagenum == y.pagenum && x.rownum == y.rownum && x.row == y.row
                        })
                    });
                }

                // If the table does not exist in the target branch
                if get_db_instance()?
                    .get_table_path_from_dir(&insert_source_diff.table_name, target_table_dir)
                    .is_err()
                {
                    // Check if we have a create table in the source diffs
                    let create_table_diff_option =
                        source_diffs.iter().find_map(|diff| match diff {
                            Diff::TableCreate(create_table_diff) => {
                                if create_table_diff.table_name == insert_source_diff.table_name {
                                    Some(create_table_diff)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        });

                    // If we don't create the table in the source diffs, that means the table was deleted in the target branch
                    // This is a merge conflict if our algorithm is NoConflicts
                    if create_table_diff_option.is_none() {
                        if conflict_res_algo == MergeConflictResolutionAlgo::NoConflicts {
                            return Err(
                                format!(
                                    "Merge Conflict: Table {} has been deleted in target branch, but has rows inserted in the source branch", 
                                    insert_source_diff.table_name
                                )
                            );
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseSource {
                            // Get the table remove diff from target_diffs_on_the_table if it exists
                            let remove_table_diff_target_option = target_diffs_on_the_table
                                .iter()
                                .find_map(|diff| match diff {
                                    Diff::TableRemove(tab_remove_diff) => Some(tab_remove_diff),
                                    _ => None,
                                });

                            if remove_table_diff_target_option.is_some() {
                                let remove_table_diff_target: &TableRemoveDiff =
                                    remove_table_diff_target_option.unwrap();
                                let create_table_target_premerge: TableCreateDiff =
                                    TableCreateDiff {
                                        table_name: remove_table_diff_target.table_name.clone(),
                                        schema: remove_table_diff_target.schema.clone(),
                                    };
                                prev_merge_diffs
                                    .push(Diff::TableCreate(create_table_target_premerge));
                                let insert_table_target_premerge: InsertDiff = InsertDiff {
                                    table_name: remove_table_diff_target.table_name.clone(),
                                    schema: remove_table_diff_target.schema.clone(),
                                    rows: remove_table_diff_target.rows_removed.clone(),
                                };
                                prev_merge_diffs.push(Diff::Insert(insert_table_target_premerge));
                            } else {
                                return Err(
                                    format!(
                                        "Merge Conflict: Table Create Diff for {} not found in target branch", 
                                        insert_source_diff.table_name
                                    )
                                );
                            }
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseTarget {
                            continue;
                        }
                    }
                    // Otherwise, we just insert the rows into the table since we are creating it in the source branch
                    else {
                        // Add the new mapped rows to the result_diffs
                        result_diffs
                            .table_diffs
                            .entry(insert_source_diff.table_name.clone())
                            .or_insert_with(|| {
                                TableSquashDiff::new(
                                    &insert_source_diff.table_name,
                                    &insert_source_diff.schema,
                                )
                            })
                            .insert_diff
                            .rows
                            .append(insert_source_diff.rows.as_mut());
                    }
                }
                // The table does exist on the target branch
                else {
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
                        insert_map.insert(
                            (row.pagenum, row.rownum),
                            (free_row_locations[i].pagenum, free_row_locations[i].rownum),
                        );

                        // Add the new mapped rows to the result_diffs
                        result_diffs
                            .table_diffs
                            .entry(insert_source_diff.table_name.clone())
                            .or_insert_with(|| {
                                TableSquashDiff::new(
                                    &insert_source_diff.table_name,
                                    &insert_source_diff.schema,
                                )
                            })
                            .insert_diff
                            .rows
                            .push(RowInfo {
                                pagenum: free_row_locations[i].pagenum,
                                rownum: free_row_locations[i].rownum,
                                row: row.row.clone(),
                            });
                    }
                }
            }
            Diff::Update(mut update_source_diff) => {
                // If the table does not exist in the target branch
                if get_db_instance()?
                    .get_table_path_from_dir(&update_source_diff.table_name, target_table_dir)
                    .is_err()
                {
                    // Check if we have a create table in the source diffs
                    let create_table_diff_option =
                        source_diffs.iter().find_map(|diff| match diff {
                            Diff::TableCreate(create_table_diff) => {
                                if create_table_diff.table_name == update_source_diff.table_name {
                                    Some(create_table_diff)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        });

                    // If we don't create the table in the source diffs, that means the table was deleted in the target branch
                    // This is a merge conflict if our algorithm is NoConflicts
                    if create_table_diff_option.is_none() {
                        if conflict_res_algo == MergeConflictResolutionAlgo::NoConflicts {
                            return Err(
                                format!(
                                    "Merge Conflict: Table {} has been deleted in target branch, but is updated in the source branch", 
                                    update_source_diff.table_name
                                )
                            );
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseSource {
                            // Get the table remove diff from target_diffs_on_the_table if it exists
                            let remove_table_diff_target_option = target_diffs_on_the_table
                                .iter()
                                .find_map(|diff| match diff {
                                    Diff::TableRemove(tab_remove_diff) => Some(tab_remove_diff),
                                    _ => None,
                                });

                            if remove_table_diff_target_option.is_some() {
                                let remove_table_diff_target: &TableRemoveDiff =
                                    remove_table_diff_target_option.unwrap();
                                let create_table_target_premerge: TableCreateDiff =
                                    TableCreateDiff {
                                        table_name: remove_table_diff_target.table_name.clone(),
                                        schema: remove_table_diff_target.schema.clone(),
                                    };
                                prev_merge_diffs
                                    .push(Diff::TableCreate(create_table_target_premerge));
                                let insert_table_target_premerge: InsertDiff = InsertDiff {
                                    table_name: remove_table_diff_target.table_name.clone(),
                                    schema: remove_table_diff_target.schema.clone(),
                                    rows: remove_table_diff_target.rows_removed.clone(),
                                };
                                prev_merge_diffs.push(Diff::Insert(insert_table_target_premerge));
                            } else {
                                return Err(
                                    format!(
                                        "Merge Conflict: Table Create Diff for {} not found in target branch", 
                                        update_source_diff.table_name
                                    )
                                );
                            }
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseTarget {
                            continue;
                        }
                    }
                }

                // We need to map the rows in update_source_diff to the rows in the target
                for row in update_source_diff.rows.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) =
                        insert_map.get(&(row.pagenum, row.rownum))
                    {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }

                // Get the update diff from target_diffs_on_the_table if it exists
                let update_diff_target_option =
                    target_diffs_on_the_table
                        .iter()
                        .find_map(|diff| match diff {
                            Diff::Update(up_diff) => Some(up_diff),
                            _ => None,
                        });

                // If there is an update diff in the target, we need to remove any duplicate row updates.
                if let Some(update_diff_target) = update_diff_target_option {
                    update_source_diff.rows.retain(|x| {
                        !update_diff_target.rows.iter().any(|y| {
                            x.pagenum == y.pagenum && x.rownum == y.rownum && x.row == y.row
                        })
                    });
                }

                // Retain the old rows
                update_source_diff.old_rows.retain(|x| {
                    update_source_diff
                        .rows
                        .iter()
                        .any(|y| x.pagenum == y.pagenum && x.rownum == y.rownum)
                });

                // Add the new mapped rows to the result_diffs
                result_diffs
                    .table_diffs
                    .entry(update_source_diff.table_name.clone())
                    .or_insert_with(|| {
                        TableSquashDiff::new(
                            &update_source_diff.table_name,
                            &update_source_diff.schema,
                        )
                    })
                    .update_diff
                    .rows
                    .append(&mut update_source_diff.rows);

                result_diffs
                    .table_diffs
                    .entry(update_source_diff.table_name.clone())
                    .or_insert_with(|| {
                        TableSquashDiff::new(
                            &update_source_diff.table_name,
                            &update_source_diff.schema,
                        )
                    })
                    .update_diff
                    .old_rows
                    .append(&mut update_source_diff.old_rows);
            }
            Diff::Remove(mut remove_source_diff) => {
                // If the table does not exist in the target branch
                if get_db_instance()?
                    .get_table_path_from_dir(&remove_source_diff.table_name, target_table_dir)
                    .is_err()
                {
                    // Check if we have a create table in the source diffs
                    let create_table_diff_option =
                        source_diffs.iter().find_map(|diff| match diff {
                            Diff::TableCreate(create_table_diff) => {
                                if create_table_diff.table_name == remove_source_diff.table_name {
                                    Some(create_table_diff)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        });

                    // If we don't create the table in the source diffs, that means the table was deleted in the target branch
                    // This is a merge conflict if our algorithm is NoConflicts
                    if create_table_diff_option.is_none() {
                        if conflict_res_algo == MergeConflictResolutionAlgo::NoConflicts {
                            return Err(
                                format!(
                                    "Table {} has been deleted in target branch, but has rows removed from it in the source branch", 
                                    remove_source_diff.table_name
                                )
                            );
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseSource {
                            // Get the table remove diff from target_diffs_on_the_table if it exists
                            let remove_table_diff_target_option = target_diffs_on_the_table
                                .iter()
                                .find_map(|diff| match diff {
                                    Diff::TableRemove(tab_remove_diff) => Some(tab_remove_diff),
                                    _ => None,
                                });

                            if remove_table_diff_target_option.is_some() {
                                let remove_table_diff_target: &TableRemoveDiff =
                                    remove_table_diff_target_option.unwrap();
                                let create_table_target_premerge: TableCreateDiff =
                                    TableCreateDiff {
                                        table_name: remove_table_diff_target.table_name.clone(),
                                        schema: remove_table_diff_target.schema.clone(),
                                    };
                                prev_merge_diffs
                                    .push(Diff::TableCreate(create_table_target_premerge));
                                let insert_table_target_premerge: InsertDiff = InsertDiff {
                                    table_name: remove_table_diff_target.table_name.clone(),
                                    schema: remove_table_diff_target.schema.clone(),
                                    rows: remove_table_diff_target.rows_removed.clone(),
                                };
                                prev_merge_diffs.push(Diff::Insert(insert_table_target_premerge));
                            } else {
                                return Err(
                                    format!(
                                        "Merge Conflict: Table Create Diff for {} not found in target branch", 
                                        remove_source_diff.table_name
                                    )
                                );
                            }
                        } else if conflict_res_algo == MergeConflictResolutionAlgo::UseTarget {
                            continue;
                        }
                    }
                }

                // We need to map the rows in remove_source_diff to the rows in the target
                for row in remove_source_diff.rows.iter_mut() {
                    // If it is mapped to the target, use the mapped row location
                    if let Some((target_pagenum, target_rownum)) =
                        insert_map.get(&(row.pagenum, row.rownum))
                    {
                        row.pagenum = *target_pagenum;
                        row.rownum = *target_rownum;
                    }
                    // If it is not mapped to the target, use the normal row location, so nothing needs to be done
                }

                // Get the remove diff from target_diffs_on_the_table if it exists
                let remove_diff_target_option =
                    target_diffs_on_the_table
                        .iter()
                        .find_map(|diff| match diff {
                            Diff::Remove(rem_diff) => Some(rem_diff),
                            _ => None,
                        });

                // If there is a remove diff in the target, we need to remove any duplicate row removals.
                if let Some(remove_diff_target) = remove_diff_target_option {
                    remove_source_diff.rows.retain(|x| {
                        !remove_diff_target.rows.iter().any(|y| {
                            x.pagenum == y.pagenum && x.rownum == y.rownum && x.row == y.row
                        })
                    });
                }

                // Add the new mapped rows to the result_diffs
                result_diffs
                    .table_diffs
                    .entry(remove_source_diff.table_name.clone())
                    .or_insert_with(|| {
                        TableSquashDiff::new(
                            &remove_source_diff.table_name,
                            &remove_source_diff.schema,
                        )
                    })
                    .remove_diff
                    .rows
                    .append(&mut remove_source_diff.rows);
            }
            Diff::TableCreate(table_create_source_diff) => {
                // Get the table_create diff from target_diffs_on_the_table if it exists
                let table_create_diff_target_option =
                    target_diffs_on_the_table
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
                    result_diffs
                        .table_diffs
                        .entry(table_create_source_diff.table_name.clone())
                        .or_insert_with(|| {
                            TableSquashDiff::new(
                                &table_create_source_diff.table_name,
                                &table_create_source_diff.schema,
                            )
                        })
                        .table_create_diff = Some(table_create_source_diff.clone());
                }
            }
            Diff::TableRemove(table_remove_source_diff) => {
                // Get the table_remove diff from target_diffs_on_the_table if it exists
                let table_remove_diff_target_option =
                    target_diffs_on_the_table
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
                    result_diffs
                        .table_diffs
                        .entry(table_remove_source_diff.table_name.clone())
                        .or_insert_with(|| {
                            TableSquashDiff::new(
                                &table_remove_source_diff.table_name,
                                &table_remove_source_diff.schema,
                            )
                        })
                        .table_remove_diff = Some(table_remove_source_diff.clone());
                }
            }
            Diff::IndexCreate(mut index_create_source_diff) => {
                // Get the index_create diff from target_diffs_on_the_table if it exists
                let index_create_diff_target_option =
                    target_diffs_on_the_table
                        .iter()
                        .find_map(|diff| match diff {
                            Diff::IndexCreate(index_create_diff) => Some(index_create_diff),
                            _ => None,
                        });

                // If there is a index_create diff in the target, we need to remove any duplicate index creations.
                // If there is an insert diff in the target, we need to remove any duplicate row insertions.
                if let Some(index_create_diff_target) = index_create_diff_target_option {
                    index_create_source_diff
                        .indexes
                        .retain(|(x_idx_name, x_idx_id)| {
                            !index_create_diff_target.indexes.iter().any(
                                |(y_idx_name, y_idx_id)| {
                                    x_idx_name == y_idx_name && x_idx_id == y_idx_id
                                },
                            )
                        });
                }

                // Add the new index creation to the result_diffs
                result_diffs
                    .table_diffs
                    .entry(index_create_source_diff.table_name.clone())
                    .or_insert_with(|| {
                        TableSquashDiff::new(
                            &index_create_source_diff.table_name,
                            &index_create_source_diff.schema,
                        )
                    })
                    .index_create_diff
                    .indexes
                    .extend(index_create_source_diff.indexes);
            }
            Diff::IndexRemove(mut index_remove_source_diff) => {
                // Get the index_remove diff from target_diffs_on_the_table if it exists
                let index_remove_diff_target_option =
                    target_diffs_on_the_table
                        .iter()
                        .find_map(|diff| match diff {
                            Diff::IndexRemove(index_remove_diff) => Some(index_remove_diff),
                            _ => None,
                        });

                // If there is a index_remove diff in the target, we need to remove any duplicate index creations.
                // If there is an insert diff in the target, we need to remove any duplicate row insertions.
                if let Some(index_remove_diff_target) = index_remove_diff_target_option {
                    index_remove_source_diff
                        .indexes
                        .retain(|(x_idx_name, x_idx_id)| {
                            !index_remove_diff_target.indexes.iter().any(
                                |(y_idx_name, y_idx_id)| {
                                    x_idx_name == y_idx_name && x_idx_id == y_idx_id
                                },
                            )
                        });
                }

                // Add the new index creation to the result_diffs
                result_diffs
                    .table_diffs
                    .entry(index_remove_source_diff.table_name.clone())
                    .or_insert_with(|| {
                        TableSquashDiff::new(
                            &index_remove_source_diff.table_name,
                            &index_remove_source_diff.schema,
                        )
                    })
                    .index_remove_diff
                    .indexes
                    .extend(index_remove_source_diff.indexes);
            }
        }
    }

    // Now the result_diffs contains all the diffs that need to be applied to the target to get the source merged in
    let (mut prereq_diffs, mut merge_diffs) = handle_merge_conflicts(
        &mut result_diffs,
        target_diffs,
        target_table_dir,
        conflict_res_algo,
    )?;

    // Assemble the final diffs into a chronological list of diffs
    prev_merge_diffs.append(&mut prereq_diffs);
    prev_merge_diffs.append(&mut merge_diffs);
    Ok(prev_merge_diffs)
}

/// Handles merge conflicts by applying the conflict resolution algorithm to the diffs.
/// Returns a tuple:
///    - The diffs that should be applied to the target as a prerequisite to get the source merged in
///    - The diffs that should be applied to the target to complete the merge
fn handle_merge_conflicts(
    processed_source_diffs: &mut SquashDiffs, // The source diffs to merge into the target diffs
    target_diffs: &Vec<Diff>,                 // The target diffs to merge the source diff into
    target_table_dir: &String, // The directory where the target branch tables are located.
    conflict_res_algo: MergeConflictResolutionAlgo, // The merge conflict resolution algorithm to use
) -> Result<(Vec<Diff>, Vec<Diff>), String> {
    // Keep track of the diffs that need to be applied to the target before the source diffs can be applied
    let mut prereq_diffs: Vec<Diff> = Vec::new();

    // We need to check for merge conflicts in the processed_source_diffs now for each table
    let result_keys: Vec<String> = processed_source_diffs.table_diffs.keys().cloned().collect();
    for res_table_name in result_keys {
        // Get the result and target diffs for the same table name
        let res_table_diff = processed_source_diffs
            .table_diffs
            .get_mut(&res_table_name)
            .unwrap();
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
            let target_insert_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, but row was also inserted at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            }
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, but row was also updated at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            }
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, but row was also removed at the same location in the target", 
                                                   res_insert_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.insert_diff
                                res_table_diff.insert_diff.rows.remove(idx);
                                // continue to next result insert row without incrementing idx because we removed an element
                                continue 'result_insert_loop;
                            }
                        }
                    }
                }
            } // end target_remove_diff

            // Get the target table remove diff
            let target_table_remove_diff_opt =
                target_table_diff.iter().find_map(|diff| match diff {
                    Diff::TableRemove(table_del_diff) => Some(table_del_diff),
                    _ => None,
                });

            // If the target has a table removed, we have a merge conflict
            if let Some(_target_table_remove_diff) = target_table_remove_diff_opt {
                match conflict_res_algo {
                    MergeConflictResolutionAlgo::NoConflicts => {
                        // We don't want to handle merge conflicts, so just throw error
                        return Err(format!("Merge Conflict: Inserted row at location {:?} in table {} in source, but table was also removed in the target", 
                                           res_insert_row.get_row_location(),
                                           res_table_name));
                    }
                    MergeConflictResolutionAlgo::UseSource => {
                        // Keep the source's row changes by removing the table remove diff from the target
                        println!("Removing table remove diff from target");
                        res_table_diff.table_remove_diff = None;
                    }
                    MergeConflictResolutionAlgo::UseTarget => {
                        // Remove the row from the source by removing it from the res_table_diff.insert_diff
                        res_table_diff.insert_diff.rows.remove(idx);
                        // continue to next result insert row without incrementing idx because we removed an element
                        continue 'result_insert_loop;
                    }
                }
            } // end target_table_remove_diff

            idx += 1;
        } // end of result insert loop

        /********** Update **********/
        let mut idx: usize = 0;
        'result_update_loop: while idx < res_table_diff.update_diff.rows.len() {
            let res_update_row: RowInfo = res_table_diff.update_diff.rows[idx].clone();

            // Get the target insert diff
            let target_insert_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, but row was also inserted at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            }
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, but row was also updated at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by keeping the source row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            }
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Updated row at location {:?} in table {} in source, but row was also removed at the same location in the target", 
                                                   res_update_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Overwrite the target row with the source row by changing the source row from an update to an insert
                                res_table_diff.insert_diff.rows.push(res_update_row.clone());
                                res_table_diff.update_diff.rows.remove(idx);
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.update_diff
                                res_table_diff.update_diff.rows.remove(idx);
                                // continue to next result update row without incrementing idx because we removed an element
                                continue 'result_update_loop;
                            }
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
            let target_insert_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, but row was also inserted at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            }
                        }
                    }
                }
            } // end target_insert_diff

            // Get the target update diff
            let target_update_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, but row was also updated at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            }
                        }
                    }
                }
            } // end target_update_diff

            // Get the target remove diff
            let target_remove_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed row at location {:?} in table {} in source, but row was also removed at the same location in the target", 
                                                   res_remove_row.get_row_location(),
                                                   res_table_name));
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the row from the target by keeping the source's remove row
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the row from the source by removing it from the res_table_diff.remove_diff
                                res_table_diff.remove_diff.rows.remove(idx);
                                // continue to next result remove row without incrementing idx because we removed an element
                                continue 'result_remove_loop;
                            }
                        }
                    }
                }
            } // end target_remove_diff

            idx += 1;
        } // end result_remove_loop

        /********** TableCreate **********/
        {
            // Get the target table create diff
            let target_table_create_diff_opt =
                target_table_diff.iter().find_map(|diff| match diff {
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
                            }
                            MergeConflictResolutionAlgo::UseSource => {
                                // Remove the table from the target by keeping the source's table create
                                // and undoing the target's table create with a prerequisite remove table diff
                                let table: Table = Table::new(
                                    target_table_dir,
                                    &target_table_create_diff.table_name,
                                    None,
                                )?;
                                let table_rows: Vec<RowInfo> = table.into_iter().collect();

                                let table_remove_diff: Diff = Diff::TableRemove(TableRemoveDiff {
                                    table_name: target_table_create_diff.table_name.clone(),
                                    schema: target_table_create_diff.schema.clone(),
                                    rows_removed: table_rows,
                                });

                                prereq_diffs.push(table_remove_diff);
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table from the source by removing it from the res_table_diff
                                res_table_diff.table_create_diff = None;
                            }
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
                let target_insert_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed table {} in source, but rows were also inserted into the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            }
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
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            }
                        }
                    }
                } // end target_insert_diff
            }

            if res_table_remove_diff_exists {
                // Get the target update diff
                let target_update_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed table {} in source, but rows were also updated in the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            }
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
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            }
                        }
                    }
                } // end target_update_diff
            }

            if res_table_remove_diff_exists {
                // Get the target remove diff
                let target_remove_diff_opt = target_table_diff.iter().find_map(|diff| match diff {
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
                                return Err(format!("Merge Conflict: Removed table {} in source, but rows were also removed from the same table in the target", 
                                                    res_table_remove_diff.table_name,
                                                    ));
                            }
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
                            }
                            MergeConflictResolutionAlgo::UseTarget => {
                                // Remove the table remove from the source by removing it from the res_table_diff.table_remove_diff
                                res_table_diff.table_remove_diff = None;
                                res_table_remove_diff_exists = false;
                            }
                        }
                    }
                } // end target_remove_diff
            }

            if res_table_remove_diff_exists {
                // Get the target table remove diff
                let target_table_remove_diff_opt =
                    target_table_diff.iter().find_map(|diff| match diff {
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
        if table_diff.table_remove_diff.is_some() {
            res_diffs.push(Diff::TableRemove(
                table_diff.table_remove_diff.clone().unwrap(),
            ));
        }
        if table_diff.table_create_diff.is_some() {
            res_diffs.push(Diff::TableCreate(
                table_diff.table_create_diff.clone().unwrap(),
            ));
        }
        if table_diff.remove_diff.rows.len() > 0 {
            res_diffs.push(Diff::Remove(table_diff.remove_diff.clone()));
        }
        if table_diff.insert_diff.rows.len() > 0 {
            res_diffs.push(Diff::Insert(table_diff.insert_diff.clone()));
        }
        if table_diff.update_diff.rows.len() > 0 {
            res_diffs.push(Diff::Update(table_diff.update_diff.clone()));
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
            let mut contains_index_create: bool = false;
            let mut contains_index_remove: bool = false;
            for diff in diffs_of_same_table {
                match diff {
                    Diff::TableCreate(_) => {
                        if contains_create_table {
                            return Err(format!(
                                "Multiple create table diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_create_table = true;
                    }
                    Diff::TableRemove(_) => {
                        if contains_remove_table {
                            return Err(format!(
                                "Multiple remove table diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_remove_table = true;
                    }
                    Diff::Insert(_) => {
                        if contains_insert {
                            return Err(format!(
                                "Multiple insert diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_insert = true;
                    }
                    Diff::Remove(_) => {
                        if contains_remove {
                            return Err(format!(
                                "Multiple remove diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_remove = true;
                    }
                    Diff::Update(_) => {
                        if contains_update {
                            return Err(format!(
                                "Multiple update diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_update = true;
                    }
                    Diff::IndexCreate(_) => {
                        if contains_index_create {
                            return Err(format!(
                                "Multiple index create diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_index_create = true;
                    }
                    Diff::IndexRemove(_) => {
                        if contains_index_remove {
                            return Err(format!(
                                "Multiple index remove diffs for table {}",
                                diff.get_table_name()
                            ));
                        }
                        contains_index_remove = true;
                    }
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
        executor::query::create_table,
        fileio::{
            databaseio::{delete_db_instance, get_db_instance, MAIN_BRANCH_NAME},
            header::Schema,
            tableio::{create_table_in_dir, delete_table_in_dir},
        },
        user::userdata::User,
        util::{
            bench::{create_demo_db, fcreate_db_instance},
            dbtype::*,
            row::Row,
        },
        version_control::{commit::Commit, diff::Diff},
    };

    use super::*;

    #[test]
    #[serial]
    fn test_basic_insert_merge() {
        // Tests inserting rows into the source branch and merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let target_diffs: Vec<Diff> = Vec::new();

        // Insert rows into the source branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(1), Value::String("John".to_string())],
            vec![Value::I32(2), Value::String("Jane".to_string())],
            vec![Value::I32(3), Value::String("Joe".to_string())],
        ];
        let mut src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = src_table1.insert_rows(rows).unwrap();
        src_diffs.push(Diff::Insert(insert_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Insert(insert_diff) = &merge_diffs[0] {
            assert_eq!(insert_diff.rows.len(), 3);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(
                insert_diff.rows[0].row[1],
                Value::String("John".to_string())
            );
            assert_eq!(insert_diff.rows[1].row.len(), 2);
            assert_eq!(insert_diff.rows[1].row[0], Value::I32(2));
            assert_eq!(
                insert_diff.rows[1].row[1],
                Value::String("Jane".to_string())
            );
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
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
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
        let mut src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = src_table1.insert_rows(rows).unwrap();
        src_diffs.push(Diff::Insert(insert_diff));

        // Insert rows into the target branch
        let rows: Vec<Row> = vec![
            vec![Value::I32(4), Value::String("John".to_string())],
            vec![Value::I32(2), Value::String("Jane".to_string())], // Same as source branch
            vec![Value::I32(6), Value::String("Joe".to_string())],
        ];
        let mut target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = target_table1.insert_rows(rows).unwrap();
        target_diffs.push(Diff::Insert(insert_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Insert(insert_diff) = &merge_diffs[0] {
            assert_eq!(insert_diff.rows.len(), 2);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(
                insert_diff.rows[0].row[1],
                Value::String("John".to_string())
            );
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
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            _table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let target_diffs: Vec<Diff> = Vec::new();

        // Create a table in the source branch
        let src_new_table_name: String = "new_table_123".to_string();
        let src_new_table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("score".to_string(), Column::Float),
        ];
        let (_src_new_table, src_table_create_diff) =
            create_table_in_dir(&src_new_table_name, &src_new_table_schema, &src_branch_dir)
                .unwrap();
        src_diffs.push(Diff::TableCreate(src_table_create_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

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

    #[test]
    #[serial]
    fn test_complex_table_create_merge() {
        // Tests creating a table in both the source branch and the target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            _table_name1,
            _table_name2,
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
        let (_src_new_table, src_table_create_diff) =
            create_table_in_dir(&src_new_table_name, &src_new_table_schema, &src_branch_dir)
                .unwrap();
        src_diffs.push(Diff::TableCreate(src_table_create_diff));

        // Create a table in the target branch
        let target_new_table_name: String = "new_table_456".to_string();
        let target_new_table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("score".to_string(), Column::Float),
        ];
        let (_target_new_table, target_table_create_diff) = create_table_in_dir(
            &target_new_table_name,
            &target_new_table_schema,
            &target_branch_dir,
        )
        .unwrap();
        target_diffs.push(Diff::TableCreate(target_table_create_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

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

    #[test]
    #[serial]
    fn test_complex_create_table_merge_same_table() {
        // Tests creating the same table in both the source branch and the target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            _table_name1,
            _table_name2,
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
        let (_src_new_table, src_table_create_diff) =
            create_table_in_dir(&src_new_table_name, &src_new_table_schema, &src_branch_dir)
                .unwrap();
        src_diffs.push(Diff::TableCreate(src_table_create_diff));

        // Create a table in the target branch
        let target_new_table_schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (_target_new_table, target_table_create_diff) = create_table_in_dir(
            &src_new_table_name,
            &target_new_table_schema,
            &target_branch_dir,
        )
        .unwrap();
        target_diffs.push(Diff::TableCreate(target_table_create_diff));

        // Merge the source branch's diffs into the target branch's diffs
        // Assert that it fails because they are merging same table with different schema
        assert_eq!(
            create_merge_diffs(
                &src_diffs,
                &target_diffs,
                &target_branch_dir,
                MergeConflictResolutionAlgo::NoConflicts
            )
            .is_err(),
            true
        );

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_create_table_merge_exact_same_table() {
        // Tests creating a table in both the source branch and the target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            _table_name1,
            _table_name2,
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
        let (_src_new_table, src_table_create_diff) =
            create_table_in_dir(&src_new_table_name, &src_new_table_schema, &src_branch_dir)
                .unwrap();
        src_diffs.push(Diff::TableCreate(src_table_create_diff));

        // Create the exact same table in the target branch
        let (_target_new_table, target_table_create_diff) = create_table_in_dir(
            &src_new_table_name,
            &src_new_table_schema,
            &target_branch_dir,
        )
        .unwrap();
        target_diffs.push(Diff::TableCreate(target_table_create_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 0);

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_basic_update_merge() {
        // Tests updating rows in the source branch and merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let target_diffs: Vec<Diff> = Vec::new();

        // Update rows in the source branch
        let rows: Vec<RowInfo> = vec![RowInfo {
            pagenum: 1,
            rownum: 1,
            row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
        }];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = src_table1.rewrite_rows(rows).unwrap();
        src_diffs.push(Diff::Update(update_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Update(update_diff) = &merge_diffs[0] {
            assert_eq!(update_diff.rows.len(), 1);
            assert_eq!(update_diff.rows[0].row.len(), 2);
            assert_eq!(update_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(
                update_diff.rows[0].row[1],
                Value::String("JohnUpdated".to_string())
            );
        } else {
            panic!("Expected update diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_update_merge() {
        // Tests updating rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Update rows in the source branch
        let rows: Vec<RowInfo> = vec![RowInfo {
            pagenum: 1,
            rownum: 1,
            row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
        }];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = src_table1.rewrite_rows(rows).unwrap();
        src_diffs.push(Diff::Update(update_diff));

        // Update rows in the target branch
        let rows: Vec<RowInfo> = vec![
            RowInfo {
                pagenum: 1,
                rownum: 0,
                row: vec![
                    Value::I32(1),
                    Value::String("FirstUpdatedTarget".to_string()),
                ],
            },
            RowInfo {
                pagenum: 1,
                rownum: 2,
                row: vec![
                    Value::I32(1),
                    Value::String("SecondUpdatedTarget".to_string()),
                ],
            },
        ];
        let target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = target_table1.rewrite_rows(rows).unwrap();
        target_diffs.push(Diff::Update(update_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Update(update_diff) = &merge_diffs[0] {
            assert_eq!(update_diff.rows.len(), 1);
            assert_eq!(update_diff.rows[0].row.len(), 2);
            assert_eq!(update_diff.rows[0].row[0], Value::I32(1));
            assert_eq!(
                update_diff.rows[0].row[1],
                Value::String("JohnUpdated".to_string())
            );
        } else {
            panic!("Expected update diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_update_merge_conflict_with_same_row() {
        // Tests updating rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Update rows in the source branch
        let rows: Vec<RowInfo> = vec![
            RowInfo {
                pagenum: 1,
                rownum: 1,
                row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
            },
            RowInfo {
                pagenum: 1,
                rownum: 2,
                row: vec![Value::I32(33), Value::String("JoeUpdated".to_string())],
            },
        ];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = src_table1.rewrite_rows(rows).unwrap();
        src_diffs.push(Diff::Update(update_diff));

        // Update rows in the target branch
        let rows: Vec<RowInfo> = vec![
            RowInfo {
                pagenum: 1,
                rownum: 0,
                row: vec![
                    Value::I32(1),
                    Value::String("FirstUpdatedTarget".to_string()),
                ],
            },
            RowInfo {
                pagenum: 1,
                rownum: 1,
                row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
            },
        ];
        let target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = target_table1.rewrite_rows(rows).unwrap();
        target_diffs.push(Diff::Update(update_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Update(update_diff) = &merge_diffs[0] {
            assert_eq!(update_diff.rows.len(), 1);
            assert_eq!(update_diff.rows[0].row.len(), 2);
            assert_eq!(update_diff.rows[0].row[0], Value::I32(33));
            assert_eq!(
                update_diff.rows[0].row[1],
                Value::String("JoeUpdated".to_string())
            );
        } else {
            panic!("Expected update diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_update_merge_conflict() {
        // Tests updating rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Update rows in the source branch
        let rows: Vec<RowInfo> = vec![
            RowInfo {
                pagenum: 1,
                rownum: 1,
                row: vec![Value::I32(1), Value::String("JohnUpdatedSrc".to_string())],
            },
            RowInfo {
                pagenum: 1,
                rownum: 2,
                row: vec![Value::I32(33), Value::String("JoeUpdated".to_string())],
            },
        ];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = src_table1.rewrite_rows(rows).unwrap();
        src_diffs.push(Diff::Update(update_diff));

        // Update rows in the target branch
        let rows: Vec<RowInfo> = vec![
            RowInfo {
                pagenum: 1,
                rownum: 0,
                row: vec![
                    Value::I32(1),
                    Value::String("FirstUpdatedTarget".to_string()),
                ],
            },
            RowInfo {
                pagenum: 1,
                rownum: 1,
                row: vec![
                    Value::I32(1),
                    Value::String("JohnUpdatedTarget".to_string()),
                ],
            },
        ];
        let target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let update_diff: UpdateDiff = target_table1.rewrite_rows(rows).unwrap();
        target_diffs.push(Diff::Update(update_diff));

        // Merge the source branch's diffs into the target branch's diffs
        assert_eq!(
            create_merge_diffs(
                &src_diffs,
                &target_diffs,
                &target_branch_dir,
                MergeConflictResolutionAlgo::NoConflicts
            )
            .is_err(),
            true
        );

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_basic_remove_merge() {
        // Tests removing rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let target_diffs: Vec<Diff> = Vec::new();

        // Remove rows in the source branch
        let rows: Vec<RowLocation> = vec![
            RowLocation {
                pagenum: 1,
                rownum: 1,
            },
            RowLocation {
                pagenum: 1,
                rownum: 2,
            },
        ];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let remove_diff: RemoveDiff = src_table1.remove_rows(rows).unwrap();
        src_diffs.push(Diff::Remove(remove_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Remove(remove_diff) = &merge_diffs[0] {
            assert_eq!(remove_diff.rows.len(), 2);
            assert_eq!(remove_diff.rows[0].row.len(), 2);
            assert_eq!(remove_diff.rows[0].row[0], Value::I32(200));
            assert_eq!(
                remove_diff.rows[0].row[1],
                Value::String("Second".to_string())
            );
            assert_eq!(remove_diff.rows[1].row.len(), 2);
            assert_eq!(remove_diff.rows[1].row[0], Value::I32(300));
            assert_eq!(
                remove_diff.rows[1].row[1],
                Value::String("Third".to_string())
            );
        } else {
            panic!("Expected remove diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_remove_merge() {
        // Tests removing rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Remove rows in the source branch
        let rows: Vec<RowLocation> = vec![
            RowLocation {
                pagenum: 1,
                rownum: 1,
            },
            RowLocation {
                pagenum: 1,
                rownum: 2,
            },
        ];
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let remove_diff: RemoveDiff = src_table1.remove_rows(rows).unwrap();
        src_diffs.push(Diff::Remove(remove_diff));

        // Remove rows in the target branch
        let rows: Vec<RowLocation> = vec![
            RowLocation {
                pagenum: 1,
                rownum: 0,
            },
            RowLocation {
                pagenum: 1,
                rownum: 1,
            },
        ];
        let target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let remove_diff: RemoveDiff = target_table1.remove_rows(rows).unwrap();
        target_diffs.push(Diff::Remove(remove_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Remove(remove_diff) = &merge_diffs[0] {
            assert_eq!(remove_diff.rows.len(), 1);
            assert_eq!(remove_diff.rows[0].row.len(), 2);
            assert_eq!(remove_diff.rows[0].row[0], Value::I32(300));
            assert_eq!(
                remove_diff.rows[0].row[1],
                Value::String("Third".to_string())
            );
        } else {
            panic!("Expected remove diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_simple_table_remove_merge() {
        // Tests removing rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            _table_name1,
            table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let target_diffs: Vec<Diff> = Vec::new();

        // Remove a table from the source branch
        let src_table2: Table = Table::new(&src_branch_dir, &table_name2, None).unwrap();
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name2, &src_branch_dir).unwrap();
        src_diffs.push(Diff::TableRemove(table_remove_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name2);
        if let Diff::TableRemove(table_remove_diff) = &merge_diffs[0] {
            assert_eq!(table_remove_diff.table_name, table_name2);
            assert_eq!(table_remove_diff.schema, src_table2.schema);
            // Assert that the table remove diff has the correct rows
            assert_rows_are_correct(
                table_remove_diff
                    .rows_removed
                    .iter()
                    .map(|row| row.row.clone())
                    .collect(),
                vec![
                    vec![
                        Value::I32(100),
                        Value::String("First".to_string()),
                        Value::I32(1),
                    ],
                    vec![
                        Value::I32(200),
                        Value::String("Second".to_string()),
                        Value::I32(2),
                    ],
                    vec![
                        Value::I32(300),
                        Value::String("Third".to_string()),
                        Value::I32(3),
                    ],
                    vec![
                        Value::I32(400),
                        Value::String("Fourth".to_string()),
                        Value::I32(4),
                    ],
                    vec![
                        Value::I32(500),
                        Value::String("Fifth".to_string()),
                        Value::I32(5),
                    ],
                    vec![
                        Value::I32(600),
                        Value::String("Sixth".to_string()),
                        Value::I32(6),
                    ],
                ],
            );
        } else {
            panic!("Expected table remove diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_complex_table_remove_merge() {
        // Tests removing rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Remove a table from the source branch
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name2, &src_branch_dir).unwrap();
        src_diffs.push(Diff::TableRemove(table_remove_diff));

        // Remove another table from the source branch
        let src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name1, &src_branch_dir).unwrap();
        src_diffs.push(Diff::TableRemove(table_remove_diff));

        // Remove a table from the target branch
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name2, &target_branch_dir).unwrap();
        target_diffs.push(Diff::TableRemove(table_remove_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::TableRemove(table_remove_diff) = &merge_diffs[0] {
            assert_eq!(table_remove_diff.table_name, table_name1);
            assert_eq!(table_remove_diff.schema, src_table1.schema);
            // Assert that the table remove diff has the correct rows
            assert_rows_are_correct(
                table_remove_diff
                    .rows_removed
                    .iter()
                    .map(|row| row.row.clone())
                    .collect(),
                vec![
                    vec![Value::I32(100), Value::String("First".to_string())],
                    vec![Value::I32(200), Value::String("Second".to_string())],
                    vec![Value::I32(300), Value::String("Third".to_string())],
                ],
            );
        } else {
            panic!("Expected table remove diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_inserts_updates_and_removes_merge() {
        // Tests inserting, updating, and removing rows in both the source branch and target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Insert a row into the source branch
        let mut src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = src_table1
            .insert_rows(vec![vec![
                Value::I32(400),
                Value::String("NewRowSrc".to_string()),
            ]])
            .unwrap();
        src_diffs.push(Diff::Insert(insert_diff));

        // Update a row in the source branch
        let update_diff: UpdateDiff = src_table1
            .rewrite_rows(vec![RowInfo {
                pagenum: 1,
                rownum: 0,
                row: vec![
                    Value::I32(150),
                    Value::String("FirstUpdatedSrc".to_string()),
                ],
            }])
            .unwrap();
        src_diffs.push(Diff::Update(update_diff));

        // Remove a row from the source branch
        let remove_diff: RemoveDiff = src_table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        src_diffs.push(Diff::Remove(remove_diff));

        // Remove the second table from the source branch
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name2, &src_branch_dir).unwrap();
        src_diffs.push(Diff::TableRemove(table_remove_diff));

        // Insert a row into the target branch
        let mut target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let insert_diff: InsertDiff = target_table1
            .insert_rows(vec![vec![
                Value::I32(400),
                Value::String("NewRowTarget".to_string()),
            ]])
            .unwrap();
        target_diffs.push(Diff::Insert(insert_diff));

        // Update a row in the target branch
        let update_diff: UpdateDiff = target_table1
            .rewrite_rows(vec![RowInfo {
                pagenum: 1,
                rownum: 2,
                row: vec![
                    Value::I32(100),
                    Value::String("ThirdUpdatedSrc".to_string()),
                ],
            }])
            .unwrap();
        target_diffs.push(Diff::Update(update_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 4);

        // Assert insert diff is correct
        let insert_diff: InsertDiff = get_insert_diff(&merge_diffs);
        assert_eq!(insert_diff.table_name, table_name1);
        assert_eq!(insert_diff.schema, src_table1.schema);
        assert_eq!(insert_diff.rows.len(), 1);
        assert_eq!(insert_diff.rows[0].row[0], Value::I32(400));
        assert_eq!(
            insert_diff.rows[0].row[1],
            Value::String("NewRowSrc".to_string())
        );

        // Assert update diff is correct
        let update_diff: UpdateDiff = get_update_diff(&merge_diffs);
        assert_eq!(update_diff.table_name, table_name1);
        assert_eq!(update_diff.schema, src_table1.schema);
        assert_eq!(update_diff.rows.len(), 1);
        assert_eq!(update_diff.rows[0].row[0], Value::I32(150));
        assert_eq!(
            update_diff.rows[0].row[1],
            Value::String("FirstUpdatedSrc".to_string())
        );

        // Assert remove diff is correct
        let remove_diff: RemoveDiff = get_remove_diff(&merge_diffs);
        assert_eq!(remove_diff.table_name, table_name1);
        assert_eq!(remove_diff.schema, src_table1.schema);
        assert_eq!(remove_diff.rows.len(), 1);
        assert_eq!(remove_diff.rows[0].row[0], Value::I32(200));
        assert_eq!(
            remove_diff.rows[0].row[1],
            Value::String("Second".to_string())
        );

        // Assert table remove diff is correct
        let table_remove_diff: TableRemoveDiff = get_table_remove_diff(&merge_diffs);
        assert_eq!(table_remove_diff.table_name, table_name2);

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_many_inserts_merge() {
        // Tests inserting rows into both the source branch and the target branch then merging them into the target branch

        // Create the database
        let (
            _user,
            _src_branch,
            _target_branch,
            src_branch_dir,
            target_branch_dir,
            table_name1,
            _table_name2,
        ) = setup_test_db();

        // Create a vector for the source and target diffs
        let mut src_diffs: Vec<Diff> = Vec::new();
        let mut target_diffs: Vec<Diff> = Vec::new();

        // Insert rows into the source branch
        let src_row: Row = vec![Value::I32(1), Value::String("John".to_string())];
        let mut src_rows: Vec<Row> = Vec::new();
        for _ in 0..500 {
            src_rows.push(src_row.clone());
        }
        let mut src_table1: Table = Table::new(&src_branch_dir, &table_name1, None).unwrap();
        let src_insert_diff: InsertDiff = src_table1.insert_rows(src_rows).unwrap();
        src_diffs.push(Diff::Insert(src_insert_diff));

        // Insert rows into the target branch
        let target_row: Row = vec![Value::I32(2), Value::String("Jane".to_string())];
        let mut target_rows: Vec<Row> = Vec::new();
        for _ in 0..300 {
            target_rows.push(target_row.clone());
        }
        let mut target_table1: Table = Table::new(&target_branch_dir, &table_name1, None).unwrap();
        let target_insert_diff: InsertDiff = target_table1.insert_rows(target_rows).unwrap();
        target_diffs.push(Diff::Insert(target_insert_diff));

        // Merge the source branch's diffs into the target branch's diffs
        let merge_diffs: Vec<Diff> = create_merge_diffs(
            &src_diffs,
            &target_diffs,
            &target_branch_dir,
            MergeConflictResolutionAlgo::NoConflicts,
        )
        .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);
        assert_eq!(merge_diffs[0].get_table_name(), table_name1);
        if let Diff::Insert(insert_diff) = &merge_diffs[0] {
            assert_eq!(insert_diff.rows.len(), 500);
            for i in 0..500 {
                assert_eq!(insert_diff.rows[i].row.len(), 2);
                assert_eq!(insert_diff.rows[i].row[0], Value::I32(1));
                assert_eq!(
                    insert_diff.rows[i].row[1],
                    Value::String("John".to_string())
                );
            }
        } else {
            panic!("Expected insert diff");
        }

        // Clean up the database
        delete_test_db();
    }

    #[test]
    #[serial]
    fn test_db_side_basic_merge() {
        // This will test creating a branch off of main then checking merge conflicts to it
        let db_name: String = "test_creating_a_branch_of_branch_and_switch".to_string();
        let branch_name: String = "new_branch_1".to_string();
        let table_name1: String = "table1".to_string();

        // Create a new database
        fcreate_db_instance(&db_name);

        // Create a new user
        let mut user: User = User::new("test_user".to_string());

        // Create the table on the main branch
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table1_diff) =
            create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();
        user.append_diff(&Diff::TableCreate(table1_diff));

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![
            Value::I32(100),
            Value::String("InitialRow".to_string()),
        ]);
        rows.push(vec![
            Value::I32(200),
            Value::String("SecondRow".to_string()),
        ]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Create a commit on main branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on Main Branch".to_string(),
                &"Create Table;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![Value::I32(1), Value::String("John".to_string())]);
        rows.push(vec![Value::I32(2), Value::String("Jane".to_string())]);
        rows.push(vec![Value::I32(3), Value::String("Joe".to_string())]);
        rows.push(vec![Value::I32(4), Value::String("Bill".to_string())]);
        rows.push(vec![Value::I32(5), Value::String("Bob".to_string())]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Update some of those rows
        let table1_update_diff: UpdateDiff = table1
            .rewrite_rows(vec![
                RowInfo {
                    pagenum: 1,
                    rownum: 2,
                    row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
                },
                RowInfo {
                    pagenum: 1,
                    rownum: 3,
                    row: vec![Value::I32(2), Value::String("JaneUpdated".to_string())],
                },
            ])
            .unwrap();
        user.append_diff(&Diff::Update(table1_update_diff));

        // Remove one of the rows
        let table1_remove_diff: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 3,
            }])
            .unwrap();
        user.append_diff(&Diff::Remove(table1_remove_diff));

        // Create a commit on main branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"Second Commit on Main Branch".to_string(),
                &"Insert, Update, and Remove Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Create a new branch
        get_db_instance()
            .unwrap()
            .create_branch(&branch_name, &None,&mut user)
            .unwrap();

        // Get the table on the new branch
        let new_table_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let mut new_table1: Table = Table::new(&new_table_dir, &table_name1, None).unwrap();

        // Insert rows into the table on the new branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![
            Value::I32(-1),
            Value::String("JohnNewBranch".to_string()),
        ]);
        rows.push(vec![
            Value::I32(-2),
            Value::String("JaneNewBranch".to_string()),
        ]);
        rows.push(vec![
            Value::I32(-3),
            Value::String("JoeNewBranch".to_string()),
        ]);
        let new_table1_insert_diff: InsertDiff = new_table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(new_table1_insert_diff));

        // Update some of those rows
        let new_table1_update_diff: UpdateDiff = new_table1
            .rewrite_rows(vec![
                RowInfo {
                    pagenum: 1,
                    rownum: 7,
                    row: vec![Value::I32(-22), Value::String("JaneNewUpdated".to_string())],
                },
                RowInfo {
                    pagenum: 1,
                    rownum: 8,
                    row: vec![Value::I32(-33), Value::String("JoeNewUpdated".to_string())],
                },
            ])
            .unwrap();
        user.append_diff(&Diff::Update(new_table1_update_diff));

        // Remove one of the rows
        let new_table1_remove_diff: RemoveDiff = new_table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        user.append_diff(&Diff::Remove(new_table1_remove_diff));

        // Create a commit on new branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on New Branch".to_string(),
                &"Insert, Update, and Remove Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Switch to main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the branches
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Assert that the merge commit has the correct number of diffs
        let merge_diffs: Vec<Diff> = merge_commit.diffs;
        assert_eq!(merge_diffs.len(), 2);

        // Assert that the merge commit has the correct diffs
        if let Diff::Remove(remove_diff) = &merge_diffs[0] {
            assert_eq!(remove_diff.table_name, table_name1);
            assert_eq!(remove_diff.schema, table1.schema);
            assert_eq!(remove_diff.rows.len(), 1);
            assert_eq!(remove_diff.rows[0].row.len(), 2);
            assert_eq!(remove_diff.rows[0].pagenum, 1);
            assert_eq!(remove_diff.rows[0].rownum, 1);
            assert_eq!(remove_diff.rows[0].row[0], Value::I32(200));
            assert_eq!(
                remove_diff.rows[0].row[1],
                Value::String("SecondRow".to_string())
            );
        } else {
            panic!("Expected remove diff");
        }
        if let Diff::Insert(insert_diff) = &merge_diffs[1] {
            assert_eq!(insert_diff.table_name, table_name1);
            assert_eq!(insert_diff.schema, table1.schema);
            assert_eq!(insert_diff.rows.len(), 3);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(-1));
            assert_eq!(
                insert_diff.rows[0].row[1],
                Value::String("JohnNewBranch".to_string())
            );
            assert_eq!(insert_diff.rows[1].row.len(), 2);
            assert_eq!(insert_diff.rows[1].row[0], Value::I32(-22));
            assert_eq!(
                insert_diff.rows[1].row[1],
                Value::String("JaneNewUpdated".to_string())
            );
            assert_eq!(insert_diff.rows[2].row.len(), 2);
            assert_eq!(insert_diff.rows[2].row[0], Value::I32(-33));
            assert_eq!(
                insert_diff.rows[2].row[1],
                Value::String("JoeNewUpdated".to_string())
            );
        } else {
            panic!("Expected insert diff");
        }

        // Delete the database
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_db_side_complex_merge() {
        // This will test creating a branch off of main then checking merge conflicts to it
        let db_name: String = "test_creating_a_branch_of_branch_and_switch".to_string();
        let branch_name: String = "new_branch_1".to_string();
        let table_name1: String = "table1".to_string();

        // Create a new database
        fcreate_db_instance(&db_name);

        // Create a new user
        let mut user: User = User::new("test_user".to_string());

        // Create the table on the main branch
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table1_diff) =
            create_table(&table_name1, &schema, get_db_instance().unwrap(), &mut user).unwrap();
        user.append_diff(&Diff::TableCreate(table1_diff));

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![
            Value::I32(100),
            Value::String("InitialRow".to_string()),
        ]);
        rows.push(vec![
            Value::I32(200),
            Value::String("SecondRow".to_string()),
        ]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Create a commit on main branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on Main Branch".to_string(),
                &"Create Table;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Create a new branch
        get_db_instance()
            .unwrap()
            .create_branch(&branch_name, &None,&mut user)
            .unwrap();

        // Switch to back to main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Insert rows into the table on the main branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![Value::I32(1), Value::String("John".to_string())]);
        rows.push(vec![Value::I32(2), Value::String("Jane".to_string())]);
        rows.push(vec![Value::I32(3), Value::String("Joe".to_string())]);
        rows.push(vec![Value::I32(4), Value::String("Bill".to_string())]);
        rows.push(vec![Value::I32(5), Value::String("Bob".to_string())]);
        let table1_insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(table1_insert_diff));

        // Update some of those rows
        let table1_update_diff: UpdateDiff = table1
            .rewrite_rows(vec![
                RowInfo {
                    pagenum: 1,
                    rownum: 2,
                    row: vec![Value::I32(1), Value::String("JohnUpdated".to_string())],
                },
                RowInfo {
                    pagenum: 1,
                    rownum: 3,
                    row: vec![Value::I32(2), Value::String("JaneUpdated".to_string())],
                },
            ])
            .unwrap();
        user.append_diff(&Diff::Update(table1_update_diff));

        // Remove one of the rows
        let table1_remove_diff: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 3,
            }])
            .unwrap();
        user.append_diff(&Diff::Remove(table1_remove_diff));

        // Create a commit on main branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"Second Commit on Main Branch".to_string(),
                &"Insert, Update, and Remove Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Switch to the new branch
        get_db_instance()
            .unwrap()
            .switch_branch(&branch_name, &mut user)
            .unwrap();

        // Get the table on the new branch
        let new_table_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let mut new_table1: Table = Table::new(&new_table_dir, &table_name1, None).unwrap();

        // Insert rows into the table on the new branch
        let mut rows: Vec<Row> = Vec::new();
        rows.push(vec![
            Value::I32(-1),
            Value::String("JohnNewBranch".to_string()),
        ]);
        rows.push(vec![
            Value::I32(-2),
            Value::String("JaneNewBranch".to_string()),
        ]);
        rows.push(vec![
            Value::I32(-3),
            Value::String("JoeNewBranch".to_string()),
        ]);
        let new_table1_insert_diff: InsertDiff = new_table1.insert_rows(rows).unwrap();
        user.append_diff(&Diff::Insert(new_table1_insert_diff));

        // Update some of those rows
        let new_table1_update_diff: UpdateDiff = new_table1
            .rewrite_rows(vec![
                RowInfo {
                    pagenum: 1,
                    rownum: 3,
                    row: vec![Value::I32(-22), Value::String("JaneNewUpdated".to_string())],
                },
                RowInfo {
                    pagenum: 1,
                    rownum: 4,
                    row: vec![Value::I32(-33), Value::String("JoeNewUpdated".to_string())],
                },
            ])
            .unwrap();
        user.append_diff(&Diff::Update(new_table1_update_diff));

        // Remove one of the rows
        let new_table1_remove_diff: RemoveDiff = new_table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        user.append_diff(&Diff::Remove(new_table1_remove_diff));

        // Create a commit on new branch
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on New Branch".to_string(),
                &"Insert, Update, and Remove Rows;".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Switch to main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the branches
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Here's what we have done so far
        // Create DB
        //   Create Table
        //   Insert 2 rows
        // Commit on Main
        // Create new branch
        // Switch back to Main
        //   Insert 5 rows
        //   Update the 2 rows (rows 3 and 4)
        //   Remove 1 row (row 4)
        // Commit on Main
        // Switch to new branch
        //   Insert 3 rows
        //   Update 2 rows (rows 4 and 5)
        //   Remove 1 row (row 2)
        // Commit on new branch

        // Assert that the merge commit has the correct number of diffs
        let merge_diffs: Vec<Diff> = merge_commit.diffs;
        assert_eq!(merge_diffs.len(), 2);

        // Assert that the merge commit has the correct diffs
        if let Diff::Remove(remove_diff) = &merge_diffs[0] {
            assert_eq!(remove_diff.table_name, table_name1);
            assert_eq!(remove_diff.schema, table1.schema);
            assert_eq!(remove_diff.rows.len(), 1);
            assert_eq!(remove_diff.rows[0].row.len(), 2);
            assert_eq!(remove_diff.rows[0].pagenum, 1);
            assert_eq!(remove_diff.rows[0].rownum, 1);
            assert_eq!(remove_diff.rows[0].row[0], Value::I32(200));
            assert_eq!(
                remove_diff.rows[0].row[1],
                Value::String("SecondRow".to_string())
            );
        } else {
            panic!("Expected remove diff");
        }
        if let Diff::Insert(insert_diff) = &merge_diffs[1] {
            assert_eq!(insert_diff.table_name, table_name1);
            assert_eq!(insert_diff.schema, table1.schema);
            assert_eq!(insert_diff.rows.len(), 3);
            assert_eq!(insert_diff.rows[0].row.len(), 2);
            assert_eq!(insert_diff.rows[0].row[0], Value::I32(-1));
            assert_eq!(
                insert_diff.rows[0].row[1],
                Value::String("JohnNewBranch".to_string())
            );
            assert_eq!(insert_diff.rows[1].row.len(), 2);
            assert_eq!(insert_diff.rows[1].row[0], Value::I32(-22));
            assert_eq!(
                insert_diff.rows[1].row[1],
                Value::String("JaneNewUpdated".to_string())
            );
            assert_eq!(insert_diff.rows[2].row.len(), 2);
            assert_eq!(insert_diff.rows[2].row[0], Value::I32(-33));
            assert_eq!(
                insert_diff.rows[2].row[1],
                Value::String("JoeNewUpdated".to_string())
            );
        } else {
            panic!("Expected insert diff");
        }

        // Delete the database
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_table_create_and_insert_merge() {
        // Create the database
        let db_name: String = "test_merge".to_string();
        fcreate_db_instance(&db_name);

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
        let (mut table1, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        main_branch_diffs.clear();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch_name, &None,&mut user)
            .unwrap();

        // Create a second table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
            ("age".to_string(), Column::I32),
        ];
        let (mut table2, table_create_diff2) = create_table(
            &table_name_2,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff2));

        // Insert some rows into the second table
        let rows: Vec<Row> = vec![
            vec![
                Value::I32(100),
                Value::String("First".to_string()),
                Value::I32(1),
            ],
            vec![
                Value::I32(200),
                Value::String("Second".to_string()),
                Value::I32(2),
            ],
            vec![
                Value::I32(300),
                Value::String("Third".to_string()),
                Value::I32(3),
            ],
            vec![
                Value::I32(400),
                Value::String("Fourth".to_string()),
                Value::I32(4),
            ],
            vec![
                Value::I32(500),
                Value::String("Fifth".to_string()),
                Value::I32(5),
            ],
            vec![
                Value::I32(600),
                Value::String("Sixth".to_string()),
                Value::I32(6),
            ],
        ];
        let insert_diff: InsertDiff = table2.insert_rows(rows.clone()).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the new branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"2nd Commit".to_string(),
                &"Created a new Table".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();
        let merge_diffs: Vec<Diff> = merge_commit.diffs;

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 2);
        if let Diff::TableCreate(table_create_diff) = &merge_diffs[0] {
            assert_eq!(table_create_diff.table_name, table_name_2);
            assert_eq!(table_create_diff.schema, schema);
        } else {
            panic!("Expected table create diff");
        }

        if let Diff::Insert(insert_diff) = &merge_diffs[1] {
            assert_eq!(insert_diff.table_name, table_name_2);
            assert_eq!(insert_diff.rows.len(), 6);

            // Assert that the rows are correct
            assert_rows_are_correct(
                insert_diff.rows.iter().map(|x| x.row.clone()).collect(),
                rows,
            );
        } else {
            panic!("Expected insert diff");
        }

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_dual_removes_merge() {
        // Create the database
        let db_name: String = "test_merge".to_string();
        fcreate_db_instance(&db_name);

        // Table names
        let table_name_1: String = "table1".to_string();

        // Branch name
        let branch_name: String = "test_branch1".to_string();

        // Diffs applied to the branches
        let mut main_branch_diffs: Vec<Diff> = Vec::new();
        let mut test_branch1_diffs: Vec<Diff> = Vec::new();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        main_branch_diffs.clear();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch1_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table1_test_branch1: Table =
            Table::new(&test_branch1_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table1_test_branch1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch1_diffs.push(Diff::Remove(remove_diff));

        // Create a commit on the new branch
        user.set_diffs(&test_branch1_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch1".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch1_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        let remove_diff: RemoveDiff = table1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        main_branch_diffs.push(Diff::Remove(remove_diff));

        // Create a commit on the main
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"2nd Commit on main branch".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        main_branch_diffs.clear();

        // Merge the new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();
        let merge_diffs: Vec<Diff> = merge_commit.diffs;

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 0);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_dual_removes_on_different_branches_merge() {
        // Create the database
        let db_name: String = "test_merge".to_string();
        fcreate_db_instance(&db_name);

        // Table names
        let table_name_1: String = "table1".to_string();

        // Branch name
        let branch1_name: String = "test_branch1".to_string();
        let branch2_name: String = "test_branch2".to_string();

        // Diffs applied to the branches
        let mut main_branch_diffs: Vec<Diff> = Vec::new();
        let mut test_branch1_diffs: Vec<Diff> = Vec::new();
        let mut test_branch2_diffs: Vec<Diff> = Vec::new();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        main_branch_diffs.clear();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch1_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch1_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table1_test_branch1: Table =
            Table::new(&test_branch1_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table1_test_branch1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch1_diffs.push(Diff::Remove(remove_diff));

        // Create a commit on the new branch
        user.set_diffs(&test_branch1_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch1".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch1_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch2_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch2_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table2_test_branch1: Table =
            Table::new(&test_branch2_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table2_test_branch1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch2_diffs.push(Diff::Remove(remove_diff));

        // Create a commit on the 2nd new branch
        user.set_diffs(&test_branch2_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch2".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch2_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the first new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch1_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();
        let merge_diffs: Vec<Diff> = merge_commit.diffs;

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);

        // Assert that the merge diff is a remove diff
        match &merge_diffs[0] {
            Diff::Remove(remove_diff) => {
                // Assert that the remove diff is correct
                assert_eq!(remove_diff.table_name, table_name_1);
                assert_eq!(remove_diff.rows.len(), 1);
                assert_eq!(remove_diff.rows[0].pagenum, 1);
                assert_eq!(remove_diff.rows[0].rownum, 1);
            }
            _ => panic!("Expected a remove diff"),
        }

        // Merge the second new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch2_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit.diffs.len(), 0);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_remove_insert_case_merge() {
        // Create the database
        let db_name: String = "test_merge".to_string();
        fcreate_db_instance(&db_name);

        // Table names
        let table_name_1: String = "table1".to_string();

        // Branch name
        let branch1_name: String = "test_branch1".to_string();
        let branch2_name: String = "test_branch2".to_string();

        // Diffs applied to the branches
        let mut main_branch_diffs: Vec<Diff> = Vec::new();
        let mut test_branch1_diffs: Vec<Diff> = Vec::new();
        let mut test_branch2_diffs: Vec<Diff> = Vec::new();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        main_branch_diffs.clear();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch1_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch1_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table1_test_branch1: Table =
            Table::new(&test_branch1_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table1_test_branch1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch1_diffs.push(Diff::Remove(remove_diff));

        // Create a commit on the new branch
        user.set_diffs(&test_branch1_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch1".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch1_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch2_name, &None,&mut user)
            .unwrap();

        // Update the 2nd row from the first table
        // Get the table on the new branch
        let test_branch2_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table2_test_branch1: Table =
            Table::new(&test_branch2_dir, &table_name_1, None).unwrap();
        let update_diff: UpdateDiff = table2_test_branch1
            .rewrite_rows(vec![RowInfo {
                pagenum: 1,
                rownum: 1,
                row: vec![Value::I32(200), Value::String("SecondUpdated".to_string())],
            }])
            .unwrap();
        test_branch2_diffs.push(Diff::Update(update_diff));

        // Create a commit on the 2nd new branch
        user.set_diffs(&test_branch2_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch2".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch2_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the first new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch1_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();
        let merge_diffs: Vec<Diff> = merge_commit.diffs;

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);

        // Assert that the merge diff is a remove diff
        match &merge_diffs[0] {
            Diff::Remove(remove_diff) => {
                // Assert that the remove diff is correct
                assert_eq!(remove_diff.table_name, table_name_1);
                assert_eq!(remove_diff.rows.len(), 1);
                assert_eq!(remove_diff.rows[0].pagenum, 1);
                assert_eq!(remove_diff.rows[0].rownum, 1);
            }
            _ => panic!("Expected a remove diff"),
        }

        // Try merging the second new branch into the main branch
        assert_eq!(
            get_db_instance()
                .unwrap()
                .merge_branches(
                    &branch2_name,
                    &mut user,
                    &"Merged Branches".to_string(),
                    true,
                    MergeConflictResolutionAlgo::NoConflicts,
                    false,
                )
                .is_err(),
            true
        );

        // Merge the second new branch using the source branch's changes
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch2_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::UseSource,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit.diffs.len(), 1);

        // Assert that the merge diff is an insert diff
        match &merge_commit.diffs[0] {
            Diff::Insert(insert_diff) => {
                // Assert that the insert diff is correct
                assert_eq!(insert_diff.table_name, table_name_1);
                assert_eq!(insert_diff.rows.len(), 1);
                assert_eq!(insert_diff.rows[0].pagenum, 1);
                assert_eq!(insert_diff.rows[0].rownum, 1);
                assert_eq!(insert_diff.rows[0].row.len(), 2);
                assert_eq!(insert_diff.rows[0].row[0], Value::I32(200));
                assert_eq!(
                    insert_diff.rows[0].row[1],
                    Value::String("SecondUpdated".to_string())
                );
            }
            _ => panic!("Expected an insert diff"),
        }

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_dual_remove_tables_on_different_branches_merge() {
        // Create the database
        let db_name: String = "test_merge".to_string();
        fcreate_db_instance(&db_name);

        // Table names
        let table_name_1: String = "table1".to_string();

        // Branch name
        let branch1_name: String = "test_branch1".to_string();
        let branch2_name: String = "test_branch2".to_string();

        // Diffs applied to the branches
        let mut main_branch_diffs: Vec<Diff> = Vec::new();
        let mut test_branch1_diffs: Vec<Diff> = Vec::new();
        let mut test_branch2_diffs: Vec<Diff> = Vec::new();

        // Create a user on the main branch
        let mut user: User = User::new("test_user".to_string());

        // Create a new table in the database
        let schema: Schema = vec![
            ("id".to_string(), Column::I32),
            ("name".to_string(), Column::String(50)),
        ];
        let (mut table1, table_create_diff1) = create_table(
            &table_name_1,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff1));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows.clone()).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        main_branch_diffs.clear();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch1_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch1_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let table1_test_branch1: Table =
            Table::new(&test_branch1_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table1_test_branch1
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch1_diffs.push(Diff::Remove(remove_diff));

        // Remove the first table
        let table_remove_diff: TableRemoveDiff =
            delete_table_in_dir(&table_name_1, &test_branch1_dir).unwrap();
        test_branch1_diffs.push(Diff::TableRemove(table_remove_diff));

        // Create a commit on the new branch
        user.set_diffs(&test_branch1_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch1".to_string(),
                &"Removed row 2, and removed table1".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch1_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch2_name, &None,&mut user)
            .unwrap();

        // Remove the 2nd row from the first table
        // Get the table on the new branch
        let test_branch2_dir: String = get_db_instance()
            .unwrap()
            .get_current_working_branch_path(&user);
        let mut table1_test_branch2: Table =
            Table::new(&test_branch2_dir, &table_name_1, None).unwrap();
        let remove_diff: RemoveDiff = table1_test_branch2
            .remove_rows(vec![RowLocation {
                pagenum: 1,
                rownum: 1,
            }])
            .unwrap();
        test_branch2_diffs.push(Diff::Remove(remove_diff));

        // Insert a new row into the first table
        let rows2: Vec<Row> = vec![vec![Value::I32(400), Value::String("Fourth".to_string())]];
        let insert_diff: InsertDiff = table1_test_branch2.insert_rows(rows2).unwrap();
        test_branch2_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the 2nd new branch
        user.set_diffs(&test_branch2_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit on test_branch2".to_string(),
                &"Removed row 2".to_string(),
                &mut user,
                None,
            )
            .unwrap();
        test_branch2_diffs.clear();

        // Switch to the main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge the first new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch1_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();
        let merge_diffs: Vec<Diff> = merge_commit.diffs;

        // Assert that the merge diffs are correct
        assert_eq!(merge_diffs.len(), 1);

        // Assert that the merge diff is a TableRemove
        let table_remove_diff: &TableRemoveDiff = match &merge_diffs[0] {
            Diff::TableRemove(table_remove_diff) => table_remove_diff,
            _ => panic!("Expected a TableRemoveDiff"),
        };
        assert_eq!(table_remove_diff.table_name, table_name_1);
        assert_rows_are_correct(
            table_remove_diff
                .rows_removed
                .iter()
                .map(|rowinfo| rowinfo.row.clone())
                .collect(),
            vec![
                vec![Value::I32(100), Value::String("First".to_string())],
                vec![Value::I32(300), Value::String("Third".to_string())],
                vec![Value::I32(200), Value::String("Second".to_string())],
            ],
        );

        // Merge the second new branch into the main branch
        let merge_commit: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &branch2_name,
                &mut user,
                &"Merged Branches".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit.diffs.len(), 0);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_bench_db_merge() {
        // Create a demo database
        let mut user: User = create_demo_db("test_bench_db_merge");

        // Switch to main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge test_branch2 into main
        let merge_commit1: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &"test_branch2".to_string(),
                &mut user,
                &"Merged Branches test_branch2 & main".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit1.diffs.len(), 1);

        // Assert that the merge diff is a TableRemove
        let table_remove_diff: &TableRemoveDiff = match &merge_commit1.diffs[0] {
            Diff::TableRemove(table_remove_diff) => table_remove_diff,
            _ => panic!("Expected a TableRemoveDiff"),
        };
        assert_eq!(table_remove_diff.table_name, "locations");
        assert_rows_are_correct_no_order(
            table_remove_diff
                .rows_removed
                .iter()
                .map(|rowinfo| rowinfo.row.clone())
                .collect(),
            vec![
                vec![
                    Value::I32(1),
                    Value::String("Home".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(2),
                    Value::String("Work".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(21),
                    Value::String("Gym".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(4),
                    Value::String("Gymnasium".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(22),
                    Value::String("Garden".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(6),
                    Value::String("Restaurant".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(23),
                    Value::String("Gallery".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(8),
                    Value::String("Garden".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(9),
                    Value::String("Library".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(10),
                    Value::String("Gallery".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(24),
                    Value::String("Gymnasium".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(25),
                    Value::String("University".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(27),
                    Value::String("Dubai".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(28),
                    Value::String("London".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(29),
                    Value::String("New York".to_string()),
                    Value::Bool(true),
                ],
                vec![
                    Value::I32(30),
                    Value::String("Paris".to_string()),
                    Value::Bool(false),
                ],
                vec![
                    Value::I32(31),
                    Value::String("Tokyo".to_string()),
                    Value::Bool(true),
                ],
            ],
        );

        // Try merging test_branch1 into main
        assert_eq!(
            get_db_instance()
                .unwrap()
                .merge_branches(
                    &"test_branch1".to_string(),
                    &mut user,
                    &"Merged Branches test_branch1 & main".to_string(),
                    true,
                    MergeConflictResolutionAlgo::NoConflicts,
                    false,
                )
                .is_err(),
            true
        );

        // Merge test_branch1 into main using the target branch's changes
        let merge_commit2: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &"test_branch1".to_string(),
                &mut user,
                &"Merged Branches test_branch1 & main".to_string(),
                true,
                MergeConflictResolutionAlgo::UseTarget,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit2.diffs.len(), 1);

        // Assert that the merge diff is an Insert
        let insert_diff: &InsertDiff = match &merge_commit2.diffs[0] {
            Diff::Insert(insert_diff) => insert_diff,
            _ => panic!("Expected an InsertDiff"),
        };
        assert_eq!(insert_diff.table_name, "personal_info");

        // Assert that the rows are correct
        assert_rows_are_correct_no_order(
            insert_diff
                .rows
                .iter()
                .map(|rowinfo| rowinfo.row.clone())
                .collect(),
            vec![
                vec![
                    Value::I32(19),
                    Value::String("Dwayne".to_string()),
                    Value::String("Johnson".to_string()),
                    Value::I64(30),
                    Value::Float(5.8),
                    Value::Timestamp(parse_time(&"2020-01-08 00:00:11".to_string()).unwrap()),
                ],
                vec![
                    Value::I32(20),
                    Value::String("Chris".to_string()),
                    Value::String("Hemsworth".to_string()),
                    Value::I64(28),
                    Value::Null(Column::Float),
                    Value::Timestamp(parse_time(&"2020-01-09 12:00:23".to_string()).unwrap()),
                ],
                vec![
                    Value::I32(21),
                    Value::String("Chris".to_string()),
                    Value::String("Evans".to_string()),
                    Value::I64(35),
                    Value::Float(5.9),
                    Value::Timestamp(parse_time(&"2020-01-20 00:00:11".to_string()).unwrap()),
                ],
                vec![
                    Value::I32(15),
                    Value::String("Wanda".to_string()),
                    Value::String("Vision".to_string()),
                    Value::I64(28),
                    Value::Float(5.6),
                    Value::Timestamp(parse_time(&"2020-01-01 12:00:23".to_string()).unwrap()),
                ],
                vec![
                    Value::I32(17),
                    Value::String("Scottish".to_string()),
                    Value::String("Language".to_string()),
                    Value::I64(35),
                    Value::Float(5.9),
                    Value::Timestamp(parse_time(&"2020-01-06 00:00:11".to_string()).unwrap()),
                ],
            ],
        );

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    #[test]
    #[serial]
    fn test_bench_db_2_merge() {
        // Create a demo database
        let mut user: User = create_demo_db("test_bench_db_merge");

        // Switch to main branch
        get_db_instance()
            .unwrap()
            .switch_branch(&MAIN_BRANCH_NAME.to_string(), &mut user)
            .unwrap();

        // Merge test_branch1 into main
        let merge_commit1: Commit = get_db_instance()
            .unwrap()
            .merge_branches(
                &"test_branch1".to_string(),
                &mut user,
                &"Merged Branches test_branch1 & main".to_string(),
                true,
                MergeConflictResolutionAlgo::NoConflicts,
                false,
            )
            .unwrap();

        // Assert that the merge diffs are correct
        assert_eq!(merge_commit1.diffs.len(), 4);

        // Delete the db instance
        delete_db_instance().unwrap();
    }

    /// Asserts that the given rows are correct, but doesn't matter what order
    fn assert_rows_are_correct_no_order(rows: Vec<Row>, mut expected_rows: Vec<Row>) {
        // Assert we have the correct number of rows
        assert_eq!(rows.len(), expected_rows.len());
        for i in 0..rows.len() {
            // Check which row from expected_rows matches the current row
            let mut is_row_present: i32 = -1;

            for j in 0..expected_rows.len() {
                // Check to see if any row is correct
                if is_row_is_correct(&rows[i], expected_rows[j].clone()) {
                    is_row_present = j as i32;
                    break;
                }
            }

            assert!(is_row_present != -1, "Row not found: {:?}", rows[i]);

            // Remove the row from expected_rows
            expected_rows.remove(is_row_present as usize);
        }
    }

    /// Asserts that the given rows are correct.
    fn assert_rows_are_correct(rows: Vec<Row>, expected_rows: Vec<Row>) {
        // Assert we have the correct number of rows
        assert_eq!(rows.len(), expected_rows.len());
        for i in 0..rows.len() {
            // Assert each row is correct
            assert_row_is_correct(&rows[i], expected_rows[i].clone());
        }
    }

    /// Asserts that a single row is correct by matching it to the expected row.
    fn assert_row_is_correct(row: &Row, expected_row: Row) {
        // Assert that the row has the correct number of values
        assert_eq!(row.len(), expected_row.len());
        for i in 0..row.len() {
            // Assert the values are correct
            assert_eq!(row[i], expected_row[i]);
        }
    }

    /// Checks that a single row is correct by matching it to the expected row.
    fn is_row_is_correct(row: &Row, expected_row: Row) -> bool {
        // Assert that the row has the correct number of values
        if row.len() == expected_row.len() {
            for i in 0..row.len() {
                // Check the values are correct
                if row[i] != expected_row[i] {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    /// Gets the insert Diff from within the vector of Diffs
    fn get_insert_diff(diffs: &Vec<Diff>) -> InsertDiff {
        for diff in diffs {
            if let Diff::Insert(insert_diff) = diff {
                return insert_diff.clone();
            }
        }
        panic!("Could not find insert diff");
    }

    /// Gets the update Diff from within the vector of Diffs
    fn get_update_diff(diffs: &Vec<Diff>) -> UpdateDiff {
        for diff in diffs {
            if let Diff::Update(update_diff) = diff {
                return update_diff.clone();
            }
        }
        panic!("Could not find update diff");
    }

    /// Gets the remove Diff from within the vector of Diffs
    fn get_remove_diff(diffs: &Vec<Diff>) -> RemoveDiff {
        for diff in diffs {
            if let Diff::Remove(remove_diff) = diff {
                return remove_diff.clone();
            }
        }
        panic!("Could not find remove diff");
    }

    /// Gets the create table Diff from within the vector of Diffs
    fn get_create_table_diff(diffs: &Vec<Diff>) -> TableCreateDiff {
        for diff in diffs {
            if let Diff::TableCreate(create_table_diff) = diff {
                return create_table_diff.clone();
            }
        }
        panic!("Could not find create table diff");
    }

    /// Gets the table remove Diff from within the vector of Diffs
    fn get_table_remove_diff(diffs: &Vec<Diff>) -> TableRemoveDiff {
        for diff in diffs {
            if let Diff::TableRemove(table_remove_diff) = diff {
                return table_remove_diff.clone();
            }
        }
        panic!("Could not find table remove diff");
    }

    /// Sets up the test database and creates 2 tables on the main branch for first commit,
    /// and inserts some rows into each of them.
    /// Also creates a branch off of the first commit on the main branch.
    /// Returns the a tuple:
    ///   - The user
    ///   - The source branch
    ///   - The target branch
    ///   - The source branch directory
    ///   - The target branch directory
    ///   - The name of the first table
    ///   - The name of the second table
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
        fcreate_db_instance(&db_name);

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
        let (mut table1, table_create_diff1) = create_table(
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
        let (mut table2, table_create_diff2) = create_table(
            &table_name_2,
            &schema,
            get_db_instance().unwrap(),
            &mut user,
        )
        .unwrap();
        main_branch_diffs.push(Diff::TableCreate(table_create_diff2));

        // Insert some rows into the first table
        let rows: Vec<Row> = vec![
            vec![Value::I32(100), Value::String("First".to_string())],
            vec![Value::I32(200), Value::String("Second".to_string())],
            vec![Value::I32(300), Value::String("Third".to_string())],
        ];
        let insert_diff: InsertDiff = table1.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Insert some rows into the second table
        let rows: Vec<Row> = vec![
            vec![
                Value::I32(100),
                Value::String("First".to_string()),
                Value::I32(1),
            ],
            vec![
                Value::I32(200),
                Value::String("Second".to_string()),
                Value::I32(2),
            ],
            vec![
                Value::I32(300),
                Value::String("Third".to_string()),
                Value::I32(3),
            ],
            vec![
                Value::I32(400),
                Value::String("Fourth".to_string()),
                Value::I32(4),
            ],
            vec![
                Value::I32(500),
                Value::String("Fifth".to_string()),
                Value::I32(5),
            ],
            vec![
                Value::I32(600),
                Value::String("Sixth".to_string()),
                Value::I32(6),
            ],
        ];
        let insert_diff: InsertDiff = table2.insert_rows(rows).unwrap();
        main_branch_diffs.push(Diff::Insert(insert_diff));

        // Create a commit on the main branch
        user.set_diffs(&main_branch_diffs);
        get_db_instance()
            .unwrap()
            .create_commit_and_node(
                &"First Commit".to_string(),
                &"Create 2 Tables".to_string(),
                &mut user,
                None,
            )
            .unwrap();

        // Create a branch off of the first commit
        get_db_instance()
            .unwrap()
            .create_branch(&branch_name, &None,&mut user)
            .unwrap();

        // Get the two branch directories
        let new_branch_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&branch_name);
        let main_branch_dir: String = get_db_instance()
            .unwrap()
            .get_branch_path_from_name(&MAIN_BRANCH_NAME.to_string());

        user.set_diffs(&Vec::new());
        (
            user,
            branch_name,
            MAIN_BRANCH_NAME.to_string(),
            new_branch_dir,
            main_branch_dir,
            table_name_1,
            table_name_2,
        )
    }

    /// Cleans up the test database
    fn delete_test_db() {
        delete_db_instance().unwrap();
    }
}
