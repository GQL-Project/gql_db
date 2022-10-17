use super::diff::*;
use std::collections::HashMap;

pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
}

/// Merges a single diff to merge into the list of diffs to merge into using a merge conflict algorithm
/// Returns a new list of diffs that would be the result of applying diff_to_merge into diffs_to_merge_into 
pub fn merge_diff_into_list(
    diff_to_merge: &Diff,
    diffs_to_merge_into: &Vec<Diff>,
    conflict_res_algo: MergeConflictResolutionAlgo
) -> Result<Vec<Diff>, String> {
    // Get all the diffs that affect the same table as diff_to_merge
    let diffs_of_same_table: Vec<Diff> = diffs_to_merge_into
        .iter()
        .filter(|diff| diff.get_table_name() == diff_to_merge.get_table_name())
        .cloned()
        .collect();

    // We assume diffs_of_same_table only contains one diff of each type for that table
    let contains_create_table: bool = false;
    let contains_remove_table: bool = false;
    let contains_insert: bool = false;
    let contains_remove: bool = false;
    let contains_update: bool = false;
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

    // 

    Ok(())
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
            // Add all the source diffs to the result
            result.extend(source_diffs.clone());
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

    Ok(result)
}