use super::diff::*;


pub enum MergeConflictResolutionAlgo {
    NoConflicts, // Fails if there are conflicts. This is a 'clean' merge
    UseTarget,   // Uses the target's version of any conflicting cases
    UseSource,   // Uses the source's version of any conflicting cases
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
    let mut result: Vec<Diff> = Vec::new();

    match conflict_res_algo {
        MergeConflictResolutionAlgo::NoConflicts => {
            // Check if there any conflicts between the two diffs
            // Add the source diffs to the result
            for src_diff in source_diffs {
                for target_diff in target_diffs {
                    if src_diff.is_merge_conflict(target_diff) {
                        return Err("Merge Conflict Exists".to_string());
                    }
                }
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