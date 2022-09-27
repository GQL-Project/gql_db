# Version Control

## Terminology

* **Source Branch**: When merging, this is the branch that is being merged into the target branch.
* **Target Branch**: When merging, this is the branch that will receive the changes from the source branch.
* **Source Commits**: When merging, these are the commits that are between the common ancestor and the source branch's HEAD.
* **Target Commits**: When merging, these are the commits that are between the common ancestor and the target branch's HEAD.

## Overview of Entire Structure

This is a representation of how we would visually think about it, and how it is represented across the 4 version control files for the database.

<kbd>![BranchesStructure](https://user-images.githubusercontent.com/54650222/192557378-ef1f7a0d-c717-43e4-a927-ad862ebc255c.png)</kbd>

## Commit Storage

Commits can have variable length, and hence encoding commits changes cannot be done (completely) using pages. 

The pages for Diffs are stored in a similar way to the pages for commit headers, with the file being divided into pages of 4096 bytes. Except here, a commit can be split across multiple pages, and so we need to store the page number and the offset of the commit in the page. This is done by storing the page number and offset in the `commitheaders.gql` file, and then storing the actual commit data in the `deltas.gql` file.

First, we have a file called `commitheaders.gql`, which helps us lookup which page a commit is in `deltas.gql`. This is a fairly simple file, with each row being a commit hash, and the page number it is in.
- Commit Hash: 32 bytes (String)
- Page Number: 4 bytes (I32)
- Offset in Page: 4 bytes (I32)

This is implemented using the Schema and `fileio` functions.

Then, we have a file called `deltas.gql`, which stores the actual commit data. This includes the following fields:
- Commit Hash: 32 bytes (String)
- GQL Commands: 512 bytes (String[])
- Message: 64 bytes (String)
- Diff Size: 4 bytes (I32)
- Diff: Variable length (String)

The `Diff` refers to the changes that are made in this commit, which consists of these three "types" of fields:
 - `INSERT` - This consists of the rows inserted into the table. This operation has the following subfields:
   - `table_name`: 64 bytes -  The name of the table that the rows are inserted into
   - `row_size`: 4 bytes (I32) - The size of each row
   - `num_rows`: 4 bytes (I32) - The number of rows inserted
   - `rows:` - The rows that are inserted into the table, as well as the page number and row offset of the row. This is stored as a list of the following fields:
     - `page_number`: 4 bytes (I32) - The page number of the row
     - `row_num`: 4 bytes (I32) - The offset of the row in the page
     - `row`: `row_size` bytes (Byte[]) - The actual row data
- `UPDATE` - This consists of the rows updated in the table. This operation has the following subfields:
   - `table_name`: 64 bytes -  The name of the table that the rows are updated into
   - `row_size`: 4 bytes (I32) - The size of each row
   - `num_rows`: 4 bytes (I32) - The number of rows updated
   - `rows:` - The new values for the rows updated in the table, as well as the page number and row offset of the row. This is stored as a list of the following fields:
     - `page_number`: 4 bytes (I32) - The page number of the row
     - `row_num`: 4 bytes (I32) - The offset of the row in the page
     - `row`: `row_size` bytes (Byte[]) - The actual row data
- `REMOVE` - This consists of the rows removed in the table. This operation has the following subfields:
   - `table_name`: 64 bytes -  The name of the table that the rows are removed into
   - `num_rows`: 4 bytes (I32) - The number of rows removed
   - `rows:` - The new values for the rows removed in the table, as well as the page number and row offset of the row. This is stored as a list of the following fields:
     - `page_number`: 4 bytes (I32) - The page number of the row
     - `row_num`: 4 bytes (I32) - The offset of the row in the page
     
The page number and row number help us find the row in the table to update, and additionally allow us to map the changes when needed.

## Branch Storage

Branches are stored across 2 files: `branches.gql` and `branch_heads.gql`. The `branches.gql` file contains the individual branch nodes that make up the "tree" of the version control. They are linked from the branch HEADs with only prev pointers pointing all the way back to the origin. The branches will be stored using the [fileio](https://github.com/GQL-Project/gql_db/tree/main/src/fileio#readme) system. That allows the `branch_heads.gql` file to contain the HEAD pointer for each branch by storing the page number and offset of the branch node in the `branches.gql` file.

Each branch node contains the following fields:
- Hash: 32 bytes (String)
- Prev Pointer [pagenum, offset]: 8 bytes (Tuple containing 2 integers)
- Branch Name: 60 bytes (String)

### Accessing Branch Heads

Branch heads are stored separately from the branch nodes. The branch heads are stored in a file called `branch_heads.gql` and are stored in a 4096 byte header as rows of 64 bytes each. Each branch head has the following fields:
- Branch Name: 60 bytes (String)
- Branch Head [pagenum, offset]: 8 bytes (Tuple containing 2 integers)

## Merge Strategy

There are two general strategies for merging branches. The first is the "fast-forward" strategy, which is used when only the source branch has new commits. The second is the "3-way" strategy, which is used when there are changes on both branches.

### Finding a Common Ancestor

Finding a common ancestor is a 3-step process.

1. Start with the source branch's HEAD and go backwards until you reach the origin. While going back, keep a list of tuples containing (<branch_name>, <commit_hash>) of every commit where you encounter a new branch. 
2. Begin going backward from the target branch's HEAD until you reach a branch name that is in the list you accumulated in step 1. Store both those commits.
3. Whichever commit has the older timestamp is your common ancestor.

### Merging the Branches

Now that you have the common ancestor, you can merge the branches. The merge strategy is as follows:

1. Start with the source commits. Iterate through each commit and squash them into a single commit. Each row modified will be stored in a hash that maps the operation to the rows modified. This way as you iterate through the commits, you can keep track of the changes made to each row (i.e. if a row is modified twice, the last operation will be the one that is stored in the hash).
    * For example, if you have a commit that inserts 3 rows, and another that deletes 2 rows, the hash will look like this: 
    
    ```
    {
        "insert": [(<p_num>, <r_num>), (<p_num>, <r_num>), (<p_num>, <r_num>)], 
        "delete": [(<p_num>, <r_num>), (<p_num>, <r_num>)]
    }
    ```
2. Repeat step 1 with the target commits.
3. **NOTE** Use the empty bits in the first byte of the table row to indicate rows that are ready to be deleted/inserted/updated. This way, you can keep track of which rows will cause a merge conflict or not. 

## Invariants

1. All commits in a branch have increasing timestamps.
2. The first commit that branches off of a commit has the name of the new branch.
3. All branch merges are between a source and target branch. The commits between the common ancestor and the HEAD of the source branch are squashed when merging into the target branch.
4. All branch heads are always stored in the `branch_heads.gql` file.
