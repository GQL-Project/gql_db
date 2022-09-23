# Version Control

## Terminology

* **Source Branch**: When merging, this is the branch that is being merged into the target branch.
* **Target Branch**: When merging, this is the branch that will receive the changes from the source branch.
* **Source Commits**: When merging, these are the commits that are between the common ancestor and the source branch's HEAD.
* **Target Commits**: When merging, these are the commits that are between the common ancestor and the target branch's HEAD.

## Commit Storage


## Branches Storage

Branches are stored in a file separate from the commits. The branches will be stored using a page/offset system similar to the [fileio](https://github.com/GQL-Project/gql_db/tree/main/src/fileio#readme) system. 

Each branch node is exactly 104 bytes and contains the following information:
1. Hash (String of 32 bytes)
2. Prev Pointer [pagenum, offset] (Tuple containing 2 integers, total of 8 bytes)
3. Branch Name (String of 60 bytes)

### Accessing Branch Heads

Branch heads are stored separately from the branch nodes. The branch heads are stored in a file called `branch_heads.gql` and are stored in a 4096 byte header as rows of 64 bytes each. They are in the following format:
1. Branch Name (String of 60 bytes)
2. Branch Head [pagenum, offset] (Tuple containing 2 shorts, total of 4 bytes)

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
        "insert": [(<p_num>, <r_num>), <p_num>, <r_num>), <p_num>, <r_num>)], 
        "delete": [<p_num>, <r_num>), <p_num>, <r_num>)]
    }
    ```

## Invariants

1. All commits in a branch have increasing timestamps.
2. The first commit that branches off of a commit has the name of the new branch.
3. All branch merges are between a source and target branch. The commits between the common ancestor and the HEAD of the source branch are squashed when merging into the target branch.
4. All branch heads are always stored in the `branches.gql` file.
