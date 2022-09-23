# Version Control

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
- GQL Command: 512 bytes (String)
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

## Branches Storage

Branches are stored in a file separate from the commits. The branches will be stored using a page/offset system 
