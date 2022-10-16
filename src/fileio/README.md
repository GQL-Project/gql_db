# File Management in GQL

## File format for GQL
- Each database is stored in a folder, with the name of the database.
- Each table is stored in a file, with the name of the table.
    - Pages refer to sections of the file, with a fixed size, which by default is 1024 bytes.
    - The first page of the file is the header page, which contains the table's schema, and how many pages are in the file.


## Workflow for File IO
1. We track the referenced database for each user.
2. When a query is made on a table, we open the corresponding file in the known database folder
    - Here, we can check the schema and ensure the columns and their types match.

## Constraints:
- All types are of fixed length. 
- The total size of a row in the schema cannot be more than 4096 bytes.
- Strings are not allowed to have null characters.
- Columns can only have names of 50 characters or less.
- Schemas are restricted to 60 columns for each table.

### Header Page Format:
- We first have a `uint32` counting how many pages are present in the file. Everytime pages are added, we update this value.
    - To reduce the times needed for this to occur, we double the number of pages in the file after each IO.
- We then have a `uint8` counting the number of elements in the schema, telling us how many records to scan.
- Each schema shape is represented as a `uint16` for the type, followed by a 32 character name:
    - If the first bit is 1, the type is nullable
    - If the second bit is 1, we have a string of size `n ^ (1 << 14)` bytes (which means the maximum size of a string is 2^14 - 1 bytes)
    - If the second bit is 0, for the given values of `n`, we have:
        - 0: Int32
        - 1: Int64
        - 2: Float32
        - 3: Double64
        - 4: Timestamp (32 bits)
        - 5: Boolean

### Page Format:
- The page format is fairly simple: it contains all of the rows sequentially, with each row having an additional byte in the beginning.
- If a row is removed, the byte is set to 0, allowing for us to use this location for insertion later on, and skip it while performing scans.
- Strings of length `n` will have space for all `n` characters, even if it is not initally using the entire space.

## Null Values:
- *Only* null values have an additional byte in the beginning of each cell value, which is set to 1 if the value is null, and 0 otherwise.
- This is done to ensure that we can have null values for all types, and not just strings.
- Additionally, this way we avoid the need to have a null value for each type, which would be a waste of space.

## Concurrency Considerations
- File reads using positioned-io can be done concurrently, as long as no file writes are done at the same time.
- In other words, we can have multiple threads reading from the same file, but as one thread writes, all other threads must wait.

### Future Implementation Ideas:
- Creating a free list for pages
- Representing B-Trees in the same file
- Traversing different "lists" in the header of page.
- Creating a free list for the blocks within a page, rather than doing a manual scan.
- Variable Length Types
- Buffer Management
- Statistics and Transaction Logging

## Resources
We found the following resources particularly helpful in our implementation of a file system:
- [CS 44800 Course Slides](https://www.cs.purdue.edu/homes/clifton/cs44800/)
- [Database System Concepts](https://db-book.com/)
- [SQLite's File Format](https://www.sqlite.org/fileformat.html).