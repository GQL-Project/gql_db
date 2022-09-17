# File Management in GQL

## File format for GQL
- Each database is stored in a folder, with the name of the database.
- Each table is stored in a file, with the name of the table.
    - Pages refer to sections of the file, with a fixed size, which by default is 1024 bytes.
    - The first page of the file is the header page, which contains the table's schema, and how many pages are in the file.

## Constraints:
- All types are of fixed length. 
- The total size of a row in the schema cannot be more than 4096 bytes.
- Columns can only have names of 60 characters or less.
- Schemas are restricted to 60 columns.

### Header Page Format:
- We first have a `uint16` counting how many pages are present in the file. Everytime pages are added, we update this value.
    - To reduce the times needed for this to occur, we double the number of pages in the file after each IO.
- We then have a `uint8` counting the number of elements in the schema, telling us how many records to scan.
- Each schema shape is represented as a `uint16`, followed by the name, terminated by `'\0'`:
    - If the first bit is 1, we have a string of size `n ^ (1 << 16)` bytes
    - If the first bit is 0, for the given values of `n`, we have:
        - 0: Int32
        - 1: Int64
        - 2: Float32
        - 3: Double64
        - 4: Timestamp (32 bits)

### Page Format:
- The page format is fairly simple: it contains all of the rows sequentially, with each row having an additional byte.
- If a row is removed, the byte is set to 1, allowing for us to use this location for insertion later on, and skip it while performing scans.
- Strings of length `n` will have space for all `n` characters, even if it is not initally using the entire space.

### Future Implementation Ideas:
- Creating a free list for pages
- Representing B-Trees in the same file
- Traversing different "lists" in the header of page.
- Creating a free list for the blocks within a page, rather than doing a manual scan.
- Variable Length Types
- Buffer Management

## Resources
We found the following resources particularly helpful in our implementation of a file system:
- [CS 44800 Course Slides](https://www.cs.purdue.edu/homes/clifton/cs44800/)
- [Database System Concepts](https://db-book.com/)
- [SQLite's File Format](https://www.sqlite.org/fileformat.html).