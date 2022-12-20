# GQL Database Server
gql-db is an SQL database server with version control integrated into the database itself. It's written in Rust and uses Protocol Buffers/gRPC for communication. This was our Capstone Software Engineering project for Purdue University.

We've also implemented a UI for the database server, which can be found at: [GQL-Project/gql_client](https://github.com/GQL-Project/gql_client).

## Features
- SQL: 
    - Basic SQL operations: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`
    - Support for `JOIN` operations, as well as `GROUP BY` and `ORDER BY` operations
    - BTrees for indexing
    - User Permisions
- Version Control:
    - Creating, Reverting and Squashing Commits
    - Creating, Switching and Deleting Branches
    - Pulling Changes from source branch
    - Merging Branches, with conflict resolution policies
    - Logs, Viewing Specific Commits and Viewing Database Schema at a specific commit
    - For a full list, look at [vc_commands.rs](src/parser/vc_commands.rs)


## Design Overview
- All branches, commits and files are stored in the database server itself. This differs from git, where commits are first stored in a local repository and then pushed to a remote repository. 
- More of the design choices can be found here: 
    - [Version Control System](src/version_control/README.md) 
    - [Page File Structure](src/fileio/README.md)

## Setting up gql-db
Refer to [SETUP.md](SETUP.md) for how to install Rust/Protocol Buffers and set up the project.

## Running gql-db
To run the server, run the following command in the root directory of the project:
```
cargo run
```

Additionally, to run the server with a demo database, you can instead run:
```
cargo run -- --demo
```

To run a terminal client:
```
cargo run -- --client
```
You can then run SQL commands in the client. For example:
```sql
GQL> select P.*, L.location from personal_info P, locations L where P.id = L.id;
```

To run version control commands in the client, preface the command with `gql`.
```
GQL> gql --help;
GQL> gql squash JRTukqo lhvgqP6
```

## Resources and Helpful Links
- [The Rust Book](https://doc.rust-lang.org/stable/book/)
- [Protocol Buffers Language Guide](https://developers.google.com/protocol-buffers/docs/proto3)
- [Cargo (Package Management) Guide](https://doc.rust-lang.org/cargo/guide)
- [rustup documentation](https://rust-lang.github.io/rustup/index.html)