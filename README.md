# Setting up GQL_DB
## Setting up Rust
- Install Rust from [here](https://www.rust-lang.org/tools/install), 64-bit if you can.
- On Windows, you'll also want to install the [Visual Studio C++ Build Tools](https://visualstudio.microsoft.com/downloads/). 
    - Go to `Tools for Visual Studio`, download and run `Build Tools for Visual Studio 2022`. 
    - When selecting tools to install in the program:
    1. Go to the `Workloads` tab and click `Desktop Development with C++`.
    2. Go to the `Individual Components` tab and search for `Windows 11 SDK`, click the latest one.
- To ensure things are working, open up a terminal, and run `cargo --help` and `rustc --help`.
- Also, installing [Windows Terminal](https://apps.microsoft.com/store/detail/windows-terminal/9N0DX20HK701) will save you so much time later on, it's just much cleaner and easier to use than Git Bash or Command Prompt.

## Setting up the Project
- Rust uses a cool package manager called `cargo`. Here are the main commands you need to know:
    - `cargo run`: Compile and run your code
    - `cargo build --release`: Compile your code, optionally with optimizations enabled.
    - `cargo test`: Run all the tests
    - `cargo clean`: Remove binaries and build artifacts
    - `cargo fix`: Fix linter and code warnings in the code.
    - `cargo bench`: Run defined benchmarks (for later sprints, maybe)
    - Any dependencies we add will be automatically downloaded, and builds are pretty simple. 
- Anyways: 
    1. `git clone git@github.com:GQL-Project/gql_db.git`
    2. `cd gql_db`
    3. Ensure `cargo run` works now.

## Setting up VS Code for Rust
- Download and install [VS Code](https://code.visualstudio.com/).
- Open the project folder `gql_db` in VS Code
- In the extensions market place, first install [`rust-analyzer`](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
- Then, to help with debugging, install: [`cpp-tools`](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools).
- Ensure that the `main.rs` file shows errors when removing semi-colons and etc.
- For more help, refer to [this article](https://code.visualstudio.com/docs/languages/rust). 

## Resources to Use
- [The Rust Book](https://doc.rust-lang.org/stable/book/)
- [Cargo (Package Management) Guide](https://doc.rust-lang.org/cargo/guide)
- [rustup documentation](https://rust-lang.github.io/rustup/index.html)