# Polars View

[![License](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

**A fast, cross-platform viewer for Parquet and CSV files, powered by Polars and egui.**

## Overview

Polars View is a lightweight and efficient tool for quickly inspecting and exploring Parquet and CSV datasets. Built with the [Polars](https://www.pola.rs/) data processing library and the [egui](https://www.egui.rs/) immediate mode GUI framework, Polars View offers a user-friendly interface for viewing, filtering, and sorting tabular data. It also supports querying data using SQL.

**This project is a fork of [parqbench](https://github.com/Kxnr/parqbench), reimagined to leverage the power of Polars instead of DataFusion.**

## Features

*   **Fast Loading:** Leverages Polars for efficient data loading and processing.
*   **Cross-Platform:** Runs on Windows, macOS, and Linux.
*   **Parquet and CSV Support:** Handles both popular data formats.
*   **User-Friendly Interface:** Uses egui for a responsive and intuitive GUI.
*   **Filtering:** Easily filter data using SQL queries.
*   **Sorting:** Sort data by one or more columns in ascending or descending order.
*   **Metadata Display:** View file metadata and schema information.
*   **SQL Querying:** Search and filter data using SQL syntax.

## Installation

1.  **Install Rust:** If you don't have Rust installed, download it from [https://www.rust-lang.org/](https://www.rust-lang.org/) using `rustup`. Polars View requires Rust version 1.85 or higher.

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/[your_username]/polars-view.git
    cd polars-view
    ```

3.  **Build the Project:**

    ```bash
    cargo build --release
    ```

## Usage

1.  **Run the Executable:** The compiled executable will be located in the `target/release` directory.

    ```bash
    ./target/release/polars-view
    ```

2.  **Open a File:**
    *   Drag and drop a Parquet or CSV file onto the application window.
    *   Alternatively, use the "File > Open" menu option.

3.  **Explore the Data:**
    *   View the data in a tabular format.
    *   Use the "Query" panel to apply SQL-like filters.
    *   Click on column headers to sort the data.
    *   View file metadata and schema information in the side panel.

### Using SQL Queries

Polars View allows you to query your data using SQL syntax for powerful filtering and data manipulation.

1.  **Enter Your Query:** In the "Query" panel, enter your SQL query in the text area. The table name is `AllData`.

2.  **Apply the Query:** Click the "Apply SQL Commands" button.

**Example Queries:**

*   `SELECT * FROM AllData WHERE "column name" > 100;`
*   `SELECT column1, column2 FROM AllData WHERE column3 = 'value';`
*   `SELECT COUNT(*) FROM AllData;`

**Important Notes:**

*   Polars SQL uses a subset of standard SQL syntax. Consult the Polars documentation for specific limitations and supported features.
*   Ensure your column names in the SQL query match the actual column names in your data.
*   Complex queries may take longer to execute, especially on large datasets.  Use quotes for column names containing spaces or special characters.

## Command-Line Arguments

Polars View supports the following command-line arguments:

*   `-f, --filename <FILE>`: Open the specified file on startup.
*   `-q, --query <SQL>`: Apply the SQL query to the loaded data.
*   `-t, --table_name <NAME>`: Assign a table name for queries. (Note: the table name defaults to `AllData`.)

## Contributing

Contributions are welcome! Please feel free to submit bug reports, feature requests, or pull requests.

1.  **Fork the Repository.**
2.  **Create a Branch:** `git checkout -b feature/your-feature`
3.  **Make Changes and Commit:** `git commit -m "Add your feature"`
4.  **Push to Origin:** `git push origin feature/your-feature`
5.  **Create a Pull Request.**

## License

This project is licensed under the [GPL-3.0-or-later](LICENSE) license.

## Acknowledgements

*   [Polars](https://www.pola.rs/) - For fast and efficient data processing, including SQL querying.
*   [egui](https://www.egui.rs/) - For the easy-to-use GUI framework.
*   [tokio](https://tokio.rs/) - For asynchronous runtime.
*   [rfd](https://github.com/native-toolkit/rfd) - For the native file dialogs.
*   [parqbench](https://github.com/Kxnr/parqbench) - Inspiration and initial structure for this project.

## Screenshots

*(Add screenshots of your application here, including the Query panel to showcase the SQL functionality)*