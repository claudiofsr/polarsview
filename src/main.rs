#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use polars_view::{Arguments, DataFilters, DataFrameContainer, PolarsViewApp};

/*
cargo fmt
cargo test -- --nocapture
cargo run -- --help
cargo run -- ~/Documents/Rust/projects/join_with_assignments/df_consolidacao_natureza_da_bcalc.parquet
cargo doc --open
cargo b -r && cargo install --path=.
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Initialize the tracing subscriber for logging.
    tracing_subscriber::fmt::init();

    // Parse command-line arguments.
    let args = Arguments::build();

    // Configure the native options for the eframe application.
    let options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        ..Default::default()
    };

    // Run the eframe application.
    eframe::run_native(
        "PolarsView",
        options,
        Box::new(move |cc| {
            // Create a new PolarsViewApp. If a filename is provided, load the data.
            Ok(Box::new(match &args.filename {
                Some(filename) => {
                    // Log debug information about the data filters.
                    DataFilters::debug(&args);

                    // Load the Parquet data from the specified filename.
                    let future = DataFrameContainer::load_data(filename.to_string());

                    // Create a new PolarsViewApp with the data loading future.
                    PolarsViewApp::new_with_future(cc, Box::new(Box::pin(future)))
                }
                None => PolarsViewApp::new(cc), // Create a new PolarsViewApp without loading data.
            }))
        }),
    )
}
