use crate::{
    Error, MyStyle, Popover, Settings,
    components::{FileMetadata, file_dialog},
    data::{DataFilters, DataFrameContainer, DataFuture},
};

use egui::{
    CentralPanel, Context, FontId, RichText, ScrollArea, SidePanel, TopBottomPanel,
    ViewportCommand, menu, style::Visuals, warn_if_debug_build, widgets,
};
use std::sync::Arc;
use tokio::sync::oneshot::{self, error::TryRecvError};

/// The main application struct for PolarsView.
pub struct PolarsViewApp {
    /// The `DataFrameContainer` holds the loaded data (Parquet, CSV, etc.).  Using `Arc` for shared ownership and thread-safe access.
    pub table: Arc<Option<DataFrameContainer>>,
    /// Component for managing data filters (SQL queries, sorting, etc.).
    pub data_filters: DataFilters,
    /// Metadata extracted from the loaded file (if available).
    pub metadata: Option<FileMetadata>,
    /// Optional popover window for displaying errors, settings, or other notifications.
    pub popover: Option<Box<dyn Popover>>,

    /// Tokio runtime for asynchronous operations (file loading, queries).
    runtime: tokio::runtime::Runtime,
    /// Channel for receiving the result of asynchronous data loading.
    pipe: Option<tokio::sync::oneshot::Receiver<Result<DataFrameContainer, String>>>,

    /// Vector of active asynchronous tasks.  Used to prevent the application from hanging if a task fails.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    fn default() -> Self {
        Self {
            table: Arc::new(None),
            data_filters: DataFilters::default(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime"),
            pipe: None,
            popover: None,
            metadata: None,
            tasks: Vec::new(),
        }
    }
}

impl PolarsViewApp {
    /// Creates a new `PolarsViewApp` instance.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_visuals(Visuals::dark()); // Set dark theme.
        cc.egui_ctx.set_style_init(); // Apply custom styles.
        Default::default()
    }

    /// Creates a new `PolarsViewApp` with a pre-existing `DataFuture`.  Used for asynchronous loading when the filename is known in advance.
    pub fn new_with_future(cc: &eframe::CreationContext<'_>, future: DataFuture) -> Self {
        let mut app: Self = Default::default();
        cc.egui_ctx.set_visuals(Visuals::dark());
        cc.egui_ctx.set_style_init();
        app.run_data_future(future, &cc.egui_ctx);
        app
    }

    /// Checks if a popover is active and displays it.  If the popover is closed by the user, it is removed.
    fn check_popover(&mut self, ctx: &Context) {
        if let Some(popover) = &mut self.popover {
            if !popover.show(ctx) {
                self.popover = None; // Remove closed popover.
            }
        }
    }

    /// Checks if there is a data loading operation pending (asynchronous).
    ///
    /// Returns `true` if data is still loading, `false` otherwise.  Also handles potential errors from the loading process.
    fn check_data_pending(&mut self) -> bool {
        // Take the receiver out of the `Option`.  This allows us to check if the data has arrived.
        let Some(mut output) = self.pipe.take() else {
            return false; // No data loading in progress.
        };

        match output.try_recv() {
            Ok(data) => match data {
                Ok(data) => {
                    // Data loaded successfully!
                    let filename = data.filename.clone();
                    dbg!(&filename);

                    // Create data filters
                    let mut data_filters = DataFilters::new(&filename);
                    if let Some(delimiter) = &data.filters.csv_delimiter {
                        data_filters.csv_delimiter = Some(delimiter.to_string())
                    }
                    self.data_filters = data_filters;

                    dbg!(&data.filters);

                    // Load metadata
                    self.metadata = FileMetadata::from_filename(&filename).ok();
                    self.table = Arc::new(Some(data));
                    false // Data loading complete.
                }
                Err(msg) => {
                    // An error occurred during data loading.
                    self.popover = Some(Box::new(Error { message: msg }));
                    false // Data loading complete (with an error).
                }
            },
            Err(error) => match error {
                TryRecvError::Empty => {
                    // Data is still loading. Put the receiver back into the `Option`.
                    self.pipe = Some(output);
                    true // Data loading still in progress.
                }
                TryRecvError::Closed => {
                    // The data loading task was terminated unexpectedly.
                    self.popover = Some(Box::new(Error {
                        message: "Data operation terminated without response.".to_string(),
                    }));
                    false // Data loading complete (due to termination).
                }
            },
        }
    }

    /// Runs a `DataFuture` to load data asynchronously. This function takes a future, spawns a Tokio task, and sets up a channel to receive the result.
    fn run_data_future(&mut self, future: DataFuture, ctx: &Context) {
        // Before scheduling a new future, ensure no tasks are stuck
        self.tasks.retain(|task| !task.is_finished());

        // Create a oneshot channel for sending the data from the async task to the UI thread.
        let (tx, rx) = oneshot::channel::<Result<DataFrameContainer, String>>();
        self.pipe = Some(rx);

        // Clone the context for use within the asynchronous task (to request repaints).
        let ctx_clone = ctx.clone();

        // Spawn an async task to load the data.
        let handle = self.runtime.spawn(async move {
            let data = future.await;
            if tx.send(data).is_err() {
                eprintln!("Receiver dropped before data could be sent."); // Handle potential error if the receiver is dropped.
            }
            ctx_clone.request_repaint(); // Request a repaint of the UI to display the loaded data.
        });

        self.tasks.push(handle); // Track the task.
    }
}

// See
// https://github.com/emilk/egui/blob/master/examples/custom_window_frame/src/main.rs
// https://rodneylab.com/trying-egui/

impl eframe::App for PolarsViewApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Check and display any active popovers (errors, settings, etc.).
        self.check_popover(ctx);

        // Handle dropped files.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            if let Some(path) = &dropped_file.path {
                if let Some(filename) = path.to_str() {
                    // Load data from the dropped file.
                    self.run_data_future(
                        Box::new(Box::pin(DataFrameContainer::load_data(
                            filename.to_string(),
                        ))),
                        ctx,
                    );
                }
            }
        }

        // Define the main UI layout.
        //
        // Using static layout until I put together a TabTree that can make this dynamic
        //
        //  | menu_bar        widgets |
        //  ---------------------------
        //  |         |               |
        //  | Data    |     main      |
        //  | Filters |     table     |
        //  |         |               |
        //  ---------------------------
        //  | notification footer     |

        TopBottomPanel::top("top_panel").show(ctx, |ui| {
            menu::bar(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.menu_button("File", |ui| {
                        if ui.button("Open").clicked() {
                            // Open a file dialog to select a file.
                            if let Ok(filename) = self.runtime.block_on(file_dialog()) {
                                self.run_data_future(
                                    Box::new(Box::pin(DataFrameContainer::load_data(filename))),
                                    ctx,
                                );
                            }
                            ui.close_menu();
                        }

                        if ui.button("Settings").clicked() {
                            // Show the settings popover.
                            self.popover = Some(Box::new(Settings {}));
                            ui.close_menu();
                        }

                        ui.menu_button("About", |ui| {
                            // Display application information.
                            let version = env!("CARGO_PKG_VERSION");
                            let authors = env!("CARGO_PKG_AUTHORS");
                            ui.label(RichText::new("Polars View").font(FontId::proportional(20.0)));
                            ui.label(format!("Version: {version}"));
                            ui.label(format!("Author: {authors}"));
                            ui.label("Built with egui");
                        });

                        if ui.button("Quit").clicked() {
                            // Close the application.
                            ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                        }
                    });

                    // Add spacing to align theme switch to the right.
                    let delta = ui.available_width() - 15.0;
                    if delta > 0.0 {
                        ui.add_space(delta);
                        widgets::global_theme_preference_switch(ui);
                    }
                });
            });
        });

        SidePanel::left("side_panel")
            .resizable(true)
            .show(ctx, |ui| {
                ScrollArea::vertical().show(ui, |ui| {
                    // Add Metadata section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui);
                        });
                    }

                    // Add Query section
                    ui.collapsing("Query", |ui| {
                        if let Some(filters) = self.data_filters.render_filter(ui) {
                            // Load data with the applied query.
                            self.run_data_future(
                                Box::new(Box::pin(DataFrameContainer::load_data_with_sql(filters))),
                                ctx,
                            );
                        }
                    });

                    // Add Schema section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                            metadata.render_schema(ui);
                        });
                    }
                });
            });

        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            // Display the filename of the loaded data.
            ui.horizontal(|ui| match &*self.table {
                Some(table) => {
                    ui.label(format!("{:#?}", table.filename));
                }
                None => {
                    ui.label("no file set");
                }
            });
        });

        // Main table display area.
        // https://whoisryosuke.com/blog/2023/getting-started-with-egui-in-rust
        // https://github.com/emilk/egui/issues/1376
        // https://github.com/emilk/egui/discussions/3069
        // https://github.com/lucasmerlin/hello_egui/blob/main/crates/egui_dnd/examples/horizontal.rs
        // https://github.com/vvv/egui-table-click/blob/table-row-framing/src/lib.rs
        // https://github.com/emilk/eframe_template/blob/4f613f5d6266f0f0888544df4555e6bc77a5d079/src/app.rs
        CentralPanel::default().show(ctx, |ui| {
            warn_if_debug_build(ui); // Show a warning in debug builds.

            match self.table.as_ref().clone() {
                Some(parquet_data) if parquet_data.df.width() > 0 => {
                    // Data loaded successfully, display the table.
                    ScrollArea::horizontal().show(ui, |ui| {
                        let opt_filters = parquet_data.render_table(ui); // Render the table and get any filter updates.
                        if let Some(filters) = opt_filters {
                            let future = parquet_data.sort(Some(filters)); // Sort the data.
                            self.run_data_future(Box::new(Box::pin(future)), ctx); // Run the sorting task.
                        }
                    });
                }
                _ => {
                    // No data loaded yet, show a prompt.
                    ui.centered_and_justified(|ui| {
                        ui.label("Drag and drop parquet file here.");
                    });
                }
            };

            // Show a loading spinner if data is currently being loaded.
            if self.check_data_pending() {
                ui.disable(); // Disable UI interaction while loading.
                if self.table.as_ref().is_none() {
                    ui.centered_and_justified(|ui| {
                        // Show spinner while loading initial data.
                        ui.spinner();
                    });
                }
            }
        });
    }
}
