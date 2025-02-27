use crate::{
    ExtraInteractions,
    data::{DataFilters, DataFrameContainer, SortState},
};

use egui::{Color32, Frame, Grid, Layout, Stroke, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use parquet::{
    basic::ColumnOrder,
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
    },
};
use polars::prelude::*;
use rfd::AsyncFileDialog;
use std::{fs::File, path::Path};

// Struct to hold Parquet file metadata.  This is used for reading Parquet-specific metadata.
pub struct FileMetadata {
    info: ParquetMetaData, // Parquet metadata.
}

impl FileMetadata {
    /// Creates a `FileMetadata` instance from a filename.
    pub fn from_filename(filename: &str) -> Result<Self, String> {
        let path = Path::new(filename);

        // Attempt to open the file.
        let file = File::open(path).map_err(|_| "Could not open file".to_string())?;

        // Create a SerializedFileReader to read Parquet metadata.
        let reader = SerializedFileReader::new(file)
            .map_err(|error| format!("Error creating Parquet reader: {}", error))?;

        // Extract and store the Parquet metadata.
        Ok(Self {
            info: reader.metadata().to_owned(),
        })
    }

    /// Renders the file metadata in the UI using egui.
    pub fn render_metadata(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata();

        // Use a frame to visually group the metadata.
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY)) // Thin gray border for visual separation.
            .outer_margin(2.0)
            .inner_margin(10.0)
            .show(ui, |ui| {
                // Extract metadata values, providing defaults if they're missing.
                // Create a grid layout
                Grid::new("version_grid")
                    .num_columns(2)
                    .spacing([10.0, 20.0])
                    .striped(true)
                    .show(ui, |ui| {
                        let created_by = file_metadata.created_by().unwrap_or("unknown");

                        ui.label("Data processing:");
                        ui.label(created_by);
                        ui.end_row();

                        let version = env!("CARGO_PKG_VERSION");

                        ui.label("Polars View Version");
                        ui.label(version);
                        ui.end_row();

                        let nc = file_metadata.schema_descr().num_columns();

                        ui.label("Columns:");
                        ui.label(nc.to_string());
                        ui.end_row();

                        let nr = file_metadata.num_rows();

                        ui.label("Rows:");
                        ui.label(nr.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the file schema information in the UI using egui.
    pub fn render_schema(&self, ui: &mut Ui) {
        let file_metadata = self.info.file_metadata();
        // Iterate over the columns in the schema.
        for (idx, field) in file_metadata.schema_descr().columns().iter().enumerate() {
            // Create a collapsing header for each column to show its details.
            ui.collapsing(field.name(), |ui| {
                // Determine the field type and format it as a string.
                let field_type = field.self_type();
                let field_type_str = if field_type.is_primitive() {
                    format!("{}", field_type.get_physical_type())
                } else {
                    format!("{}", field.converted_type())
                };

                // Display the field type.
                ui.label(format!("type: {}", field_type_str));

                // Display the sort order of the column, if defined.
                ui.label(format!(
                    "sort_order: {}",
                    match file_metadata.column_order(idx) {
                        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) => format!("{}", sort_order),
                        _ => "undefined".to_string(),
                    }
                ));
            });
        }
    }
}

impl DataFrameContainer {
    /// Renders the DataFrame as a table using egui.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        let style = ui.style().as_ref();

        /// Checks if a given column is currently sorted.
        fn is_sorted_column(sorted_col: &Option<SortState>, column_name: &str) -> bool {
            match sorted_col {
                Some(sort) => match sort {
                    SortState::Ascending(sorted_column) => *sorted_column == column_name,
                    SortState::Descending(sorted_column) => *sorted_column == column_name,
                    SortState::NotSorted(_) => false, // Not sorted state, returns false.
                },
                None => false, // No sort state, returns false.
            }
        }

        let mut filters: Option<DataFilters> = None; // The `DataFilters` to be returned if sorting is applied.
        let mut sorted_column = self.filters.sort.clone(); // The current sort state of the table.

        let text_height = TextStyle::Body.resolve(style).size; // Height of a text line, used for row height calculation.

        let initial_col_width =
            (ui.available_width() - style.spacing.scroll.bar_width) / (self.df.width() + 1) as f32; // Initial column width, based on available width.

        // Prevents columns from resizing smaller than the window. Remainder stops the last column
        // growing, which we explicitly want to allow for the case of large datatypes.
        let min_col_width = if style.spacing.interact_size.x > initial_col_width {
            style.spacing.interact_size.x
        } else {
            initial_col_width / 4.0
        };

        let header_height = style.spacing.interact_size.y + 2.0f32 * style.spacing.item_spacing.y; // Header height, including padding.

        // Configuration for the table columns.  See https://github.com/emilk/egui/issues/3680
        let column = Column::initial(initial_col_width)
            .at_least(min_col_width)
            .resizable(true)
            .clip(true);

        // Defines a closure to render the table header.  This creates the interactive sort buttons.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            // Iterate over the column names in the DataFrame.
            for column_name in self.df.get_column_names() {
                table_row.col(|ui| {
                    // Determine the current sort state of the column.
                    let column_label = if is_sorted_column(&sorted_column, column_name) {
                        sorted_column.clone().unwrap() // Display the sort state (ascending/descending).
                    } else {
                        SortState::NotSorted(column_name.to_string()) // Default to "not sorted".
                    };

                    // Create a centered layout for the sort button.
                    ui.horizontal_centered(|ui| {
                        // Creates the sort button using the ExtraInteractions trait.
                        let response = ui.sort_button(&mut sorted_column, column_label.clone());
                        if response.clicked() {
                            // If the sort button is clicked, create a DataFilters to trigger a resort.
                            filters = Some(DataFilters {
                                sort: sorted_column.clone(), // Updates the filters with the new sort state.
                                ..self.filters.clone()       // Inherit other filter settings.
                            });
                        }
                    });
                });
            }
        };

        // Defines a closure to render the table rows.
        // This displays the data from each cell.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            let row_index = table_row.index(); // Gets the current row index.

            // Iterate over the columns in the DataFrame.
            for column in self.df.get_columns() {
                // Convert the AnyValue in the cell to a String for display.
                let mut value: String = column
                    .get(row_index)
                    .map(|any_value| {
                        match any_value {
                            AnyValue::String(s) => s.to_string(),
                            AnyValue::Null => "".to_string(), // Display "" for Null values.
                            av => av.to_string(), // Fallback to Debug formatting for other types.
                        }
                    })
                    .unwrap_or_else(|_| "Error: This is not a value!".to_string());

                // Determine the layout based on the data type for alignment.
                let layout = if column.dtype().is_float() {
                    // Check if the column name contains "Alíquota" (tax rate in Portuguese)
                    let col_aliquota = column.name().contains("Alíquota");

                    // Convert string to floating point number and format it
                    value = match value.trim().parse::<f64>() {
                        Ok(float) => {
                            // If column is Alíquota format to 4 decimal places, else to 2.
                            if col_aliquota {
                                format!("{float:0.4}")
                            } else {
                                format!("{float:0.2}")
                            }
                        }
                        Err(_) => value, // If parsing fails, keep the original string.
                    };

                    // Align center if it's an "Alíquota" column, otherwise align right.
                    if col_aliquota {
                        Layout::centered_and_justified(egui::Direction::LeftToRight)
                    } else {
                        Layout::right_to_left(egui::Align::Center)
                    }
                } else if column.dtype().is_integer() {
                    // Center integer values.
                    Layout::centered_and_justified(egui::Direction::LeftToRight)
                } else {
                    // Default to left alignment for other data types.
                    Layout::left_to_right(egui::Align::Center)
                };

                // Add the cell to the table row.
                table_row.col(|ui| {
                    // Display the value within the determined layout.
                    // Disable wrapping to prevent text overflow.
                    ui.with_layout(layout.with_main_wrap(false), |ui| {
                        ui.label(value); // Display the value.
                    });
                });
            }
        };

        // Build the table using egui_extras::TableBuilder.
        TableBuilder::new(ui)
            .striped(false) // Disable striped rows.
            .columns(column, self.df.width()) // Set up the columns.
            .column(Column::remainder())
            .auto_shrink([false, false]) // Disable auto-shrinking to fit content.
            .min_scrolled_height(1000.0) // Set a minimum height for the table.
            .header(header_height, analyze_header) // Render the table header.
            .body(|body| {
                let num_rows = self.df.height();
                body.rows(text_height, num_rows, analyze_rows); // Render the table rows.
            });

        filters // Returns the DataFilters if sorting has been applied.
    }
}

/// Asynchronously opens a file dialog.
pub async fn file_dialog() -> Result<String, String> {
    let opt_file_handle = AsyncFileDialog::new().pick_file().await; // Open the file dialog.

    match opt_file_handle {
        Some(file_handle) => Ok(file_handle.file_name()), // Return the filename if a file is selected.
        None => Err("No file loaded.".to_string()),       // Return an error if no file is selected.
    }
}
