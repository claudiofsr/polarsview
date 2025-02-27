use crate::{Arguments, SQL_COMMANDS, get_extension};
use egui::{
    Align, CollapsingHeader, Color32, Frame, Grid, Hyperlink, Layout, Stroke, TextEdit, Ui, Vec2,
};
use polars::{prelude::*, sql::SQLContext};
use std::{fs::File, future::Future, sync::Arc};

pub type DataResult = Result<DataFrameContainer, String>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

// Set values that will be interpreted as missing/null.
static NULL_VALUES: &[&str] = &["", " ", "<N/D>", "*DIVERSOS*"];

/// Represents the sorting state for a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SortState {
    /// The column is not sorted.
    NotSorted(String),
    /// The column is sorted in ascending order.
    Ascending(String),
    /// The column is sorted in descending order.
    Descending(String),
}

/// Holds filters to be applied to the data.
#[derive(Clone, Debug, Default)]
pub struct DataFilters {
    /// Optional filename of the data source.
    pub filename: Option<String>,
    /// Optional table name for registering with Polars SQL Context.
    pub table_name: Option<String>,
    /// Optional CSV delimiter.
    pub csv_delimiter: Option<String>,
    /// Optional SQL query to apply to the data.
    pub query: Option<String>,
    /// Optional column sorting state.
    pub sort: Option<SortState>,
}

impl DataFilters {
    /// Creates a new `DataFilters` instance with a default configuration, including the filename.
    pub fn new(filename: impl AsRef<str> + ToString) -> Self {
        DataFilters {
            filename: Some(filename.to_string()),
            table_name: Some("AllData".to_string()),
            csv_delimiter: Some(";".to_string()),
            query: Some(SQL_COMMANDS[0].to_string()),
            ..Default::default()
        }
    }

    /// Prints the debug information about the `DataFilters` based on the provided `Arguments`.
    pub fn debug(args: &Arguments) {
        let data_filters = DataFilters {
            filename: args.filename.clone(),
            query: args.query.clone(),
            table_name: args.table_name.clone(),
            ..Default::default()
        };

        dbg!(data_filters);
    }

    /// Renders the query pane UI for configuring data filters.
    pub fn render_filter(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Create mutable copies of the filter values to allow editing.
        let mut filename = self.filename.clone()?;
        let mut table_name = self.table_name.clone()?;
        let mut csv_delimiter = self.csv_delimiter.clone()?;
        let mut query = self.query.clone()?;

        let width_max = ui.available_width();

        // Create a grid layout for the filter configuration.
        let mut result = None; // Mover a declaração para fora do Grid

        let grid = Grid::new("data_filters_grid")
            .num_columns(2)
            .spacing([10.0, 20.0])
            .striped(true);

        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()),
            Layout::top_down(Align::LEFT),
            |ui| {
                grid.show(ui, |ui| {
                ui.label("Filename:");
                let filename_edit = TextEdit::singleline(&mut filename).desired_width(width_max);
                ui.add(filename_edit)
                    .on_hover_text("Enter filename and press the Apply button...");
                ui.end_row();

                ui.label("Table Name:");
                let table_name_edit =
                    TextEdit::singleline(&mut table_name).desired_width(width_max);
                ui.add(table_name_edit)
                    .on_hover_text("Enter table name for SQL queries...");
                ui.end_row();

                ui.label("CSV Delimiter:");
                let csv_delimiter_edit =
                    TextEdit::singleline(&mut csv_delimiter).desired_width(width_max);
                ui.add(csv_delimiter_edit)
                    .on_hover_text("Enter the CSV delimiter character...");
                ui.end_row();

                ui.label("SQL Query:");
                let query_edit = TextEdit::multiline(&mut query).desired_width(width_max);
                ui.add(query_edit)
                    .on_hover_text("Enter SQL query to filter and transform the data...");
                ui.end_row();

                // Add the button to the grid.
                ui.label(""); // Empty label to align with the label column.
                ui.with_layout(Layout::top_down(Align::Center), |ui| {
                    if ui.button("Apply SQL Commands").clicked() {
                        // Only create and return DataFilters if the required fields are not empty.
                        if !filename.trim().is_empty()
                            && !table_name.trim().is_empty()
                            && !csv_delimiter.trim().is_empty()
                            && !query.trim().is_empty()
                        {
                            result = Some(DataFilters {
                                filename: Some(filename.clone()),
                                table_name: Some(table_name.clone()),
                                csv_delimiter: Some(csv_delimiter.clone()),
                                query: Some(query.clone()),
                                sort: self.sort.clone(), // Preserve existing sort state
                            });
                        } else {
                            // Handle the case where required fields are empty.
                            eprintln!(
                                "Error: Filename, Table Name, CSV Delimiter, and Query cannot be empty."
                            );
                            result = None;
                        }
                    }
                });
                ui.end_row();
            });
        });

        // Update the filter values with the edited values.
        self.filename = Some(filename);
        self.table_name = Some(table_name);
        self.csv_delimiter = Some(csv_delimiter);
        self.query = Some(query);

        // Collapsing header for SQL command examples.
        CollapsingHeader::new("SQL Command Examples:")
            .default_open(false) // Initially collapsed.
            .show(ui, |ui| {
                // Highlighted frame for displaying SQL command examples.
                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY))
                    .outer_margin(2.0)
                    .inner_margin(10.0)
                    .show(ui, |ui| {
                        ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                            let url =
                                "https://docs.pola.rs/api/python/stable/reference/sql/index.html";
                            let heading = Hyperlink::from_label_and_url("SQL Interface", url);
                            ui.add(heading).on_hover_text(url);
                        });
                        ui.add(egui::Label::new(SQL_COMMANDS.join("\n\n")).selectable(true));
                    });
            });

        result // Retorne o resultado
    }
}

/// Contains a DataFrame along with associated metadata and filters.
#[derive(Debug, Clone)]
pub struct DataFrameContainer {
    /// The filename associated with the DataFrame.
    pub filename: String,
    /// The Polars DataFrame, wrapped in an Arc for shared ownership and thread-safe access.
    pub df: Arc<DataFrame>,
    /// Filters applied to the DataFrame.
    pub filters: DataFilters,
}

impl DataFrameContainer {
    /// Loads data from a file (Parquet or CSV) using Polars.
    pub async fn load_data(filename: impl AsRef<str>) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        // Determine file type based on extension and load accordingly.
        let df = match get_extension(&filename).as_deref() {
            Some("parquet") => Self::read_parquet(&filename).await,
            Some("csv") => Self::read_csv(&filename).await,
            _ => {
                let msg = format!("Unknown file type: {:#?}", filename);
                return Err(msg);
            }
        }?;

        Ok(Self {
            filename,
            df: Arc::new(df),
            filters: DataFilters::default(),
        })
    }

    /// Reads a Parquet file into a Polars DataFrame.
    async fn read_parquet(filename: &str) -> Result<DataFrame, String> {
        let file = File::open(filename).map_err(|e| format!("Error opening file: {}", e))?;
        let df = ParquetReader::new(file)
            .finish()
            .map_err(|e| format!("Error reading parquet: {}", e))?;

        Ok(df)
    }

    /// Attempts to read a CSV file with different delimiters until successful.
    async fn read_csv(filename: &str) -> Result<DataFrame, String> {
        // Delimiters to attempt when reading CSV files.
        let delimiters = [b',', b';', b'|', b'\t'];

        for delimiter in delimiters {
            let result_df = Self::attempt_read_csv(filename, delimiter).await;

            if let Ok(df) = result_df {
                return Ok(df); // Return the DataFrame on success
            }
        }

        let msg = "Failed to read CSV with common delimiters or inconsistent data.";
        eprintln!("{msg}");
        Err(msg.to_string())
    }

    /// Attempts to read a CSV file using a specific delimiter.
    async fn attempt_read_csv(filename: &str, delimiter: u8) -> Result<DataFrame, String> {
        dbg!(&filename, delimiter as char);

        // Set values that will be interpreted as missing/null.
        let null_values: Vec<PlSmallStr> = NULL_VALUES.iter().map(|&s| s.into()).collect();

        // Configure the CSV reader with flexible options.
        let lazyframe = LazyCsvReader::new(filename)
            .with_encoding(CsvEncoding::LossyUtf8) // Handle various encodings
            .with_has_header(true) // Assume the first row is a header
            .with_try_parse_dates(true) // use regex
            .with_separator(delimiter) // Set the delimiter
            .with_infer_schema_length(Some(200)) // Limit schema inference to the first 200 rows.
            .with_ignore_errors(true) // Ignore parsing errors
            .with_missing_is_null(true) // Treat missing values as null
            .with_null_values(Some(NullValues::AllColumns(null_values)))
            .finish()
            .map_err(|e| {
                format!(
                    "Error reading CSV with delimiter '{}': {}",
                    delimiter as char, e
                )
            })?;

        // Collect the lazy DataFrame into a DataFrame
        let df = lazyframe
            //.with_columns(cols()).apply(|col| round, GetOutput::from_type(DataType::String))
            .collect()
            .map_err(|e| format!("{}", e))?;

        /*
        let lz = lazyframe // Formatar colunas
            .with_columns([
                all().map(|series| {
                    series.fill_null(FillNullStrategy::Zero)
                }, GetOutput::from_type(DataType::String))
                /*
                .map(|series| round_float64_columns(series, 2),
                    GetOutput::same_type()
                    //GetOutput::from_type(DataType::String)
                )
                */
            ]);
        */

        // Check if the number of columns is reasonable
        if df.width() <= 1 {
            let msg = format!("Erro em delimiter: {}", delimiter as char);
            return Err(msg.to_string());
        }

        Ok(df)
    }

    /// Loads data and applies a SQL query using Polars.
    pub async fn load_data_with_sql(filters: DataFilters) -> Result<Self, String> {
        dbg!(&filters);

        // Extract required parameters from filters
        let Some(filename) = filters.filename.clone() else {
            return Err("No filename".to_string());
        };

        let Some(table_name) = filters.table_name.clone() else {
            return Err("No Table Name".to_string());
        };

        let Some(csv_delimiter) = filters.csv_delimiter.clone() else {
            return Err("No CSV Delimiter".to_string());
        };

        let Some(query) = &filters.query else {
            return Err("No query provided".to_string());
        };

        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        // Load the DataFrame from the file
        let df: DataFrame = match get_extension(&filename).as_deref() {
            Some("parquet") => Self::read_parquet(&filename).await?,
            Some("csv") => {
                // Convert csv_delimiter string to u8 delimiter
                let delimiter: u8 = match csv_delimiter.len() {
                    1 => csv_delimiter.as_bytes()[0],
                    _ => {
                        let msg = "Error: The CSV delimiter must be a single character.";
                        return Err(msg.to_string());
                    }
                };

                // Set values that will be interpreted as missing/null.
                let null_values: Vec<PlSmallStr> = NULL_VALUES.iter().map(|&s| s.into()).collect();

                // Read CSV using the specified delimiter
                let lazyframe = LazyCsvReader::new(&filename)
                    .with_encoding(CsvEncoding::LossyUtf8) // Handle various encodings
                    .with_try_parse_dates(true) // use regex
                    .with_has_header(true) // Assume the first row is a header
                    .with_separator(delimiter) // Set the delimiter
                    .with_infer_schema_length(Some(200)) // Limit schema inference to the first 200 rows.
                    .with_ignore_errors(true) // Ignore parsing errors
                    .with_missing_is_null(true) // Treat missing values as null
                    .with_null_values(Some(NullValues::AllColumns(null_values)))
                    .finish()
                    .map_err(|e| {
                        format!(
                            "Error reading CSV with delimiter '{}': {}",
                            delimiter as char, e
                        )
                    })?;

                lazyframe.collect().map_err(|e| format!("Error: {}", e))?
            }
            _ => {
                let msg = format!("Unknown file type: {}", filename);
                return Err(msg);
            }
        };

        // Create a SQL context and register the DataFrame
        let mut ctx = SQLContext::new();
        ctx.register(&table_name, df.lazy());

        // Execute the query and collect the results
        let sql_df: DataFrame = ctx
            .execute(query)
            .map_err(|e| format!("Polars SQL error: {}", e))?
            .collect()
            .map_err(|e| format!("DataFrame error: {}", e))?;

        Ok(Self {
            filename,
            df: Arc::new(sql_df),
            filters,
        })
    }

    /// Sorts the data based on the provided filters.
    pub async fn sort(mut self, opt_filters: Option<DataFilters>) -> Result<Self, String> {
        // If no filters are provided, return the DataFrame as is.
        let Some(filters) = opt_filters else {
            return Ok(self);
        };

        // If no sort is specified, return the DataFrame as is.
        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

        // Extract sort column and order from filters
        let (col_name, ascending) = match sort {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            SortState::NotSorted(_col_name) => return Ok(self),
        };

        dbg!(sort);
        dbg!(col_name);
        dbg!(ascending);

        // Define sort options
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true)
            .with_multithreaded(true)
            .with_order_descending(!ascending) // Sort order: ascending or descending
            .with_nulls_last(false);

        // Sort the DataFrame using Polars
        self.df = Arc::new(
            self.df
                .sort([col_name], sort_options)
                .map_err(|e| format!("Polars sort error: {}", e))?,
        );
        self.filters = filters; //Update filters

        Ok(self)
    }
}

// font: polars-0.46.0/tests/it/io/csv.rs
#[test]
fn test_quoted_bool_ints() -> PolarsResult<()> {
    let csv = r#"foo,bar,baz
1,"4","false"
3,"5","false"
5,"6","true"
"#;
    let file = std::io::Cursor::new(csv);
    let df = CsvReader::new(file).finish()?;
    let expected = df![
        "foo" => [1, 3, 5],
        "bar" => [4, 5, 6],
        "baz" => [false, false, true],
    ]?;
    assert!(df.equals_missing(&expected));

    Ok(())
}
