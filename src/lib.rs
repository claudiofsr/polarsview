// Modules that make up the ParqBench library.
mod args;
mod components;
mod data;
mod layout;
mod sqls;
mod traits;

// Publicly expose the contents of these modules.
pub use self::{args::Arguments, components::*, data::*, layout::*, sqls::*, traits::*};

use polars::{
    error::PolarsResult,
    prelude::{Column, DataType, RoundSeries},
};
use std::path::Path;

/// Extracts the file extension from a filename, converting it to lowercase.
///
/// If no extension is found, returns `None`.
///
/// # Arguments
///
/// * `filename` - A string slice representing the filename.
///
/// # Returns
///
/// An `Option<String>` containing the lowercase file extension if found, otherwise `None`.
pub fn get_extension(filename: &str) -> Option<String> {
    Path::new(filename)
        .extension() // Get the extension as an Option<&OsStr>
        .and_then(|ext| ext.to_str()) // Convert the extension to &str, returning None if the conversion fails
        .map(|ext| ext.to_lowercase()) // Convert the extension to lowercase for case-insensitive comparison
}

/// Filtra colunas do tipo float64.
///
/// Posteriormente, arredonda os valores da coluna
pub fn round_float64_columns(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let series = match col.as_series() {
        Some(s) => s,
        None => return Ok(Some(col)),
    };

    match series.dtype() {
        DataType::Float64 => Ok(Some(series.round(decimals)?.into())),
        _ => Ok(Some(col)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_extension() {
        assert_eq!(get_extension("data.parquet"), Some("parquet".to_string()));
        assert_eq!(get_extension("DATA.PARQUET"), Some("parquet".to_string())); // Case-insensitive test
    }

    #[test]
    fn test_csv_extension() {
        assert_eq!(get_extension("data.csv"), Some("csv".to_string()));
        assert_eq!(get_extension("data.CSV"), Some("csv".to_string())); // Case-insensitive test
    }

    #[test]
    fn test_no_extension() {
        assert_eq!(get_extension("data"), None); // No extension
    }

    #[test]
    fn test_empty_filename() {
        assert_eq!(get_extension(""), None); // Empty filename
    }

    #[test]
    fn test_path_with_dots() {
        assert_eq!(get_extension("path.to.file.txt"), Some("txt".to_string()));
    }
}
