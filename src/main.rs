use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read csv
    let df = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some("data/input-csv/example.csv".into()))?
        .finish()?;
    println!("{df}"); // show content of DataFrame

    Ok(())
}
