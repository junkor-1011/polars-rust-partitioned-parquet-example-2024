use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read csv
    let df = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some("data/input-csv/example.csv".into()))?
        .finish()?;
    println!("{df}"); // show content of DataFrame

    // write partitioned parquet
    {
        let mut df = df.clone();
        let _ = write_partitioned_dataset(
            &mut df,
            std::path::Path::new("data/output/partitioned-example"),
            vec!["type"],
            &ParquetWriteOptions::default(),
            4_294_967_296, // Ref: https://github.com/pola-rs/polars/blob/rs-0.43.1/py-polars/polars/dataframe/frame.py#L3651
        )?;
    }

    // read partitioned parquet
    {
        let lf = LazyFrame::scan_parquet_files(
            vec!["data/output/partitioned-example".into()].into(),
            ScanArgsParquet::default(),
        )?;

        println!("DataFrame from partitioned parquet files: \n{}", lf.collect()?);
    }

    Ok(())
}
