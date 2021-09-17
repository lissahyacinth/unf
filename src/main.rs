use std::{fs::File, sync::Arc};

use arrow::{
    array::StringArray,
    csv,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use clap::{App, Arg};
use std::convert::TryInto;
use unf::{unf_from_batch, UnfHashBuilder};

mod unf;
mod unf_config;
mod unf_vector;

use unf_config::UnfConfigBuilder;

use crate::unf_config::UnfVersion;

fn read_data(file_path: String) -> csv::Reader<File> {
    let file = File::open(file_path).unwrap();
    let builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(10));
    builder.build(file).unwrap()
}

fn unf<I>(
    mut unf_hash: UnfHashBuilder,
    batch_input: I,
    config: unf_config::UnfConfig,
) -> unf::UnfHash
where
    I: Iterator<Item = RecordBatch>,
{
    for batch in batch_input {
        unf_hash.hash(batch);
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ColumnHashes",
        DataType::Utf8,
        false,
    )]));
    let mut column_hash_data = unf_hash
        .finalize()
        .into_iter()
        .map(|x| x.short_hash)
        .collect::<Vec<String>>();
    column_hash_data.sort();
    let column_hashes = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(column_hash_data))],
    )
    .unwrap();
    unf_from_batch(column_hashes, &schema, config)
}

fn main() {
    let matches = App::new("Unf")
        .version("0.0.1")
        .arg(
            Arg::with_name("input_file")
                .short("i")
                .value_name("FILE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("truncation")
                .short("t")
                .value_name("TRUNCATION")
                .default_value("128")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("digits")
                .short("d")
                .default_value("7")
                .value_name("DIGITS")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("characters")
                .short("c")
                .value_name("CHARACTERS")
                .default_value("128")
                .takes_value(true),
        )
        .get_matches();
    let file_path = matches.value_of("input_file").unwrap();
    let truncation = matches.value_of("truncation").unwrap();
    let digits = matches.value_of("digits").unwrap();
    let characters = matches.value_of("characters").unwrap();
    let config = UnfConfigBuilder::new()
        .truncation(truncation.parse().unwrap())
        .digits(digits.parse().unwrap())
        .characters(characters.parse().unwrap())
        .build();
    let csv = read_data(file_path.to_string());
    let unf_hash = UnfHashBuilder::new(csv.schema(), UnfVersion::Six, config);
    let res = unf(unf_hash, csv.flatten(), config);
    println!(
        "File: {} | UNF Version: {:?} | ShortHash: {}",
        file_path, config.version, res.short_hash
    );
}

mod tests {
    use super::*;

    #[test]
    fn load_float_from_file() {
        let file_path = "data/ExampleData.csv";
        let config = UnfConfigBuilder::new().build();
        let csv = read_data(file_path.to_string());
        let mut unf_hash = UnfHashBuilder::new(csv.schema(), UnfVersion::Six, config);
        for batch in csv {
            if let Ok(batch) = batch {
                unf_hash.hash(batch);
            }
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ColumnHashes",
            DataType::Utf8,
            false,
        )]));
        let mut column_hash_data = unf_hash
            .finalize()
            .into_iter()
            .map(|x| x.short_hash)
            .collect::<Vec<String>>();
        column_hash_data.sort();
        let column_hashes = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(column_hash_data))],
        )
        .unwrap();
        let res = unf_from_batch(column_hashes, &schema, config);
        assert_eq!(res.short_hash, "Isf0CgUVrEZzLZdf5G46TA==".to_string());
    }
}
