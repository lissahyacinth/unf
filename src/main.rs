use std::{fs::File, sync::Arc};

use arrow::{
    array::StringArray,
    csv,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use unf::{unf_from_batch, UnfHashBuilder};

use clap::{App, Arg};

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

fn main() {
    let matches = App::new("Unf")
        .version("0.0.1")
        .arg(
            Arg::with_name("input_file")
                .short("i")
                .value_name("FILE")
                .takes_value(true),
        )
        .get_matches();
    let file_path = matches.value_of("input_file").unwrap();
    let config = UnfConfigBuilder::new().build();
    let csv = read_data(file_path.to_string());
    let mut unf_hash = UnfHashBuilder::new(csv.schema(), UnfVersion::Six, config);
    for batch in csv.flatten() {
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
    let res = unf_from_batch(column_hashes, &schema, config);
    dbg!(res);
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
