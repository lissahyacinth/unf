use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use hash_builder::{unf_from_batch, UnfHashBuilder};

pub mod config;
pub mod hash_builder;
mod unf_vector;
pub mod utils;

/// Calculate a UNF Hash for a given set of Records
pub fn calculate_unf<I>(
    mut unf_hash: UnfHashBuilder,
    batch_input: I,
    config: config::UnfConfig,
) -> hash_builder::UnfHash
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

mod tests {
    use crate::{config::UnfConfigBuilder, utils::read_csv_data};

    use super::*;

    #[test]
    fn load_float_from_file() {
        let file_path = "data/ExampleData.csv";
        let config = UnfConfigBuilder::new().build();
        let csv = read_csv_data(file_path.to_string(), 100);
        let mut unf_hash = UnfHashBuilder::new(csv.schema(), config::UnfVersion::Six, config);
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
