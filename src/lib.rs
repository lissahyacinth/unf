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
) -> u32
where
    I: Iterator<Item = RecordBatch>,
{
    let res = batch_input
        .into_iter()
        .map(|batch| unf_hash.hash(batch))
        .reduce(|acc, x| acc ^ x)
        .unwrap();
    res
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
        let mut batch_hashes: Vec<u32> = Vec::new();
        for batch in csv {
            if let Ok(batch) = batch {
                batch_hashes.push(unf_hash.hash(batch));
            }
        }
        dbg!(batch_hashes.into_iter().reduce(|acc, x| acc ^ x));
    }
}
