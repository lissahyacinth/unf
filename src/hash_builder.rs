use std::sync::Arc;

use fasthash::murmur;

use crate::{
    config::{UnfConfig, UnfVersion},
    unf_vector::UNFVector,
};
use arrow::{
    array::{
        Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::Schema,
    record_batch::RecordBatch,
};
use base64::encode;
use md5::Md5;
use sha2::{Digest, Sha256};

#[derive(Debug)]
enum UnfHashers {
    FourPlus(Vec<Sha256>),
    ThreeMinus(Vec<Md5>),
}

struct HashIterator<T>(Vec<T>);

impl<T> Iterator for HashIterator<T>
where
    T: Iterator<Item = Vec<u8>>,
{
    type Item = Vec<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter_mut().map(Iterator::next).collect()
    }
}

#[derive(Debug)]
pub struct UnfHash {
    pub short_hash: String,
    pub hash: u32,
    pub version: UnfVersion,
}

pub struct UnfHashBuilder {
    schema: Arc<Schema>,
    version: UnfVersion,
    hash: UnfHashers,
    config: UnfConfig,
}

impl UnfHashBuilder {
    pub fn new(schema: Arc<Schema>, version: UnfVersion, config: UnfConfig) -> Self {
        let hash = match version {
            UnfVersion::Six => UnfHashers::FourPlus(
                (0..schema.fields().len())
                    .into_iter()
                    .map(|_| Sha256::new())
                    .collect::<Vec<Sha256>>(),
            ),
        };
        UnfHashBuilder {
            schema,
            version,
            hash,
            config,
        }
    }

    pub(crate) fn hash(&mut self, batch: RecordBatch) -> u32 {
        unf_batch(batch, &self.schema, self.config)
    }
}

/// Create a UNF Hash from a single Record Batch
pub fn unf_from_batch(input: RecordBatch, schema: &Arc<Schema>, config: UnfConfig) -> UnfHash {
    let hash = unf_batch(input, schema, config);
    let short_hash = "None".to_string();

    UnfHash {
        short_hash,
        hash,
        version: config.version,
    }
}

fn convert_col_to_raw<'a>(
    col: &'a dyn std::any::Any,
    column_index: usize,
    schema: &Arc<Schema>,
    config: UnfConfig,
) -> Box<dyn Iterator<Item = Vec<u8>> + 'a> {
    match schema.field(column_index).data_type() {
        arrow::datatypes::DataType::Null => todo!(),
        arrow::datatypes::DataType::Boolean => todo!(),
        arrow::datatypes::DataType::Int8 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int16 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int32 => col
            .downcast_ref::<Int32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Int64 => col
            .downcast_ref::<Int64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt8 => col
            .downcast_ref::<UInt16Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt16 => col
            .downcast_ref::<UInt16Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt32 => col
            .downcast_ref::<UInt32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::UInt64 => col
            .downcast_ref::<UInt64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float16 => col
            .downcast_ref::<Float32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float32 => col
            .downcast_ref::<Float32Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Float64 => col
            .downcast_ref::<Float64Array>()
            .expect("Failed to Downcast")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::Timestamp(_, _) => todo!(),
        arrow::datatypes::DataType::Date32 => todo!(),
        arrow::datatypes::DataType::Date64 => todo!(),
        arrow::datatypes::DataType::Time32(_) => todo!(),
        arrow::datatypes::DataType::Time64(_) => todo!(),
        arrow::datatypes::DataType::Duration(_) => todo!(),
        arrow::datatypes::DataType::Interval(_) => todo!(),
        arrow::datatypes::DataType::Binary => todo!(),
        arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
        arrow::datatypes::DataType::LargeBinary => todo!(),
        arrow::datatypes::DataType::Utf8 => col
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast Utf8 -> StringArray")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::LargeUtf8 => col
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast Utf8 -> StringArray")
            .raw(config.characters, config.digits),
        arrow::datatypes::DataType::List(_) => todo!(),
        arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
        arrow::datatypes::DataType::LargeList(_) => todo!(),
        arrow::datatypes::DataType::Struct(_) => todo!(),
        arrow::datatypes::DataType::Union(_) => todo!(),
        arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
        arrow::datatypes::DataType::Decimal(_, _) => todo!(),
    }
}

/// Update MurmurHash for a given RecordBatch
///
/// Suitable for UNF V4 and above, but not suitable for V3 which relies upon md5.
/// Assumes that the ordering of the hashes matches the ordering of the schemas.
pub(crate) fn unf_batch(input: RecordBatch, schema: &Arc<Schema>, config: UnfConfig) -> u32 {
    HashIterator(
        input
            .columns()
            .iter()
            .enumerate()
            .map(|(col_index, col)| convert_col_to_raw(col.as_any(), col_index, schema, config))
            .collect(),
    )
    .map(|row| murmur::hash32(row.into_iter().flatten().collect::<Vec<u8>>()))
    .reduce(|acc, x| acc ^ x)
    .unwrap()
}
