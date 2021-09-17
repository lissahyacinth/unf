use std::sync::Arc;

use crate::{
    config::{UnfConfig, UnfVersion},
    unf_vector::UNFVector,
};
use arrow::{
    array::{
        Float32Array, Float64Array, Int32Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
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

#[derive(Debug)]
pub struct UnfHash {
    pub short_hash: String,
    pub hash: Vec<u8>,
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

    pub(crate) fn hash(&mut self, batch: RecordBatch) -> &Self {
        match self.version {
            UnfVersion::Six => match self.hash {
                UnfHashers::FourPlus(ref mut hashers) => {
                    unf_batch(batch, &self.schema, self.config, hashers);
                }
                UnfHashers::ThreeMinus(_) => unreachable!(),
            },
        };
        self
    }

    pub(crate) fn finalize(self) -> Vec<UnfHash> {
        let truncation = self.config.truncation / 8;
        let version = self.version;
        match self.hash {
            UnfHashers::FourPlus(hash) => hash
                .into_iter()
                .map(|x| {
                    let output: Vec<u8> = x.finalize().to_vec();
                    let short_hash = encode(
                        output
                            .clone()
                            .into_iter()
                            .take(truncation)
                            .collect::<Vec<u8>>(),
                    );
                    UnfHash {
                        short_hash,
                        hash: output,
                        version,
                    }
                })
                .collect(),
            UnfHashers::ThreeMinus(_) => todo!(),
        }
    }
}

/// Create a UNF Hash from a single Record Batch
pub fn unf_from_batch(input: RecordBatch, schema: &Arc<Schema>, config: UnfConfig) -> UnfHash {
    let mut hasher: Vec<Sha256> = vec![Sha256::new()];

    unf_batch(input, schema, config, &mut hasher);
    let hash = hasher.pop().unwrap().finalize().to_vec();
    let short_hash = encode(
        hash.clone()
            .into_iter()
            .take(config.truncation / 8)
            .collect::<Vec<u8>>(),
    );

    UnfHash {
        short_hash,
        hash,
        version: config.version,
    }
}

/// Update SHA256 Hashes for a given RecordBatch
///
/// Suitable for UNF V4 and above, but not suitable for V3 which relies upon md5.
/// Assumes that the ordering of the hashes matches the ordering of the schemas.
pub(crate) fn unf_batch(
    input: RecordBatch,
    schema: &Arc<Schema>,
    config: UnfConfig,
    hash: &mut Vec<Sha256>,
) {
    for (column_index, column) in input.columns().iter().enumerate() {
        let col = column.as_any();
        let raw_column_data = match schema.field(column_index).data_type() {
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
                .downcast_ref::<Int32Array>()
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
        };
        let hasher = &mut hash[column_index];
        for x in raw_column_data {
            hasher.update(x)
        }
    }
}
