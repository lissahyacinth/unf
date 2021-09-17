use arrow::array::{Array, Float32Array, Float64Array, StringArray};
use std::{
    convert::TryFrom,
    fmt::{self},
};

pub trait UNFVector {
    fn raw(&self, characters: usize, digits: u32) -> Vec<Vec<u8>> {
        self.to_unf(digits)
            .iter()
            .map(|x| {
                let mut characters = x.chars().take(characters).collect::<Vec<char>>();
                characters.push('\n');
                characters.push('\x00');
                characters
                    .into_iter()
                    .collect::<String>()
                    .as_bytes()
                    .to_vec()
            })
            .collect()
    }
    fn to_unf(&self, digits: u32) -> Vec<String>;
}

fn exp_form<T>(value: T) -> String
where
    T: fmt::Debug + fmt::LowerExp,
{
    let string_rep = format!("{:+e}", value);
    let (pre, post) = string_rep.split_once('e').unwrap();
    let mut formatted_post = post.to_string();
    if post.starts_with('0') {
        formatted_post = vec!['+'].into_iter().chain(post.chars().skip(1)).collect();
    }
    if pre.len() == 2 {
        format!("{}.e{}", pre, formatted_post)
    } else {
        format!("{}e{}", pre, formatted_post)
    }
}

fn sigfig<T>(x: T, digits: u32) -> T
where
    T: num_traits::float::Float + TryFrom<u32> + TryFrom<f32>,
    <T as TryFrom<f32>>::Error: std::fmt::Debug,
    <T as TryFrom<u32>>::Error: std::fmt::Debug,
{
    let reduction: T = x.log10();
    let digit_modifier = if reduction < T::zero() {
        reduction.abs().ceil()
    } else {
        T::zero()
    };

    let digits: T = TryFrom::try_from(digits).unwrap();
    let base_modifier: T = TryFrom::try_from(10.0_f32).unwrap();
    let rounding_factor = base_modifier.powf(digits + digit_modifier);
    (x * rounding_factor).round() / rounding_factor
}

impl UNFVector for Float64Array {
    fn to_unf(&self, digits: u32) -> Vec<String> {
        if self.null_count() == 0 {
            self.values()
                .iter()
                .map(|x| exp_form(sigfig(*x, digits - 1)))
                .collect()
        } else {
            let mut out: Vec<String> = Vec::with_capacity(self.len());
            for index in 0..self.len() {
                if self.is_null(index) {
                    out.push("+nan".to_string())
                } else {
                    out.push(exp_form(sigfig(self.value(index), digits - 1)))
                }
            }
            out
        }
    }
}

impl UNFVector for Float32Array {
    fn to_unf(&self, digits: u32) -> Vec<String> {
        let scaling_factor = 10_u32.pow(digits) as f32;
        if self.null_count() == 0 {
            self.values()
                .iter()
                .map(|x| exp_form((x * scaling_factor).round() / scaling_factor))
                .collect()
        } else {
            let mut out: Vec<String> = Vec::with_capacity(self.len());
            for index in 0..self.len() {
                if self.is_null(index) {
                    out.push("+nan".to_string())
                } else {
                    out.push(exp_form(
                        (self.value(index) * scaling_factor).round() / scaling_factor,
                    ));
                }
            }
            out
        }
    }
}

impl UNFVector for StringArray {
    fn to_unf(&self, _digits: u32) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            if self.is_null(index) {
                out.push("".to_string())
            } else {
                out.push(self.value(index).to_string());
            }
        }
        out
    }
}

mod tests {
    use std::sync::Arc;

    use crate::{unf::unf_from_batch, unf_config::UnfConfigBuilder};

    use super::*;
    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    #[test]
    fn float64_unf() {
        assert_eq!(
            Float64Array::from(vec![
                0.107352613238618,
                0.0139041461516172,
                0.573460578685626,
                0.883383935317397,
                0.698766667861491,
                0.745090883225203,
                0.234602871118113,
                0.724982571555302,
                0.977011684328318,
                0.358048414811492
            ])
            .to_unf(7),
            vec![
                "+1.073526e-1",
                "+1.390415e-2",
                "+5.734606e-1",
                "+8.833839e-1",
                "+6.987667e-1",
                "+7.450909e-1",
                "+2.346029e-1",
                "+7.249826e-1",
                "+9.770117e-1",
                "+3.580484e-1"
            ]
        );
    }

    #[test]
    fn float64_truncate() {
        assert_eq!(
            Float64Array::from(vec![0.943062649108469, 0.852143662748858]).to_unf(7),
            vec!["+9.430626e-1", "+8.521437e-1"]
        );
    }

    #[test]
    fn float32_raw() {
        assert_eq!(
            Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])
                .raw(128, 7)
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>(),
            vec![
                43, 49, 46, 101, 43, 10, 0, 43, 50, 46, 101, 43, 10, 0, 43, 51, 46, 101, 43, 10, 0,
                43, 52, 46, 101, 43, 10, 0
            ]
        );
    }

    #[test]
    fn float32_array() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "FloatTest",
            DataType::Float32,
            false,
        )]));
        let config = UnfConfigBuilder::new().build();
        let data_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]))],
        )
        .unwrap();
        let res = unf_from_batch(data_batch, &schema, config);
        assert_eq!(res.short_hash, "aWgJoh/Y7/Qo6uK9zs7ovQ==");
    }
}
