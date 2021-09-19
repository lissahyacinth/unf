use arrow::array::{
    Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array,
};
use std::{
    convert::TryFrom,
    fmt::{self},
};

pub trait UNFVector {
    fn raw<'a>(&'a self, characters: usize, digits: u32) -> Box<dyn Iterator<Item = Vec<u8>> + 'a> {
        let unf_digits = self.to_unf(digits);
        Box::new(unf_digits.into_iter().map(move |x| {
            let mut characters = x.chars().take(characters).collect::<Vec<char>>();
            characters.push('\n');
            characters.push('\x00');
            characters
                .into_iter()
                .collect::<String>()
                .as_bytes()
                .to_vec()
        }))
    }
    fn to_unf<'a>(&'a self, _digits: u32) -> Box<dyn Iterator<Item = String> + 'a>;
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
    fn to_unf<'a>(&'a self, digits: u32) -> Box<dyn Iterator<Item = String> + 'a> {
        Box::new(
            self.values()
                .iter()
                .map(move |x| exp_form(sigfig(*x, digits - 1))),
        )
    }
}

impl UNFVector for Float32Array {
    fn to_unf<'a>(&'a self, digits: u32) -> Box<dyn Iterator<Item = String> + 'a> {
        Box::new(
            self.values()
                .iter()
                .map(move |x| exp_form(sigfig(*x as f64, digits - 1))),
        )
    }
}

macro_rules! integer_unf {
    ($array_type: ident) => {
        impl UNFVector for $array_type {
            fn to_unf<'a>(&'a self, _digits: u32) -> Box<dyn Iterator<Item = String> + 'a> {
                Box::new(self.values().iter().map(exp_form))
            }
        }
    };
}

integer_unf!(Int16Array);
integer_unf!(Int32Array);
integer_unf!(Int64Array);

integer_unf!(UInt16Array);
integer_unf!(UInt32Array);
integer_unf!(UInt64Array);

impl UNFVector for StringArray {
    fn to_unf<'a>(&'a self, _digits: u32) -> Box<dyn Iterator<Item = String> + 'a> {
        Box::new((0..self.len()).map(move |x| self.value(x).to_string()))
    }
}
