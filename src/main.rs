use clap::{App, Arg};

use unfhash::calculate_unf;
use unfhash::config::{UnfConfigBuilder, UnfVersion};
use unfhash::hash_builder::UnfHashBuilder;
use unfhash::utils::read_csv_data;

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
        .arg(
            Arg::with_name("inference_rows")
                .short("r")
                .value_name("INFERENCE_ROWS")
                .default_value("100")
                .takes_value(true),
        )
        .get_matches();
    let file_path = matches.value_of("input_file").unwrap();
    let truncation = matches.value_of("truncation").unwrap();
    let digits = matches.value_of("digits").unwrap();
    let characters = matches.value_of("characters").unwrap();
    let inference_rows: usize = matches.value_of("inference_rows").unwrap().parse().unwrap();
    let config = UnfConfigBuilder::new()
        .truncation(truncation.parse().unwrap())
        .digits(digits.parse().unwrap())
        .characters(characters.parse().unwrap())
        .build();
    let csv = read_csv_data(file_path.to_string(), inference_rows);
    let unf_hash = UnfHashBuilder::new(csv.schema(), UnfVersion::Six, config);
    let res = calculate_unf(unf_hash, csv.flatten(), config);
    println!(
        "File: {} | UNF Version: {:?} | ShortHash: {}",
        file_path, config.version, res.short_hash
    );
}
