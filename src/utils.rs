use std::fs::File;

use arrow::csv;

pub fn read_csv_data(file_path: String, lines_for_type_inference: usize) -> csv::Reader<File> {
    let file = File::open(file_path).unwrap();
    let builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(lines_for_type_inference));
    builder.build(file).unwrap()
}
