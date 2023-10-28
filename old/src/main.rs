use std::{
    env,
    error::Error,
    ffi::OsString,
    fs::File,
    process,
};
Date      Time   FS   VA     VB     VC     VD      U      V      W      T       P     MX    MY  Pitch  Roll
MM DD YYYY HH MM SS.SS  cm/s   cm/s   cm/s   cm/s   cm/s   cm/s   cm/s   degC    dbar               deg    deg
#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "DateMM")]
    date: u64,
    #[serde(rename = "DateDD")]
    longitude: f64,
    #[serde(rename = "DateYYYY")]
    population: Option<u64>,
    #[serde(rename = "City")]
    city: String,
    #[serde(rename = "State")]
    state: String,
}

fn run() -> Result<(), Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        let record = result?;
        println!("{:?}", record);
    }
    Ok(())
}

/// Returns the first positional argument sent to this process. If there are no
/// positional arguments, then this returns an error.
fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}