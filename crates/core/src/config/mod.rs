use crate::{Error, Task};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug)]
pub struct Program {
    pub name: String,
    pub jobs: Vec<Task>,
    pub depends: Vec<String>, // Other Programs
}

impl Program {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Program, Error> {
        use std::fs;
        let contents = fs::read_to_string(path)?;
        let yaml: Program = serde_yaml::from_str(&contents)?;
        Ok(yaml)
    }
}
