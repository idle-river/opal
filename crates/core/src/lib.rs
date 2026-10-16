use std::collections::HashMap;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "config", derive(serde::Serialize, serde::Deserialize))]
pub struct Task {
    pub name: String,
    pub envs: Option<HashMap<String, String>>,
    pub working_dir: Option<String>,
    #[cfg_attr(feature = "caching", serde(default))]
    pub cache: Option<CacheOptions>,
    #[cfg_attr(feature = "config", serde(default))]
    pub depends: Vec<String>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "config", derive(serde::Serialize, serde::Deserialize))]
pub struct CacheOptions {
    inputs: Vec<String>, // Regex patterns (hopefully soon)
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "config", derive(serde::Serialize, serde::Deserialize))]
pub struct Step {
    pub name: String,
    pub command: String,
    pub args: Option<Vec<String>>,
}

pub type Error = Box<dyn std::error::Error>;

#[cfg(feature = "engine")]
pub mod engine;

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "caching")]
pub mod caching;
