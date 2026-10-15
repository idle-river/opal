#[derive(Debug, Clone)]
#[cfg_attr(feature = "config", derive(serde::Serialize, serde::Deserialize))]
pub struct Task {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "config", derive(serde::Serialize, serde::Deserialize))]
pub struct Step {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
}

pub type Error = Box<dyn std::error::Error>;

#[cfg(feature = "engine")]
pub mod engine;

#[cfg(feature = "config")]
pub mod config;
