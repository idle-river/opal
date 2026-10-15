#[derive(Debug, Clone)]
pub struct Task {
    pub name: String,
    pub depends: Vec<String>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub name: String,
    pub command: String,
}