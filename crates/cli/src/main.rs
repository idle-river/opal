use opal_core::Task;

#[tokio::main]
async fn main() {
    println!("{:#?}", Task { name: "Test".to_string(), depends: vec![], steps: vec![] });
}
