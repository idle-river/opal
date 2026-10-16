use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

pub fn input_fingerprint(root: &Path, inputs: &[PathBuf], config: &[u8]) -> io::Result<String> {
    let mut files = Vec::new();

    // Expand input directories into individual files.
    for input in inputs {
        let path = root.join(input);

        if path.is_file() {
            files.push(path);
        } else if path.is_dir() {
            for entry in WalkDir::new(&path).follow_links(false) {
                let entry = entry.map_err(io::Error::other)?;

                if entry.file_type().is_file() {
                    files.push(entry.into_path());
                }
            }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Input does not exist: {}", path.display()),
            ));
        }
    }

    // Sort and deduplicate for deterministic results.
    files.sort();
    files.dedup();

    let mut hasher = blake3::Hasher::new();

    // Include job configuration in the fingerprint.
    hasher.update(&(config.len() as u64).to_le_bytes());
    hasher.update(config);

    for file_path in files {
        let relative = file_path.strip_prefix(root).map_err(io::Error::other)?;

        // Include the relative path, not just the file contents.
        let path = relative.to_string_lossy();
        hasher.update(&(path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());

        // Hash file contents in chunks instead of loading
        // the entire file into memory.
        let mut file = File::open(&file_path)?;
        let mut buffer = [0u8; 8192];

        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }

            hasher.update(&buffer[..count]);
        }
    }

    Ok(hasher.finalize().to_hex().to_string())
}
