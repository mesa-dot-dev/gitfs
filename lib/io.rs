//! Random IO utilities

use std::path::Path;

/// Remove all files and directories in the given directory, but not the directory itself.
pub async fn remove_dir_contents(path: &Path) -> std::io::Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if tokio::fs::metadata(&path).await?.is_dir() {
            tokio::fs::remove_dir_all(path).await?;
        } else {
            tokio::fs::remove_file(path).await?;
        }
    }
    Ok(())
}
