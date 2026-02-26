use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// Load cursor string from a file. Returns `None` if the file does not exist
/// or is empty.
pub fn load_cursor(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(content) => {
            let cursor = content.trim().to_string();
            if cursor.is_empty() {
                info!(path = %path.display(), "cursor file is empty, starting fresh");
                None
            } else {
                info!(path = %path.display(), cursor = %cursor, "loaded cursor from file");
                Some(cursor)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %path.display(), "cursor file not found, starting fresh");
            None
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to read cursor file, starting fresh");
            None
        }
    }
}

/// Save cursor string to a file. Creates parent directories if needed.
pub fn save_cursor(path: &Path, cursor: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, cursor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_cursor_missing_file() {
        let result = load_cursor(Path::new("/tmp/nonexistent_cursor_file_12345"));
        assert!(result.is_none());
    }

    #[test]
    fn test_load_cursor_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "").unwrap();
        assert!(load_cursor(&path).is_none());
    }

    #[test]
    fn test_load_cursor_whitespace_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "  \n  ").unwrap();
        assert!(load_cursor(&path).is_none());
    }

    #[test]
    fn test_load_and_save_cursor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");

        save_cursor(&path, "abc123").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("abc123"));
    }

    #[test]
    fn test_save_cursor_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("dir").join("cursor");

        save_cursor(&path, "xyz789").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("xyz789"));
    }

    #[test]
    fn test_save_cursor_overwrites() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");

        save_cursor(&path, "first").unwrap();
        save_cursor(&path, "second").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("second"));
    }

    #[test]
    fn test_load_cursor_trims_whitespace() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        fs::write(&path, "  abc123\n").unwrap();
        assert_eq!(load_cursor(&path).as_deref(), Some("abc123"));
    }

    #[test]
    fn test_save_cursor_to_bare_filename() {
        // When path has no parent dir component, save should still work
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cursor");
        save_cursor(&path, "val").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "val");
    }
}
