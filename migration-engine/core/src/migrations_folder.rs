//! This module is responsible for the management of the contents of the
//! migrations folder. The migrations folder contains multiple migration
//! folders, named after the migration id, and each containing:
//!
//! - A migration script

use std::{
    fs::{create_dir, read_dir, DirEntry},
    io,
    path::{Path, PathBuf},
};

/// The file name for migration scripts, not including the file extension.
pub const MIGRATION_SCRIPT_FILENAME: &str = "migration";

/// Create a folder for a new migration.
pub(crate) fn create_migration_folder(migrations_folder_path: &Path, migration_name: &str) -> io::Result<PathBuf> {
    let timestamp = chrono::Utc::now().format("%Y%m%d%M%S");
    let folder_name = format!(
        "{timestamp}_{migration_name}",
        timestamp = timestamp,
        migration_name = migration_name
    );
    let folder_path = migrations_folder_path.join(folder_name);

    if folder_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            anyhow::anyhow!(
                "The migration folder already exists at {}",
                folder_path.to_string_lossy()
            ),
        ));
    }

    create_dir(&folder_path)?;

    Ok(folder_path)
}

/// List the migrations present in the migration folder.
pub(crate) fn list_migrations(
    migrations_folder_path: &Path,
) -> io::Result<impl Iterator<Item = io::Result<MigrationFolder>>> {
    let entries = read_dir(migrations_folder_path)?.filter_map(|entry_result| match entry_result {
        Ok(entry) if entry.file_type().ok()?.is_dir() => Some(Ok(MigrationFolder(entry))),
        Ok(_entry) => None,
        Err(err) => Some(Err(err)),
    });

    Ok(entries)
}

/// Proxy to a folder containing one migration, as returned by `list_migrations`.
pub(crate) struct MigrationFolder(DirEntry);
