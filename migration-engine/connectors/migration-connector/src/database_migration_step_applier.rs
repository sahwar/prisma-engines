use crate::*;
use serde::{Deserialize, Serialize};

/// Apply a single migration step to the connector's database. At this level, we are working with database migrations,
/// i.e. the [associated type on MigrationConnector](trait.MigrationConnector.html#associatedtype.DatabaseMigration).
#[async_trait::async_trait]
pub trait DatabaseMigrationStepApplier<T>: Send + Sync {
    /// Applies the step to the database
    /// Returns true to signal to the caller that the step was applied, and there could be a next one.
    async fn apply_step(&self, database_migration: &T, step: usize) -> ConnectorResult<bool>;

    /// Render steps for the CLI. Each step will contain the raw field.
    fn render_steps_pretty(&self, database_migration: &T) -> ConnectorResult<Vec<PrettyDatabaseMigrationStep>>;

    /// Render the steps as a full script. The return type should be read as
    /// (<file extension>, script)
    fn render_migration_script(
        &self,
        database_migration: &T,
        destructive_change_diagnostics: &DestructiveChangeDiagnostics,
    ) -> (&'static str, String);

    /// Apply a migration script passed with `render_migration_script`.
    async fn apply_migration_script(&self, script: &str, checksum: &[u8]) -> ConnectorResult<()>;

    /// Returns whether a database migration is empty.
    fn migration_is_empty(&self, migration: &T) -> bool;
}

/// A helper struct to serialize a database migration with an additional `raw` field containing the
/// rendered query string for that step.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrettyDatabaseMigrationStep {
    pub step: serde_json::Value,
    pub raw: String,
}
