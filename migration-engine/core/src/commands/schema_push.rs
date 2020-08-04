use super::{CommandError, CommandResult, MigrationCommand};
use crate::{
    migrations_folder::{create_migration_folder, list_migrations},
    parse_datamodel,
};
use migration_connector::{
    DatabaseMigrationMarker, DatabaseMigrationStepApplier, DestructiveChangeDiagnostics, MigrationConnector,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};

pub struct SchemaPushCommand<'a> {
    pub input: &'a SchemaPushInput,
}

#[async_trait::async_trait]
impl<'a> MigrationCommand for SchemaPushCommand<'a> {
    type Input = SchemaPushInput;
    type Output = SchemaPushOutput;

    async fn execute<C, D>(
        input: &Self::Input,
        engine: &crate::migration_engine::MigrationEngine<C, D>,
    ) -> CommandResult<Self::Output>
    where
        C: MigrationConnector<DatabaseMigration = D>,
        D: DatabaseMigrationMarker + Send + Sync + 'static,
    {
        let connector = engine.connector();
        let schema = parse_datamodel(&input.schema)?;
        let inferrer = connector.database_migration_inferrer();
        let applier = connector.database_migration_step_applier();
        let checker = connector.destructive_change_checker();

        let database_migration = inferrer.infer(&schema, &schema, &[]).await?;
        let checks = checker.check(&database_migration).await?;

        if applier.migration_is_empty(&database_migration) {
            tracing::info!("The generated database migration is empty.");
            return Ok(SchemaPushOutput {
                executed_steps: 0,
                warnings: Vec::new(),
                unexecutable: Vec::new(),
            });
        }

        // Create/overwrite the migration file, if the project is using migrations.
        if let Some(path) = input.migrations_folder_path.as_ref().map(std::path::Path::new) {
            let filesystem_migrations = list_migrations(path).map_err(|err| CommandError::Generic(err.into()))?;
            let applied_migrations = connector.read_imperative_migrations().await?;

            for (idx, filesystem_migration) in filesystem_migrations.iter().enumerate() {
                if let Some(applied_migration) = applied_migrations.get(idx) {
                    if !filesystem_migration
                        .matches_applied_migration(applied_migration)
                        .map_err(|err| CommandError::Generic(err.into()))?
                    {
                        return Err(CommandError::Generic(anyhow::anyhow!("Migration `{filesystem_migration}` does not match the `{applied_migration}` migration applied in the database.", filesystem_migration = filesystem_migration.migration_id(), applied_migration = applied_migration.name)));
                    }
                } else {
                    tracing::debug!("Applying saved migration `{}`", filesystem_migration.migration_id());
                    let script = filesystem_migration
                        .read_migration_script()
                        .map_err(|err| CommandError::Generic(err.into()))?;

                    applier.apply_migration_script(&script).await?;
                }
            }

            let (extension, script) = applier.render_migration_script(&database_migration);
            let executed_steps = apply_to_dev_database(&database_migration, &checks, input, applier.as_ref()).await?;

            if !path.exists() {
                return Err(CommandError::Input(anyhow::anyhow!(
                    "The provided migrations folder path does not exist."
                )));
            }

            let folder = create_migration_folder(path, "draft").map_err(|err| CommandError::Generic(err.into()))?;

            folder
                .write_migration_script(&script, extension)
                .map_err(|err| CommandError::Generic(err.into()))?;

            let mut hasher = Sha512::new();
            hasher.update(&script);
            let checksum = hasher.finalize();
            connector
                .persist_imperative_migration("draft", checksum.as_ref(), &script)
                .await?;

            Ok(SchemaPushOutput {
                executed_steps,
                warnings: checks.warnings.into_iter().map(|warning| warning.description).collect(),
                unexecutable: checks
                    .unexecutable_migrations
                    .into_iter()
                    .map(|unexecutable| unexecutable.description)
                    .collect(),
            })
        } else {
            // Otherwise only apply the migration.
            let executed_steps = apply_to_dev_database(&database_migration, &checks, input, applier.as_ref()).await?;

            Ok(SchemaPushOutput {
                executed_steps,
                warnings: checks.warnings.into_iter().map(|warning| warning.description).collect(),
                unexecutable: checks
                    .unexecutable_migrations
                    .into_iter()
                    .map(|unexecutable| unexecutable.description)
                    .collect(),
            })
        }
    }
}

async fn apply_to_dev_database<D: DatabaseMigrationMarker>(
    database_migration: &D,
    checks: &DestructiveChangeDiagnostics,
    input: &SchemaPushInput,
    applier: &dyn DatabaseMigrationStepApplier<D>,
) -> CommandResult<u32> {
    let mut step = 0u32;

    match (checks.unexecutable_migrations.len(), checks.warnings.len(), input.force) {
        (unexecutable, _, _) if unexecutable > 0 => {
            tracing::warn!(unexecutable = ?checks.unexecutable_migrations, "Aborting migration because at least one unexecutable step was detected.")
        }
        (0, 0, _) | (0, _, true) => {
            while applier.apply_step(&database_migration, step as usize).await? {
                step += 1
            }
        }
        _ => tracing::info!(
            "The migration was not applied because it triggered warnings and the force flag was not passed."
        ),
    }

    Ok(step)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaPushInput {
    /// The prisma schema.
    pub schema: String,
    /// Push the schema ignoring destructive change warnings.
    pub force: bool,
    /// The path to the migrations folder, in case the the project is using migrations.
    #[serde(default)]
    pub migrations_folder_path: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaPushOutput {
    pub executed_steps: u32,
    pub warnings: Vec<String>,
    pub unexecutable: Vec<String>,
}

impl SchemaPushOutput {
    pub fn had_no_changes_to_push(&self) -> bool {
        self.warnings.is_empty() && self.unexecutable.is_empty() && self.executed_steps == 0
    }
}
