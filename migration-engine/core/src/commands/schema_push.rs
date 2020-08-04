use super::{CommandError, CommandResult, MigrationCommand};
use crate::{
    migrations_folder::{create_migration_folder, list_migrations},
    parse_datamodel,
};
use migration_connector::{
    DatabaseMigrationMarker, DatabaseMigrationStepApplier, DestructiveChangeDiagnostics, DestructiveChangesChecker,
    MigrationConnector,
};
use serde::{Deserialize, Serialize};

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

        // Create/overwrite the migration file, if the project is using migrations.
        if let Some(path) = input.migrations_folder_path.as_ref().map(std::path::Path::new) {
            let persistence = connector.migration_persistence();
            let filesystem_migrations = list_migrations(path).map_err(|err| CommandError::Generic(err.into()))?;
            let applied_migrations = persistence.load_all().await?;

            for (idx, filesystem_migration) in filesystem_migrations.iter().enumerate() {
                if let Some(applied_migration) = applied_migrations.get(idx) {
                    if !filesystem_migration.matches_applied_migration(applied_migration) {
                        todo!("Migrations don't match")
                    }
                } else {
                    let script = filesystem_migration
                        .read_migration_script()
                        .map_err(|err| CommandError::Generic(err.into()))?;

                    applier.apply_migration_script(&script).await?;
                }
            }

            let executed_steps = apply_to_dev_database(&database_migration, &checks, input, applier.as_ref()).await?;

            if !path.exists() {
                return Err(CommandError::Input(anyhow::anyhow!(
                    "The provided migrations folder path does not exist."
                )));
            }

            let (extension, script) = applier.render_migration_script(&database_migration);
            let folder = create_migration_folder(path, "draft").map_err(|err| CommandError::Generic(err.into()))?;

            folder
                .write_migration_script(&script, extension)
                .map_err(|err| CommandError::Generic(err.into()))?;

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
