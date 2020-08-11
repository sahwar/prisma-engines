use super::{CommandError, CommandResult, MigrationCommand};
use crate::{
    migrations_folder::{create_migration_folder, list_migrations, MigrationFolder},
    parse_datamodel,
};
use migration_connector::{
    DatabaseMigrationMarker, DatabaseMigrationStepApplier, DestructiveChangeDiagnostics, ImperativeMigration,
    MigrationConnector,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::{io, path::Path};

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

        // Create/overwrite the migration file, if the project is using migrations.
        if let Some(path) = input.migrations_folder_path.as_ref().map(std::path::Path::new) {
            catch_up(path, input.force, connector).await?;

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

            let (extension, script) = applier.render_migration_script(&database_migration);

            let folder = create_migration_folder(path, "draft").map_err(|err| CommandError::Generic(err.into()))?;

            folder
                .write_migration_script(&script, extension)
                .map_err(|err| CommandError::Generic(err.into()))?;

            tracing::debug!("Applying new migration `{}`", folder.migration_id());
            applier.apply_migration_script(&script).await?;

            if !path.exists() {
                return Err(CommandError::Input(anyhow::anyhow!(
                    "The provided migrations folder path does not exist."
                )));
            }

            let mut hasher = Sha512::new();
            hasher.update(&script);
            let checksum = hasher.finalize();
            connector
                .persist_imperative_migration("draft", checksum.as_ref(), &script)
                .await?;

            Ok(SchemaPushOutput {
                executed_steps: 1,
                warnings: checks.warnings.into_iter().map(|warning| warning.description).collect(),
                unexecutable: checks
                    .unexecutable_migrations
                    .into_iter()
                    .map(|unexecutable| unexecutable.description)
                    .collect(),
            })
        } else {
            // Otherwise only apply the migration.

            let database_migration = inferrer.infer(&schema, &schema, &[]).await?;
            let checks = checker.check(&database_migration).await?;

            if applier.migration_is_empty(&database_migration) {
                tracing::info!("The generated database migration is empty.");
            }

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

/// Catch up the dev database with the migrations history.
///
/// Cases:
///
/// - The database is up-to-date: do nothing
/// - The database is on track but behind: apply migrations from the migrations
///   folder that are not applied
/// - The migrations in the database and in the folder diverge:
///   1. Create temporary database and bring it to canonical migrations folder
///      end-state
///   2. Migrate dev database to that state, with minimal steps.
///
///   Alternatively (more data-lossy, but also sticking closer to the migrations
///   history):
///
///   1. Revert database schema to the migration where they diverge
///   2. Apply migrations folder history from there
async fn catch_up<C, D>(migrations_folder_path: &Path, force: bool, connector: &C) -> CommandResult<()>
where
    C: MigrationConnector<DatabaseMigration = D>,
    D: DatabaseMigrationMarker + 'static,
{
    let applier = connector.database_migration_step_applier();

    let filesystem_migrations =
        list_migrations(migrations_folder_path).map_err(|err| CommandError::Generic(err.into()))?;
    let applied_migrations = connector.read_imperative_migrations().await?;
    let diagnostic = diagnose_migrations_history(&filesystem_migrations, &applied_migrations)
        .map_err(|err| CommandError::Generic(err.into()))?;

    match diagnostic {
        HistoryDiagnostic::UpToDate => (),
        HistoryDiagnostic::DatabaseIsBehind { unapplied_migrations } => {
            for filesystem_migration in unapplied_migrations {
                tracing::debug!("Applying saved migration `{}`", filesystem_migration.migration_id());
                let script = filesystem_migration
                    .read_migration_script()
                    .map_err(|err| CommandError::Generic(err.into()))?;

                applier.apply_migration_script(&script).await?;
            }
        }
        HistoryDiagnostic::FilesystemIsBehind { unpersisted_migrations } => {
            tracing::info!(
                ?unpersisted_migrations,
                "The filesystem migrations are behind the migrations applied to the database."
            );

            if force {
                tracing::warn!("Rolling back applied migrations that do not appear in the filesystem.");
                let fs_migration_scripts: Vec<String> = filesystem_migrations.iter().map(|_| todo!()).collect();
                connector
                    .revert_to(&fs_migration_scripts, unpersisted_migrations)
                    .await?;
            }
        }
        HistoryDiagnostic::HistoriesDiverge {
            last_applied_filesystem_migration: _,
        } => todo!("diverging histories"),
    }

    Ok(())
}

#[derive(Debug)]
enum HistoryDiagnostic<'a> {
    UpToDate,
    DatabaseIsBehind {
        unapplied_migrations: &'a [MigrationFolder],
    },
    FilesystemIsBehind {
        unpersisted_migrations: &'a [ImperativeMigration],
    },
    HistoriesDiverge {
        last_applied_filesystem_migration: Option<usize>,
    },
}

fn diagnose_migrations_history<'a>(
    filesystem_migrations_slice: &'a [MigrationFolder],
    applied_migrations_slice: &'a [ImperativeMigration],
) -> io::Result<HistoryDiagnostic<'a>> {
    let mut filesystem_migrations = filesystem_migrations_slice.iter().enumerate();
    let mut applied_migrations = applied_migrations_slice.iter().enumerate();
    let mut last_applied_filesystem_migration: Option<usize> = None;
    let mut checksum_buf = Vec::with_capacity(6);

    while let Some((fs_idx, fs_migration)) = filesystem_migrations.next() {
        fs_migration.checksum(&mut checksum_buf)?;

        match next_applied_migration(&mut applied_migrations) {
            Some(applied_migration) if applied_migration.checksum == checksum_buf => {
                last_applied_filesystem_migration = Some(fs_idx);
            }
            Some(_applied_migration) => {
                return Ok(HistoryDiagnostic::HistoriesDiverge {
                    last_applied_filesystem_migration,
                })
            }
            None => {
                return Ok(HistoryDiagnostic::DatabaseIsBehind {
                    unapplied_migrations: &filesystem_migrations_slice[fs_idx..],
                })
            }
        }
    }

    let next_applied_migration_idx: Option<usize> = applied_migrations.next().map(|(idx, _)| idx);

    if let Some(idx) = next_applied_migration_idx {
        return Ok(HistoryDiagnostic::FilesystemIsBehind {
            unpersisted_migrations: &applied_migrations_slice[idx..],
        });
    }

    Ok(HistoryDiagnostic::UpToDate)
}

/// Returns the next applied migration if there is one.
fn next_applied_migration<'a>(
    applied_migrations: &mut impl Iterator<Item = (usize, &'a ImperativeMigration)>,
) -> Option<&'a ImperativeMigration> {
    loop {
        let next_migration = applied_migrations.next().map(|(_, m)| m)?;

        if next_migration.is_applied() {
            return Some(next_migration);
        }
    }
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
