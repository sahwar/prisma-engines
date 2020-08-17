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
            tracing::debug!("Catching up the database");
            match catch_up(path, input.force, connector).await? {
                (Some(radical_measure), _) => {
                    return Ok(SchemaPushOutput {
                        executed_steps: 0,
                        warnings: Vec::new(),
                        unexecutable: Vec::new(),
                        radical_measure: Some(radical_measure),
                    });
                }
                (_, false) => {
                    return Ok(SchemaPushOutput {
                        executed_steps: 0,
                        warnings: Vec::new(),
                        unexecutable: Vec::new(),
                        radical_measure: None,
                    })
                }
                _ => tracing::debug!("The database is caught up."),
            }

            let database_migration = inferrer.infer(&schema, &schema, &[]).await?;
            let checks = checker.check(&database_migration).await?;
            let pure_checks = checker.pure_check(&database_migration);

            if applier.migration_is_empty(&database_migration) {
                tracing::info!("The generated database migration is empty.");
                return Ok(SchemaPushOutput {
                    executed_steps: 0,
                    warnings: Vec::new(),
                    unexecutable: Vec::new(),
                    radical_measure: None,
                });
            }

            if !should_apply(&checks, input) {
                return Ok(SchemaPushOutput {
                    executed_steps: 0,
                    warnings: Vec::new(),
                    unexecutable: Vec::new(),
                    radical_measure: None,
                });
            }

            let (extension, script) = applier.render_migration_script(&database_migration, &pure_checks);

            if !path.exists() {
                return Err(CommandError::Input(anyhow::anyhow!(
                    "The provided migrations folder path does not exist."
                )));
            }

            let folder = create_migration_folder(path, "draft").map_err(|err| CommandError::Generic(err.into()))?;

            folder
                .write_migration_script(&script, extension)
                .map_err(|err| CommandError::Generic(err.into()))?;

            // Stop here and do not apply the migration if we are in draft mode.
            if input.draft {
                tracing::info!("Draft migration was saved!");
                return Ok(SchemaPushOutput {
                    executed_steps: 0,
                    warnings: Vec::new(),
                    unexecutable: Vec::new(),
                    radical_measure: None,
                });
            }

            tracing::debug!("Applying new migration `{}`", folder.migration_id());
            let mut hasher = Sha512::new();
            hasher.update(&script);
            let checksum = hasher.finalize();

            connector
                .persist_imperative_migration_to_table(folder.migration_id(), checksum.as_ref(), &script)
                .await?;
            applier.apply_migration_script(&script, &checksum).await?;

            Ok(SchemaPushOutput {
                executed_steps: 1,
                warnings: checks.warnings.into_iter().map(|warning| warning.description).collect(),
                unexecutable: checks
                    .unexecutable_migrations
                    .into_iter()
                    .map(|unexecutable| unexecutable.description)
                    .collect(),
                radical_measure: None,
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
                radical_measure: None,
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

    if should_apply(checks, input) {
        while applier.apply_step(&database_migration, step as usize).await? {
            step += 1
        }
    }

    Ok(step)
}

fn should_apply(checks: &DestructiveChangeDiagnostics, input: &SchemaPushInput) -> bool {
    match (
        checks.unexecutable_migrations.len(),
        checks.warnings.len(),
        input.accept_data_loss,
    ) {
        (unexecutable, _, _) if unexecutable > 0 => {
            tracing::warn!(unexecutable = ?checks.unexecutable_migrations, "Aborting migration because at least one unexecutable step was detected.");
            false
        }
        (0, 0, _) | (0, _, true) => true,
        _ => {
            tracing::info!(
            "The migration was not applied because it triggered warnings and the --accept-data-loss flag was not passed."
        );
            false
        }
    }
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
#[tracing::instrument(skip(connector))]
async fn catch_up<C, D>(
    migrations_folder_path: &Path,
    force: bool,
    connector: &C,
) -> CommandResult<(Option<String>, bool)>
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
    let fs_migration_scripts: Vec<String> = filesystem_migrations
        .iter()
        .map(|folder| {
            folder
                .read_migration_script()
                .expect("Failed to read migration script.")
        })
        .collect();

    tracing::debug!(diagnostic = diagnostic.short());

    match diagnostic {
        HistoryDiagnostic::UpToDate => match connector.detect_drift(&fs_migration_scripts).await? {
            Some(drift) => {
                tracing::warn!("Detected drift between dev database and migrations history.");
                if force {
                    tracing::warn!(
                        "The force flag was passed, migrating the dev database back on track with the migrations history."
                    );

                    let mut step = 0;

                    while applier.apply_step(&drift, step).await? {
                        step += 1;
                    }
                } else {
                    tracing::warn!("The database will be brought back on track with the migrations history.");

                    return Ok((
                        Some("Detected drift between dev database and migrations history. Bring them back into sync? (this may imply data loss)".into()),
                        false
                    )
                    );
                }
            }
            None => (),
        },
        HistoryDiagnostic::DatabaseIsBehind { unapplied_migrations } => {
            for filesystem_migration in unapplied_migrations {
                tracing::debug!("Applying saved migration `{}`", filesystem_migration.migration_id());
                let script = filesystem_migration
                    .read_migration_script()
                    .map_err(|err| CommandError::Generic(err.into()))?;

                let mut hasher = Sha512::new();
                hasher.update(&script);
                let checksum = hasher.finalize();

                connector
                    .persist_imperative_migration_to_table(filesystem_migration.migration_id(), &checksum, &script)
                    .await?;

                applier.apply_migration_script(&script, &checksum).await?;
            }
        }
        HistoryDiagnostic::FilesystemIsBehind { unpersisted_migrations } => {
            tracing::info!(
                ?unpersisted_migrations,
                "The filesystem migrations are behind the migrations applied to the database."
            );

            if force {
                tracing::warn!("Rolling back applied migrations that do not appear in the filesystem.");

                connector
                    .revert_to(&fs_migration_scripts, unpersisted_migrations)
                    .await?;

                connector.initialize().await?;
            } else {
                return Ok((Some(format!("The migrations folder is behind the database. The migrations that are not in the folder will be reverted. This will drop all the data in the local database.")), false));
            }
        }
        HistoryDiagnostic::HistoriesDiverge {
            last_applied_filesystem_migration,
        } => {
            let last_applied_fs_migration = filesystem_migrations
                .get(last_applied_filesystem_migration)
                .expect("Last applied fs migration");

            if !force {
                if last_applied_filesystem_migration == applied_migrations.len() - 2
                    && last_applied_filesystem_migration == filesystem_migrations.len() - 2
                    && applied_migrations[last_applied_filesystem_migration + 1].name
                        == filesystem_migrations[last_applied_filesystem_migration + 1].migration_id()
                {
                    return Ok((Some(format!("The last migration was edited. It will be reverted and applied again. All data in the local database will be lost.")), false));
                }

                return Ok((Some(format!("The history of the migrations from the migrations table and the migrations folder diverge, after the `{}` migration. The database will be returned to a clean history. This will drop all the data in the local database. (TODO: offer to rebase)", filesystem_migrations.get(last_applied_filesystem_migration).expect("get last_applied_filesystem_migration by index").migration_id())), false));
            }

            tracing::warn!(
                "Diverging histories detected: reverting to `{}` and applying local migrations.",
                last_applied_fs_migration.migration_id()
            );

            let common_fs_migrations: Vec<String> = filesystem_migrations[..last_applied_filesystem_migration]
                .iter()
                .map(|mig| mig.read_migration_script().expect("read mig script"))
                .collect();

            // Revert
            connector
                .revert_to(
                    &common_fs_migrations,
                    &applied_migrations[last_applied_filesystem_migration..],
                )
                .await?;

            tracing::info!("Reverted!");

            // Reapply
            let unapplied_migrations = &filesystem_migrations[last_applied_filesystem_migration..];

            for filesystem_migration in unapplied_migrations {
                tracing::debug!(
                    "Applying migration from migrations folder: `{}`",
                    filesystem_migration.migration_id()
                );
                let script = filesystem_migration
                    .read_migration_script()
                    .map_err(|err| CommandError::Generic(err.into()))?;

                let mut hasher = Sha512::new();
                hasher.update(&script);
                let checksum = hasher.finalize();

                connector
                    .persist_imperative_migration_to_table(filesystem_migration.migration_id(), &checksum, &script)
                    .await?;

                applier.apply_migration_script(&script, &checksum).await?;
            }
        }
    }

    Ok((None, true))
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
        last_applied_filesystem_migration: usize,
    },
}

impl HistoryDiagnostic<'_> {
    fn short(&self) -> &'static str {
        match self {
            HistoryDiagnostic::UpToDate => "UpToDate",
            HistoryDiagnostic::DatabaseIsBehind { .. } => "DatabaseIsBehind",
            HistoryDiagnostic::FilesystemIsBehind { .. } => "FilesystemIsBehind",
            HistoryDiagnostic::HistoriesDiverge { .. } => "HistoriesDiverge",
        }
    }
}

#[tracing::instrument]
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
                if let Some(last_applied_filesystem_migration) = last_applied_filesystem_migration {
                    return Ok(HistoryDiagnostic::HistoriesDiverge {
                        last_applied_filesystem_migration,
                    });
                }

                return Ok(HistoryDiagnostic::FilesystemIsBehind {
                    unpersisted_migrations: applied_migrations_slice,
                });
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
    /// Give permission to the migration engine to revert migrations and drop data in the process.
    pub force: bool,
    /// Push the schema ignoring destructive change warnings.
    pub accept_data_loss: bool,
    /// Generate the next migration without applying it.
    pub draft: bool,
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
    pub radical_measure: Option<String>,
}

impl SchemaPushOutput {
    pub fn had_no_changes_to_push(&self) -> bool {
        self.warnings.is_empty()
            && self.unexecutable.is_empty()
            && self.executed_steps == 0
            && self.radical_measure.is_none()
    }
}
