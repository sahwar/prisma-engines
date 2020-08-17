#![deny(rust_2018_idioms)]
#![deny(unsafe_code)]
#![allow(clippy::trivial_regex)] // these will grow

mod component;
mod database_info;
mod error;
mod flavour;
mod sql_database_migration_inferrer;
mod sql_database_step_applier;
mod sql_destructive_change_checker;
mod sql_migration;
mod sql_migration_persistence;
mod sql_renderer;
mod sql_schema_calculator;
mod sql_schema_differ;
mod temporary_database;

pub use error::*;
pub use sql_migration::*;
pub use sql_migration_persistence::MIGRATION_TABLE_NAME;

use component::Component;
use database_info::DatabaseInfo;
use flavour::SqlFlavour;
use migration_connector::*;
use quaint::{
    error::ErrorKind,
    prelude::{ConnectionInfo, Queryable, SqlFamily},
    single::Quaint,
};
use sql_database_migration_inferrer::*;
use sql_database_step_applier::*;
use sql_destructive_change_checker::*;
use sql_migration_persistence::*;
use sql_schema_describer::SqlSchema;
use std::{sync::Arc, time::Duration};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct SqlMigrationConnector {
    pub database: Arc<dyn Queryable + Send + Sync + 'static>,
    pub database_info: DatabaseInfo,
    flavour: Box<dyn SqlFlavour + Send + Sync + 'static>,
}

impl SqlMigrationConnector {
    pub async fn new(database_str: &str) -> ConnectorResult<Self> {
        let (connection, database_info) = connect(database_str).await?;
        let flavour = flavour::from_connection_info(database_info.connection_info());
        flavour.check_database_info(&database_info)?;

        Ok(Self {
            flavour,
            database_info,
            database: Arc::new(connection),
        })
    }

    pub async fn create_database(database_str: &str) -> ConnectorResult<String> {
        let connection_info =
            ConnectionInfo::from_url(database_str).map_err(|err| ConnectorError::url_parse_error(err, database_str))?;
        let flavour = flavour::from_connection_info(&connection_info);
        flavour.create_database(database_str).await
    }

    async fn drop_database(&self) -> ConnectorResult<()> {
        catch(
            self.database_info().connection_info(),
            self.flavour().drop_database(self.conn(), self.schema_name()),
        )
        .await
    }

    async fn describe_schema(&self) -> SqlResult<SqlSchema> {
        let conn = self.connector().database.clone();
        let schema_name = self.schema_name();

        self.flavour.describe_schema(schema_name, conn).await
    }

    async fn ensure_imperative_migrations_table(&self) -> SqlResult<()> {
        self.flavour().ensure_imperative_migrations_table(self.conn()).await
    }
}

#[async_trait::async_trait]
impl MigrationConnector for SqlMigrationConnector {
    type DatabaseMigration = SqlMigration;

    fn connector_type(&self) -> &'static str {
        self.database_info.connection_info().sql_family().as_str()
    }

    async fn create_database(database_str: &str) -> ConnectorResult<String> {
        Self::create_database(database_str).await
    }

    async fn initialize(&self) -> ConnectorResult<()> {
        catch(self.database_info.connection_info(), async {
            self.flavour
                .initialize(self.database.as_ref(), &self.database_info)
                .await?;

            self.flavour
                .ensure_imperative_migrations_table(self.database.as_ref())
                .await?;

            Ok(())
        })
        .await?;

        self.migration_persistence().init().await?;

        Ok(())
    }

    async fn reset(&self) -> ConnectorResult<()> {
        self.migration_persistence().reset().await?;
        self.drop_database().await?;

        Ok(())
    }

    /// Optionally check that the features implied by the provided datamodel are all compatible with
    /// the specific database version being used.
    fn check_database_version_compatibility(&self, datamodel: &datamodel::dml::Datamodel) -> Vec<MigrationError> {
        self.database_info.check_database_version_compatibility(datamodel)
    }

    fn migration_persistence<'a>(&'a self) -> Box<dyn MigrationPersistence + 'a> {
        Box::new(SqlMigrationPersistence { connector: self })
    }

    fn database_migration_inferrer<'a>(&'a self) -> Box<dyn DatabaseMigrationInferrer<SqlMigration> + 'a> {
        Box::new(SqlDatabaseMigrationInferrer { connector: self })
    }

    fn database_migration_step_applier<'a>(&'a self) -> Box<dyn DatabaseMigrationStepApplier<SqlMigration> + 'a> {
        Box::new(SqlDatabaseStepApplier { connector: self })
    }

    fn destructive_change_checker<'a>(&'a self) -> Box<dyn DestructiveChangeChecker<SqlMigration> + 'a> {
        Box::new(SqlDestructiveChangeChecker { connector: self })
    }

    fn deserialize_database_migration(&self, json: serde_json::Value) -> Option<SqlMigration> {
        serde_json::from_value(json).ok()
    }

    async fn persist_imperative_migration_to_table(
        &self,
        name: &str,
        checksum: &[u8],
        script: &str,
    ) -> ConnectorResult<()> {
        let fut = async {
            self.ensure_imperative_migrations_table().await?;

            let insert = quaint::ast::Insert::single_into((self.schema_name(), "prisma_imperative_migrations"))
                .value("script", script)
                .value("checksum", checksum)
                .value("name", name);

            self.conn().execute(insert.into()).await?;

            Ok(())
        };

        catch(self.connection_info(), fut).await
    }

    async fn read_imperative_migrations(&self) -> ConnectorResult<Vec<ImperativeMigration>> {
        use quaint::ast;

        let fut = async {
            self.ensure_imperative_migrations_table().await?;

            let query = ast::Select::from_table((self.schema_name(), "prisma_imperative_migrations"))
                .column("script")
                .column("name")
                .column("checksum")
                .column("startedAt")
                .column("finishedAt")
                .column("rolledBackAt");

            let rows = self.conn().query(query.into()).await?;

            let migrations: Option<Vec<ImperativeMigration>> = rows
                .into_iter()
                .map(|row| {
                    Some(ImperativeMigration {
                        script: row.get("script")?.as_str()?.into(),
                        name: row.get("name")?.as_str()?.into(),
                        checksum: row.get("checksum")?.as_bytes()?.into(),
                        started_at: row
                            .get("startedAt")
                            .and_then(|value| value.as_datetime())
                            .map(|v| v.into()),
                        finished_at: row
                            .get("finishedAt")
                            .and_then(|value| value.as_datetime())
                            .map(|v| v.into()),
                        rolled_back_at: row
                            .get("rolledBackAt")
                            .and_then(|value| value.as_datetime())
                            .map(|v| v.into()),
                    })
                })
                .collect();

            Ok(migrations.expect("failed to fetch migrations"))
        };

        catch(self.connection_info(), fut).await
    }

    async fn revert_to(
        &self,
        filesystem_migrations: &[String],
        _to_be_rolled_back: &[ImperativeMigration],
    ) -> ConnectorResult<()> {
        tracing::warn!("Dropping the database to revert migrations.");

        self.drop_database().await?;
        catch(self.database_info().connection_info(), async {
            let conn = self.conn();
            self.flavour.initialize(conn, self.database_info()).await?;
            self.flavour.ensure_imperative_migrations_table(conn).await
        })
        .await?;

        let applier = SqlDatabaseStepApplier { connector: self };

        // apply all the migrations
        for script in filesystem_migrations {
            let checksum = migration_script_checksum(&script);
            applier.apply_migration_script(script, &checksum).await?;
        }

        Ok(())
    }

    async fn smart_revert_to(
        &self,
        filesystem_migrations: &[String],
        to_be_rolled_back: &[ImperativeMigration],
    ) -> ConnectorResult<()> {
        use quaint::ast::{self, *};

        let temporary_db = self.flavour.create_temporary_database().await?;

        // apply all the migrations
        for migration in filesystem_migrations {
            temporary_db
                .conn
                .raw_cmd(migration)
                .await
                .map_err(SqlError::from)
                .map_err(|err| err.into_connector_error(self.database_info().connection_info()))?;
        }

        // introspect current schema
        let src_schema = self
            .describe_schema()
            .await
            .map_err(SqlError::from)
            .map_err(|err| err.into_connector_error(self.database_info().connection_info()))?;

        // introspect temporary database
        let target_schema = temporary_db
            .describe(self.flavour.as_ref())
            .await
            .map_err(SqlError::from)
            .map_err(|err| err.into_connector_error(self.database_info().connection_info()))?;

        // infer database migration
        let migration = infer(src_schema, target_schema, self.database_info(), self.flavour.as_ref());

        let diagnostics = self.destructive_change_checker().check(&migration).await?;

        for warning in &diagnostics.warnings {
            tracing::warn!("WARNING: {}", warning.description);
        }

        if !diagnostics.unexecutable_migrations.is_empty() {
            todo!("Unexecutable!\n{:#?}", diagnostics.unexecutable_migrations);
        }

        // apply
        let applier = self.database_migration_step_applier();

        if applier.migration_is_empty(&migration) {
            tracing::warn!("Nothing to roll back.");
            return Ok(());
        }

        let mut step = 0;

        while applier.apply_step(&migration, step).await? {
            step += 1;
        }

        let rolled_back_checksums: Vec<quaint::Value<'_>> = to_be_rolled_back
            .iter()
            .map(|migration| quaint::Value::bytes(migration.checksum.as_slice()))
            .collect();

        // marked migrations as rolled back
        let rollback = ast::Update::table("prisma_imperative_migrations")
            .so_that(ast::Column::from("checksum").in_selection(rolled_back_checksums))
            .set("rolledBackAt", "CURRENT_TIMESTAMP");

        self.conn()
            .execute(rollback.into())
            .await
            .expect("failed to roll back in imperative migrations table");

        self.flavour.drop_temporary_database(&temporary_db).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self, filesystem_migrations))]
    async fn detect_drift(&self, filesystem_migrations: &[String]) -> ConnectorResult<Option<Self::DatabaseMigration>> {
        let temporary_db = self.flavour.create_temporary_database().await?;

        tracing::debug!("Applying migration folder migrations to temporary database.");

        for migration in filesystem_migrations {
            temporary_db
                .conn
                .raw_cmd(migration)
                .await
                .map_err(SqlError::from)
                .map_err(|err| err.into_connector_error(self.connection_info()))?;
        }

        let main_database_schema = self
            .describe_schema()
            .await
            .map_err(|err| err.into_connector_error(self.connection_info()))?;

        let temporary_database_schema = temporary_db
            .describe(self.flavour.as_ref())
            .await
            .map_err(|err| err.into_connector_error(self.connection_info()))?;

        let migration = infer(
            main_database_schema,
            temporary_database_schema,
            self.database_info(),
            self.flavour.as_ref(),
        );

        let migration = Some(migration).filter(|migration| !migration.steps.is_empty());

        Ok(migration)
    }
}

pub(crate) async fn catch<O>(
    connection_info: &ConnectionInfo,
    fut: impl std::future::Future<Output = Result<O, SqlError>>,
) -> Result<O, ConnectorError> {
    match fut.await {
        Ok(o) => Ok(o),
        Err(sql_error) => Err(sql_error.into_connector_error(connection_info)),
    }
}

async fn connect(database_str: &str) -> ConnectorResult<(Quaint, DatabaseInfo)> {
    let connection_info =
        ConnectionInfo::from_url(database_str).map_err(|err| ConnectorError::url_parse_error(err, database_str))?;

    let connection_fut = async {
        let connection = Quaint::new(database_str)
            .await
            .map_err(SqlError::from)
            .map_err(|err: SqlError| err.into_connector_error(&connection_info))?;

        // async connections can be lazy, so we issue a simple query to fail early if the database
        // is not reachable.
        connection
            .raw_cmd("SELECT 1")
            .await
            .map_err(SqlError::from)
            .map_err(|err| err.into_connector_error(&connection.connection_info()))?;

        Ok::<_, ConnectorError>(connection)
    };

    let connection = tokio::time::timeout(CONNECTION_TIMEOUT, connection_fut)
        .await
        .map_err(|_elapsed| {
            // TODO: why...
            SqlError::from(ErrorKind::ConnectTimeout("Tokio timer".into())).into_connector_error(&connection_info)
        })??;

    let database_info = DatabaseInfo::new(&connection, connection.connection_info().clone())
        .await
        .map_err(|sql_error| sql_error.into_connector_error(&connection_info))?;

    Ok((connection, database_info))
}
