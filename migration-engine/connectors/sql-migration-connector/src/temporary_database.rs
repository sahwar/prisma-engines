use crate::{flavour::SqlFlavour, SqlResult};
use quaint::single::Quaint;
use sql_schema_describer::SqlSchema;
use std::sync::Arc;

pub(crate) struct TemporaryDatabase {
    pub(crate) _name: String,
    pub(crate) _temp_dir: Option<tempfile::TempDir>,
    pub(crate) schema_name: String,
    pub(crate) conn: Quaint,
}

impl TemporaryDatabase {
    pub(crate) async fn describe(&self, flavour: &(dyn SqlFlavour + Send + Sync + 'static)) -> SqlResult<SqlSchema> {
        let conn = Arc::new(self.conn.clone());

        flavour.describe_schema(&self.schema_name, conn).await
    }
}
