use std::path::Path;

use serde::{Deserialize, Serialize};
use ijson::IValue;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct DbInfo {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TableInfo {
    pub name: String,
    pub db: DbInfo,
    pub indexes: Vec<IValue>,
    pub primary_key: String,
}

#[tracing::instrument()]
pub(crate) async fn parse_table_info(path: &Path) -> anyhow::Result<TableInfo> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut buffer = Vec::with_capacity(8 * 1024);
    file.read_to_end(&mut buffer).await?;

    let info: TableInfo = serde_json::from_slice(&buffer)?;
    Ok(info)
}
