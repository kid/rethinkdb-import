use std::path::Path;

use ijson::IValue;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct DbInfo {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TableInfo {
    pub name: String,
    pub db: DbInfo,
    pub indexes: Vec<IndexInfo>,
    pub primary_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexInfo {
    pub index: String,
    pub function: IValue,
}

#[tracing::instrument()]
pub(crate) async fn parse_table_info(path: &Path) -> anyhow::Result<TableInfo> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut buffer = Vec::with_capacity(8 * 1024);
    file.read_to_end(&mut buffer).await?;

    let info: TableInfo = serde_json::from_slice(&buffer)?;
    Ok(info)
}
