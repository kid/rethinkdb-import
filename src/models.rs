use serde::{Deserialize, Serialize};
use ijson::IValue;

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
