use anyhow::Context;
use futures::TryStreamExt;
use mobc_reql::{GetSession, Pool};

use reql::cmd::table_create::Options;

use reql::r;
use serde_json::json;
use tracing::{error, info};

use crate::TableInfo;

#[tracing::instrument(skip(pool))]
pub(crate) async fn drop_database(pool: Pool, db: &str) -> anyhow::Result<()> {
    let conn = pool.session().await?;

    match r
        .db_drop(db)
        .run::<_, ijson::IValue>(&conn)
        .try_next()
        .await
    {
        Ok(_) => info!("db dropped"),
        Err(e) => error!("{}", e),
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub(crate) async fn create_database(pool: Pool, db: &str) -> anyhow::Result<()> {
    let conn = pool.session().await?;

    match r
        .db_create(db)
        .run::<_, ijson::IValue>(&conn)
        .try_next()
        .await
    {
        Ok(_) => info!("db created"),
        Err(e) => error!("{}", e),
    }

    Ok(())
}

#[tracing::instrument(skip(pool, table))]
pub(crate) async fn create_table(pool: Pool, table: &TableInfo) -> anyhow::Result<()> {
    let conn = pool.session().await?;

    match r
        .db(&table.db.name)
        .table_create(r.args((
            table.name.to_owned(),
            Options::new().primary_key(table.primary_key.to_owned()),
        )))
        .run::<_, ijson::IValue>(&conn)
        .try_next()
        .await
    {
        Ok(_) => info!("table created"),
        Err(e) => error!("{}", e),
    }

    for index in table.indexes.iter() {
        let data: String = ijson::from_value(index.function.get("data").context("no index data")?)?;

        match r
            .db(&table.db.name)
            .table(&table.name)
            .index_create(r.args((
                "foo".to_owned(),
                json!({
                    "$reql_type$": "BINARY",
                    "data": data
                }),
            )))
            .run::<_, ijson::IValue>(&conn)
            .try_next()
            .await
        {
            Ok(_) => info!("index created"),
            Err(e) => error!("{}", e),
        }
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub(crate) async fn drop_table(pool: Pool, db: &str, table: &str) -> anyhow::Result<()> {
    let conn = pool.session().await?;

    match r
        .db(db)
        .table_drop(table)
        .run::<_, ijson::IValue>(&conn)
        .try_next()
        .await
    {
        Ok(_) => info!("table created"),
        Err(e) => error!("{}", e),
    }

    Ok(())
}
