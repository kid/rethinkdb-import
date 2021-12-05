///! A naive implementation that just spawns futures
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use futures::{future, FutureExt, StreamExt, TryStreamExt};
use mobc_reql::{GetSession, Pool};
use reql::r;
use reql::types::WriteStatus;
use tracing::{debug, error};

use crate::models::{parse_table_info, DbInfo, TableInfo};
use crate::{json_utils, rdb};

#[tracing::instrument(skip(pool))]
pub(crate) async fn prepare_tables(pool: Pool, dir: &Path) -> anyhow::Result<()> {
    match dir
        .join("**/*.info")
        .to_str()
        .map(|pattern| glob::glob(pattern))
    {
        Some(Ok(paths)) => {
            let tasks: Vec<_> = paths
                .filter_map(std::result::Result::ok)
                .map(|path| async move { parse_table_info(&path).await })
                .collect();

            let table_infos = future::try_join_all(tasks).await?;
            debug!("{:?}", table_infos.len());
            let table_infos = table_infos.into_iter().fold(
                HashMap::<DbInfo, Vec<TableInfo>>::new(),
                |mut acc, t| {
                    acc.entry(t.db.clone()).or_default().push(t);
                    acc
                },
            );

            for (db, tables) in table_infos.iter() {
                // drop_database(pool.clone(), &db.name).await?;
                rdb::create_database(pool.clone(), &db.name).await?;

                let tasks: Vec<_> = tables
                    .iter()
                    .map(|table| rdb::create_table(pool.clone(), table))
                    .collect();

                future::join_all(tasks).await;
            }
        }
        _ => {}
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
pub(crate) async fn restore_path(pool: Pool, dir: &Path) -> reql::Result<()> {
    match dir
        .join("**/*.json")
        .to_str()
        .map(|pattern| glob::glob(pattern))
    {
        Some(Ok(paths)) => {
            let tasks: Vec<_> = paths
                .filter_map(std::result::Result::ok)
                .map(|path| {
                    let pool = pool.clone();
                    tokio::spawn(async move { import_file(pool, path).await })
                })
                .collect();

            future::join_all(tasks).await;
        }
        _ => {}
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn import_file(pool: Pool, path: PathBuf) -> anyhow::Result<()> {
    let session_stream = futures::stream::repeat_with(|| pool.session().into_stream())
        .flatten()
        .filter_map(|x| async { x.ok() })
        .boxed();

    let table = path.file_stem().map(|n| n.to_string_lossy().into_owned());
    let db = path
        .parent()
        .and_then(|n| n.file_stem())
        .map(|n| n.to_string_lossy().into_owned());

    match (db, table) {
        (Some(db), Some(table)) => {
            let file = File::open(path)?;
            let reader = BufReader::with_capacity(64 * 1024, file);
            // let reader = GzDecoder::new(reader);

            futures::stream::iter(
                json_utils::iter_json_array(reader)
                    .map_while(|e: std::result::Result<ijson::IValue, std::io::Error>| e.ok()),
            )
            .chunks(200)
            .zip(session_stream)
            .for_each_concurrent(20, |(batch, conn)| {
                let db = db.clone();
                let table = table.clone();
                async move {
                    let mut query = r
                        .db(&db)
                        .table(&table)
                        .insert(batch)
                        .run::<_, WriteStatus>(&conn);
                    match query.try_next().await {
                        Ok(Some(res)) => debug!("inserted {} rows", res.inserted),
                        Ok(_) => {}
                        Err(e) => error!("{}", e),
                    }
                }
            })
            .await;
        }
        _ => {}
    }

    Ok(())
}
