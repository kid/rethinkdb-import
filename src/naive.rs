///! A naive implementation that just spawns futures
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use flate2::bufread::GzDecoder;
use futures::{future, FutureExt, StreamExt, TryStreamExt};
use mobc_reql::{GetSession, Pool};
use reql::r;
use reql::types::WriteStatus;

use crate::models::{parse_table_info, DbInfo, TableInfo};
use crate::{json_utils, rdb};

#[tracing::instrument(skip(pool))]
pub(crate) async fn prepare_tables(pool: Pool, dir: &Path) -> anyhow::Result<()> {
    match dir.join("**/*.info").to_str().map(glob::glob) {
        Some(Ok(paths)) => {
            let tasks: Vec<_> = paths
                .filter_map(std::result::Result::ok)
                .map(|path| async move { parse_table_info(&path).await })
                .collect();

            let table_infos = future::try_join_all(tasks).await?;
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

            Ok(())
        }
        _ => Err(anyhow::anyhow!("glob pattern did not return anything")),
    }
}

#[tracing::instrument(skip(pool))]
pub(crate) async fn restore_path(pool: Pool, dir: &Path) -> anyhow::Result<()> {
    match dir.join("**/*.json*").to_str().map(glob::glob) {
        Some(Ok(paths)) => {
            let tasks: Vec<_> = paths
                .filter_map(std::result::Result::ok)
                .map(|path| {
                    let pool = pool.clone();
                    tokio::spawn(async move { import_file(pool, path).await })
                })
                .collect();

            future::join_all(tasks).await;

            Ok(())
        }
        _ => Err(anyhow::anyhow!("glob pattern did not return anything")),
    }
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
        (Some(db), Some(table)) => match path.extension().and_then(OsStr::to_str) {
            Some("json") => {
                let file = File::open(path)?;
                let reader = BufReader::with_capacity(64 * 1024, file);
                let iterator = json_utils::iter_json_array(reader)
                    .map_while(|e: std::result::Result<ijson::IValue, std::io::Error>| e.ok());

                futures::stream::iter(iterator)
                    .chunks(200)
                    .zip(session_stream)
                    .fold(Ok(()), |acc, (batch, conn)| {
                        let db = db.clone();
                        let table = table.clone();
                        async move {
                            if acc.is_err() {
                                return acc;
                            }

                            let expected: u32 =
                                batch.len().try_into().expect("failed to parse batch size");
                            let mut query = r
                                .db(&db)
                                .table(&table)
                                .insert(batch)
                                .run::<_, WriteStatus>(&conn);
                            match query.try_next().await? {
                                Some(WriteStatus {
                                    inserted,
                                    replaced,
                                    unchanged,
                                    ..
                                }) if (inserted + replaced + unchanged) != expected => {
                                    Err(anyhow::anyhow!(
                                        "inserted/updated/unchanged did not match batch size"
                                    ))
                                }
                                _ => Ok(()),
                            }
                        }
                    })
                    .await
            }
            Some("jsongz") => {
                let file = File::open(path)?;
                let reader = BufReader::with_capacity(64 * 1024, file);
                let reader = GzDecoder::new(reader);
                let iterator = json_utils::iter_json_array(reader)
                    .map_while(|e: std::result::Result<ijson::IValue, std::io::Error>| e.ok());
                futures::stream::iter(iterator)
                    .chunks(200)
                    .zip(session_stream)
                    .fold(Ok(()), |acc, (batch, conn)| {
                        let db = db.clone();
                        let table = table.clone();
                        async move {
                            if acc.is_err() {
                                return acc;
                            }

                            let expected: u32 =
                                batch.len().try_into().expect("failed to parse batch size");
                            let mut query = r
                                .db(&db)
                                .table(&table)
                                .insert(batch)
                                .run::<_, WriteStatus>(&conn);
                            match query.try_next().await? {
                                Some(WriteStatus {
                                    inserted,
                                    replaced,
                                    unchanged,
                                    ..
                                }) if (inserted + replaced + unchanged) != expected => {
                                    Err(anyhow::anyhow!(
                                        "inserted/updated/unchanged did not match batch size"
                                    ))
                                }
                                _ => Ok(()),
                            }
                        }
                    })
                    .await
            }
            _ => Err(anyhow::anyhow!("unsuported file format")),
        },
        _ => Err(anyhow::anyhow!("unexpected file path")),
    }
}
