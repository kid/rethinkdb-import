#![feature(slice_as_chunks)]

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use futures::{future, TryStreamExt};
use mobc_reql::{GetSession, Pool, SessionManager};
use reql::cmd::connect::Options;
use reql::r;
use reql::types::WriteStatus;
use serde_json::{json, Value};
use tracing::{error, info, Level};

#[tokio::main]
#[tracing::instrument]
async fn main() -> reql::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let manager = SessionManager::new(Options::new());
    tokio::spawn(manager.discover_hosts());

    let pool = Pool::builder().max_open(20).build(manager);

    for db in vec!["foo", "bar", "baz"].iter() {
        drop_database(pool.clone(), db).await?;
    }
    restore_path(
        pool.clone(),
        Path::new("./rethinkdb_export_2021-11-14T12:36:24/"),
    )
    .await?;

    // for db in vec!["foo", "bar", "baz"].iter() {
    //     drop_database(pool.clone(), db).await?;
    // }
    // generate_dummy_data(pool.clone()).await?;

    info!("Done");

    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn restore_path(pool: Pool, dir: &Path) -> reql::Result<()> {
    match dir
        .join("**/*.json")
        .to_str()
        .map(|pattern| glob::glob(pattern))
    {
        Some(Ok(paths)) => {
            let tasks: Vec<_> = paths
                .filter_map(Result::ok)
                .map(|path| {
                    let pool = pool.clone();
                    tokio::spawn(async move { import_file(pool, &path).await })
                })
                .collect();

            future::join_all(tasks).await;
        }
        _ => {}
    }
    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn import_file(pool: Pool, file: &Path) -> reql::Result<()> {
    let table = file.file_stem().and_then(|n| n.to_str());
    let db = file
        .parent()
        .and_then(|n| n.file_stem())
        .and_then(|n| n.to_str());

    match (db, table) {
        (Some(table), Some(db)) => {
            let file = File::open(file)?;
            // let reader = BufReader::new(file);
            let reader = BufReader::with_capacity(64 * 1024, file);
            let items: Vec<Value> = serde_json::from_reader(reader)?;

            let conn = pool.session().await?;
            match r
                .db_create(db.to_string())
                .run::<_, Value>(&conn)
                .try_next()
                .await
            {
                Ok(_) => info!("db created"),
                Err(e) => error!("{}", e),
            }
            match r
                .db(db.to_string())
                .table_create(table.to_string())
                .run::<_, Value>(&conn)
                .try_next()
                .await
            {
                Ok(_) => info!("table created"),
                Err(e) => error!("{}", e),
            }

            let iterator = items.chunks_exact(200);
            for batch in iterator {
                let mut query = r
                    .db(db)
                    .table(table)
                    .insert(batch)
                    .run::<_, WriteStatus>(&conn);
                if let Err(e) = query.try_next().await {
                    error!("{}", e);
                }
            }

            // let mut query = r.db(db).table(table).insert(iterator.remainder()).run::<_, WriteStatus>(&conn);
            // if let Err(e) = query.try_next().await {
            //     error!("{}", e);
            // }
        }
        _ => {}
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn drop_database(pool: Pool, db: &str) -> reql::Result<()> {
    let conn = pool.session().await?;

    match r.db_drop(db).run::<_, Value>(&conn).try_next().await {
        Ok(_) => info!("db dropped"),
        Err(e) => error!("{}", e),
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn generate_dummy_data(pool: Pool) -> reql::Result<()> {
    let conn = pool.session().await?;

    let names = vec!["foo", "bar", "baz"];
    for db in names.iter() {
        match r
            .db_create(db.to_string())
            .run::<_, Value>(&conn)
            .try_next()
            .await
        {
            Ok(_) => info!("db created"),
            Err(e) => error!("{}", e),
        }
    }

    let tasks: Vec<_> = names
        .iter()
        .flat_map(|db| {
            names.iter().map(|table| {
                let pool = pool.clone();
                let db = db.clone();
                let table = table.clone();

                tokio::spawn(async move {
                    let conn = pool.session().await?;
                    match r
                        .db(db.to_string())
                        .table_create(table.to_string())
                        .run::<_, Value>(&conn)
                        .try_next()
                        .await
                    {
                        Ok(_) => info!("table created"),
                        Err(e) => error!("{}", e),
                    }

                    for id in 1..=1000_i32 {
                        let foo = json!({ "id": id, "foo": "bar" });

                        match r
                            .db(db.to_string())
                            .table(table.to_string())
                            .insert(foo)
                            .run::<_, WriteStatus>(&conn)
                            .try_next()
                            .await
                        {
                            Ok(Some(res)) if res.inserted != 1 => error!("{:?}", res),
                            Ok(_) => {}
                            Err(e) => error!("{}", e),
                        }
                    }

                    reql::Result::Ok(())
                })
            })
        })
        .collect();

    future::join_all(tasks).await;

    Ok(())
}
