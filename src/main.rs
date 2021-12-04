#![feature(slice_as_chunks)]

mod options;

use flate2::bufread::GzDecoder;
use structopt::StructOpt;

use futures::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use futures::{future, TryStreamExt};
use mobc_reql::{GetSession, Pool, SessionManager};
use reql::*;
use reql::types::WriteStatus;
use serde_json::json;
use tracing::{debug, error, info, Level};

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
#[tracing::instrument]
async fn main() -> reql::Result<()> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::INFO)
        .init();

    let manager = SessionManager::new(opt.clone().into());

    tokio::spawn(manager.discover_hosts());

    let pool = Pool::builder().max_open(20).build(manager);

    for db in vec!["cm", "sandbox", "sandboxsnapshot", "wave", "wavesnapshot"].iter() {
        drop_database(pool.clone(), db).await?;
        create_database(pool.clone(), db).await?;
    }
    restore_path(pool.clone(), &opt.directory).await?;

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
        .join("**/*.jsongz")
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
                // .take(1)
                .collect();

            future::join_all(tasks).await;
        }
        _ => {}
    }
    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn import_file(pool: Pool, path: PathBuf) -> reql::Result<()> {
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
            let reader = GzDecoder::new(reader);

            // drop_table(pool.clone(), &db, &table).await?;
            create_table(pool.clone(), &db, &table).await?;

            futures::stream::iter(
                iter_json_array(reader)
                    .map_while(|e: std::result::Result<ijson::IValue, std::io::Error>| e.ok()),
            )
            .chunks(200)
            .zip(session_stream)
            // .take(0)
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

#[tracing::instrument(skip(pool))]
async fn drop_database(pool: Pool, db: &str) -> reql::Result<()> {
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
async fn create_database(pool: Pool, db: &str) -> reql::Result<()> {
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

#[tracing::instrument(skip(pool))]
async fn create_table(pool: Pool, db: &str, table: &str) -> reql::Result<()> {
    let conn = pool.session().await?;

    match r
        .db(db)
        .table_create(table)
        .run::<_, ijson::IValue>(&conn)
        .try_next()
        .await
    {
        Ok(_) => info!("table created"),
        Err(e) => error!("{}", e),
    }

    // let command = reql::cmd::wait::Command::new(ql2::term::TermType::Wait);
    // // let opts = json!({ "timeout": 0_i32, "wait_for": "ready_for_writes" });
    // let opts = json!(null);
    // // let query = command.run(conn);
    // if let Err(e) = r.db(db).table(table).wait(reql::Command::from_json(opts)).run::<_, ijson::IValue>(&conn).try_next().await {
    //     error!("{}", e);
    // } else {
    //     info!("table ready");
    // }
 
    Ok(())
}

#[tracing::instrument(skip(pool))]
async fn drop_table(pool: Pool, db: &str, table: &str) -> reql::Result<()> {
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

#[tracing::instrument(skip(pool))]
async fn generate_dummy_data(pool: Pool) -> reql::Result<()> {
    let conn = pool.session().await?;

    let names = vec!["foo", "bar", "baz"];
    for db in names.iter() {
        match r
            .db_create(db.to_string())
            .run::<_, ijson::IValue>(&conn)
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
                        .run::<_, ijson::IValue>(&conn)
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

use serde::de::DeserializeOwned;
use serde_json::{self, Deserializer};
use std::io::{self, Read};

use crate::options::Opt;

fn read_skipping_ws(mut reader: impl Read) -> io::Result<u8> {
    loop {
        let mut byte = 0u8;
        reader.read_exact(std::slice::from_mut(&mut byte))?;
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
    }
}

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn deserialize_single<T: DeserializeOwned, R: Read>(reader: R) -> io::Result<T> {
    let next_obj = Deserializer::from_reader(reader).into_iter::<T>().next();
    match next_obj {
        Some(result) => result.map_err(Into::into),
        None => Err(invalid_data("premature EOF")),
    }
}

fn yield_next_obj<T: DeserializeOwned, R: Read>(
    mut reader: R,
    at_start: &mut bool,
) -> io::Result<Option<T>> {
    if !*at_start {
        *at_start = true;
        if read_skipping_ws(&mut reader)? == b'[' {
            // read the next char to see if the array is empty
            let peek = read_skipping_ws(&mut reader)?;
            if peek == b']' {
                Ok(None)
            } else {
                deserialize_single(io::Cursor::new([peek]).chain(reader)).map(Some)
            }
        } else {
            Err(invalid_data("`[` not found"))
        }
    } else {
        match read_skipping_ws(&mut reader)? {
            b',' => deserialize_single(reader).map(Some),
            b']' => Ok(None),
            _ => Err(invalid_data("`,` or `]` not found")),
        }
    }
}

pub fn iter_json_array<T: DeserializeOwned, R: Read>(
    mut reader: R,
) -> impl Iterator<Item = std::result::Result<T, io::Error>> {
    let mut at_start = false;
    std::iter::from_fn(move || yield_next_obj(&mut reader, &mut at_start).transpose())
}
