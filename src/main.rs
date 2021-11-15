#![feature(slice_as_chunks)]

use itertools::Itertools;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use flate2::bufread::MultiGzDecoder;
use futures::{future, TryStreamExt};
use mobc_reql::{GetSession, Pool, SessionManager};
use reql::cmd::connect::Options;
use reql::r;
use reql::types::WriteStatus;
use serde_json::{json, Value};
use tracing::{debug, error, info, Level};

#[tokio::main]
#[tracing::instrument]
async fn main() -> reql::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let args: Vec<String> = std::env::args().collect();

    let mut options = Options::default();
    options.host = args[1].to_owned().into();
    let manager = SessionManager::new(options);

    tokio::spawn(manager.discover_hosts());

    let pool = Pool::builder().max_open(20).build(manager);

    // for db in vec!["foo", "bar", "baz"].iter() {
    //     drop_database(pool.clone(), db).await?;
    // }

    restore_path(pool.clone(), Path::new(&args[2])).await?;

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
                .filter_map(Result::ok)
                .map(|path| {
                    let pool = pool.clone();
                    tokio::spawn(async move { import_file(pool, &path).await })
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
async fn import_file(pool: Pool, file: &Path) -> reql::Result<()> {
    let table = file.file_stem().and_then(|n| n.to_str());
    let db = file
        .parent()
        .and_then(|n| n.file_stem())
        .and_then(|n| n.to_str());

    match (db, table) {
        (Some(db), Some(table)) => {
            let file = File::open(file)?;
            // let reader = BufReader::new(file);
            let reader = BufReader::with_capacity(64 * 1024, file);
            let mut reader = MultiGzDecoder::new(reader);

            // let items: Vec<Value> = serde_json::from_reader(reader)?;

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

            let mut batch : Vec<Value> = Vec::with_capacity(200);
            let mut iterator = iter_json_array(reader).map_while(|e| e.ok());

            loop {
                batch.clear();
                // for x in 0..200 {
                //     // debug!("iterating {}", x);
                //     match iterator.next() {
                //         Some(e) => batch.push(e),
                //         _ => {},
                //         // None => break,
                //     }
                // }
                batch.extend((0..200).map_while(|_| iterator.next()));
                // debug!("batch size: {}", batch.len());
                if batch.is_empty() {
                    // debug!("done");
                    break;
                }
                let mut query = r
                    .db(db)
                    .table(table)
                    .insert(&batch)
                    .run::<_, WriteStatus>(&conn);
                match query.try_next().await {
                    // Ok(Some(res)) if res.inserted != u32::try_from(batch.len()).unwrap() => {
                    //     error!("{:?}", res)
                    // },
                    Ok(Some(res)) => debug!("inserted {} rows", res.inserted),
                    Ok(_) => {}
                    Err(e) => error!("{}", e),
                }
            }

            // // let (chunks, remainder) = items.as_chunks::<200>();
            // // let mut iterator = items.chunks_exact(200);
            // for batch in &iter_json_array(reader).chunks(200) {
            //     let batch: Vec<Value> = batch
            //         .filter_map(|e| e.ok())
            //         .collect();
            //     let mut query = r
            //         .db(db)
            //         .table(table)
            //         .insert(batch)
            //         .run::<_, WriteStatus>(&conn);
            //     match query.try_next().await {
            //         // Ok(Some(res)) if res.inserted != u32::try_from(batch.len()).unwrap() => {
            //         //     error!("{:?}", res)
            //         // },
            //         Ok(Some(res)) => debug!("inserted {} rows", res.inserted),
            //         Ok(_) => {}
            //         Err(e) => error!("{}", e),
            //     }
            // }
            //
            // let mut query = r
            //     .db(db)
            //     .table(table)
            //     .insert(remainder.to_vec())
            //     .run::<_, WriteStatus>(&conn);
            // match query.try_next().await {
            //     Ok(Some(res)) if res.inserted != u32::try_from(remainder.len()).unwrap() => {
            //         error!("{:?}", res);
            //     },
            //     Ok(Some(res)) => debug!("inserted {} rows", res.inserted),
            //     Ok(_) => {},
            //     Err(e) => error!("{}", e),
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

use serde::de::DeserializeOwned;
use serde_json::{self, Deserializer};
use std::io::{self, Read};

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
) -> impl Iterator<Item = Result<T, io::Error>> {
    let mut at_start = false;
    std::iter::from_fn(move || yield_next_obj(&mut reader, &mut at_start).transpose())
}
