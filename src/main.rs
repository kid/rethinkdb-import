mod json_utils;
mod models;
mod naive;
mod options;
mod rdb;

use structopt::StructOpt;

use mobc_reql::{Pool, SessionManager};
use tracing::{info, Level};

use crate::models::*;
use crate::options::Opt;

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
#[tracing::instrument]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    tracing_subscriber::fmt::fmt()
        .with_max_level(Level::INFO)
        .init();

    let manager = SessionManager::new(opt.clone().into());

    tokio::spawn(manager.discover_hosts());

    let pool = Pool::builder().max_open(20).build(manager);

    naive::prepare_tables(pool.clone(), &opt.directory).await?;
    naive::restore_path(pool.clone(), &opt.directory).await?;

    info!("Done");

    Ok(())
}
