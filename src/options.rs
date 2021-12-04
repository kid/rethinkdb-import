use reql::cmd::connect::Options;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
pub(crate) struct Opt {
    #[structopt(short = "h", long = "host", default_value = "localhost")]
    pub(crate) host: String,
    #[structopt(short = "p", long = "port", default_value = "28015")]
    pub(crate) port: u16,
    #[structopt(parse(from_os_str))]
    pub(crate) directory: PathBuf,
}

impl From<Opt> for Options {
    fn from(opt: Opt) -> Self {
        let mut options = Options::default();
        options.host = opt.host.to_owned().into();
        options.port = opt.port;
        options
    }
}
