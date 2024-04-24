mod cli;

use self::cli::NeardCmd;
use anyhow::Context;
use near_primitives::version::{Version, PROTOCOL_VERSION};
use near_store::metadata::DB_VERSION;
use nearcore::get_default_home;
use once_cell::sync::Lazy;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

static NEARD_VERSION: &str = env!("NEARD_VERSION");
static NEARD_BUILD: &str = env!("NEARD_BUILD");
static RUSTC_VERSION: &str = env!("NEARD_RUSTC_VERSION");

static NEARD_VERSION_STRING: Lazy<String> = Lazy::new(|| {
    let mut features = Vec::new();
    if cfg!(feature = "nightly") {
        features.push("nightly");
    }
    if cfg!(feature = "statelessnet_protocol") {
        features.push("statelessnet_protocol");
    }
    if cfg!(feature = "new_epoch_sync") {
        features.push("new_epoch_sync");
    }
    if cfg!(feature = "no_cache") {
        features.push("no_cache");
    }
    if cfg!(feature = "test_features") {
        features.push("test_features");
    }
    if cfg!(feature = "byzanitne_asserts") {
        features.push("byzantine_asserts");
    }
    if cfg!(feature = "sandbox") {
        features.push("sandbox");
    }
    if cfg!(feature = "protocol_feature_nonrefundable_transfer_nep491") {
        features.push("protocol_feature_nonrefundable_transfer_nep491");
    }
    if cfg!(feature = "io_trace") {
        features.push("io_trace");
    }
    if cfg!(feature = "yield_resume") {
        features.push("yield_resume");
    }
    // TODO: more features
    format!(
        "(release {}) (build {}) (rustc {}) (protocol {}) (db {}) features: {:?}",
        NEARD_VERSION, NEARD_BUILD, RUSTC_VERSION, PROTOCOL_VERSION, DB_VERSION, features
    )
});

fn neard_version() -> Version {
    Version {
        version: NEARD_VERSION.to_string(),
        build: NEARD_BUILD.to_string(),
        rustc_version: RUSTC_VERSION.to_string(),
    }
}

static DEFAULT_HOME: Lazy<PathBuf> = Lazy::new(get_default_home);

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    if env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        env::set_var("RUST_BACKTRACE", "1");
    }

    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global()
        .context("failed to create the threadpool")?;

    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(Duration::from_secs(60));

    // The default FD soft limit in linux is 1024.
    // We use more than that, for example we support up to 1000 TCP
    // connections, using 5 FDs per each connection.
    // We consider 65535 to be a reasonable limit for this binary,
    // and we enforce it here. We also set the hard limit to the same value
    // to prevent the inner logic from trying to bump it further:
    // FD limit is a global variable, so it shouldn't be modified in an
    // uncoordinated way.
    const FD_LIMIT: u64 = 65535;
    let (_, hard) = rlimit::Resource::NOFILE.get().context("rlimit::Resource::NOFILE::get()")?;
    rlimit::Resource::NOFILE.set(FD_LIMIT, FD_LIMIT).context(format!(
        "couldn't set the file descriptor limit to {FD_LIMIT}, hard limit = {hard}"
    ))?;

    NeardCmd::parse_and_run()
}
