pub mod game_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/game_types_capnp.rs"));
}
pub mod game_world_capnp {
    include!(concat!(env!("OUT_DIR"), "/game_world_capnp.rs"));
}
pub mod chat_capnp {
    include!(concat!(env!("OUT_DIR"), "/chat_capnp.rs"));
}
pub mod inventory_capnp {
    include!(concat!(env!("OUT_DIR"), "/inventory_capnp.rs"));
}
pub mod matchmaking_capnp {
    include!(concat!(env!("OUT_DIR"), "/matchmaking_capnp.rs"));
}

mod client;
mod server;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "e2e-rpc-test")]
#[command(about = "Cap'n Proto RPC e2e test - Rust implementation")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Server {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 4003)]
        port: u16,
        #[arg(long, default_value = "game_world")]
        schema: String,
    },
    Client {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 4003)]
        port: u16,
        #[arg(long, default_value = "game_world")]
        schema: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Server { host, port, schema } => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, server::run(&host, port, &schema))?;
        }
        Mode::Client { host, port, schema } => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, client::run(&host, port, &schema))?;
        }
    }

    Ok(())
}
