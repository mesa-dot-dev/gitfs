//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::{debug, error};

mod app_config;
mod daemon;
mod fs;
mod fuse_check;
mod onboarding;
mod trc;
mod updates;

use crate::app_config::Config;
use crate::trc::Trc;

#[derive(Parser)]
#[command(
    version,
    author = "Marko Vejnovic",
    about = "mesa.dev's filesystem for git repos."
)]
struct Args {
    #[arg(
        short,
        long,
        value_parser,
        help = "Optional path to a mesa config TOML."
    )]
    config_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Spawn the filesystem as a daemon process.
    Run {
        /// Run the daemon in the background.
        #[arg(short, long, help = "Run the daemon in the background.")]
        daemonize: bool,
    },

    /// Reload the configuration of a running daemon.
    Reload,
}

/// Main entry point for the application.
fn main() {
    let trc_handle = Trc::default().init().unwrap_or_else(|e| {
        eprintln!(
            "Failed to initialize logging. Without logging, we can't provide any useful error \
             messages, so we have to exit: {e}"
        );
        std::process::exit(1);
    });

    updates::check_for_updates();

    let args = Args::parse();
    let config = Config::load_or_create(args.config_path.as_deref()).unwrap_or_else(|e| {
        error!("Failed to load configuration: {e}");
        std::process::exit(1);
    });
    if let Err(error_messages) = config.validate() {
        error!("Configuration is invalid.");
        for msg in &error_messages {
            error!(" - {msg}");
        }

        std::process::exit(1);
    }

    match args.command {
        Command::Run { daemonize } => {
            if let Err(e) = fuse_check::ensure_fuse() {
                error!("{e}");
                std::process::exit(1);
            }

            if daemonize {
                debug!(config = ?config, "Initializing daemon with configuration...");
                // It is safe to unwrap this Config.validate() guarantees that pid_file's parent
                // exists.
                // Safe: Config.validate() guarantees pid_file's parent exists.
                let pid_file_parent = config.daemon.pid_file.parent().unwrap_or_else(|| {
                    unreachable!("Config.validate() ensures pid_file has a parent")
                });
                if let Err(e) = std::fs::create_dir_all(pid_file_parent) {
                    error!("Failed to create PID file directory: {e}");
                    return;
                }

                let daemonize = daemonize::Daemonize::new()
                    .pid_file(&config.daemon.pid_file)
                    .chown_pid_file(true)
                    .user(config.uid)
                    .group(config.gid);

                // TODO(markovejnovic): Handle stdout, stderr
                match daemonize.start() {
                    Ok(()) => {
                        trc_handle.reconfigure_for_daemon();
                        daemon::spawn(config);
                    }
                    Err(e) => {
                        error!("Failed to spawn the daemon: {e}");
                    }
                }
            } else {
                daemon::spawn(config);
            }
        }
        Command::Reload => {}
    }
}
