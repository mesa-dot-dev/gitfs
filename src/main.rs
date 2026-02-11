//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::{debug, error};

mod app_config;
mod daemon;
mod fs;
mod fuse_check;
mod onboarding;
mod term;
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
    command: Option<Command>,
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
    let args = Args::parse();

    // Load config first so telemetry settings are available for tracing init.
    // Errors use eprintln since tracing isn't initialized yet.
    let config = Config::load_or_create(args.config_path.as_deref()).unwrap_or_else(|e| {
        eprintln!("Failed to load configuration: {e}");
        std::process::exit(1);
    });
    if let Err(error_messages) = config.validate() {
        eprintln!("Configuration is invalid.");
        for msg in &error_messages {
            eprintln!(" - {msg}");
        }
        std::process::exit(1);
    }

    let trc_handle = Trc::default()
        .with_telemetry(&config.telemetry)
        .init()
        .unwrap_or_else(|e| {
            eprintln!(
                "Failed to initialize logging. Without logging, we can't provide any useful error \
                 messages, so we have to exit: {e}"
            );
            std::process::exit(1);
        });

    updates::check_for_updates();

    match args.command.unwrap_or(Command::Run { daemonize: false }) {
        Command::Run { daemonize } => {
            if let Err(e) = fuse_check::ensure_fuse() {
                error!("{e}");
                std::process::exit(1);
            }

            if daemonize {
                debug!(config = ?config, "Initializing daemon with configuration...");
                // Safe: Config.validate() guarantees pid_file's parent exists.
                let pid_file_parent = config.daemon.pid_file.parent().unwrap_or_else(|| {
                    unreachable!("Config.validate() ensures pid_file has a parent")
                });
                if let Err(e) = std::fs::create_dir_all(pid_file_parent) {
                    error!("Failed to create PID file directory: {e}");
                    return;
                }

                let log_file = match config.daemon.log.target.open_log_file() {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to open log file: {e}");
                        return;
                    }
                };

                let mut daemonize = daemonize::Daemonize::new()
                    .pid_file(&config.daemon.pid_file)
                    .chown_pid_file(true)
                    .user(config.uid)
                    .group(config.gid);

                if let Some(file) = log_file {
                    match file.try_clone() {
                        Ok(clone) => {
                            daemonize = daemonize.stdout(file).stderr(clone);
                        }
                        Err(e) => {
                            error!("Failed to clone log file handle: {e}");
                            return;
                        }
                    }
                }

                match daemonize.start() {
                    Ok(()) => {
                        trc_handle.reconfigure_for_daemon(config.daemon.log.should_use_color());
                        if let Err(e) = daemon::spawn(config) {
                            error!("Daemon failed: {e}");
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        error!("Failed to spawn the daemon: {e}");
                    }
                }
            } else if let Err(e) = daemon::spawn(config) {
                error!("Daemon failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Reload => {}
    }
}
