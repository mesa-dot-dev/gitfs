//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::error;

mod app_config;
mod daemon;
mod fs;
mod fuse_check;
mod onboarding;
mod term;
mod trc;
mod updates;

use crate::app_config::Config;
use crate::trc::{Trc, TrcHandle};

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

/// Initialize tracing with telemetry support. Exits the process on failure.
#[expect(
    clippy::exit,
    reason = "top-level helper that intentionally terminates the process"
)]
fn init_tracing(config: &Config) -> TrcHandle {
    let handle = Trc::default()
        .with_telemetry(&config.telemetry)
        .init()
        .unwrap_or_else(|e| {
            eprintln!(
                "Failed to initialize logging. Without logging, we can't provide any useful error \
                 messages, so we have to exit: {e}"
            );
            std::process::exit(1);
        });

    if !config.telemetry.endpoints().is_empty() {
        tracing::info!(endpoints = ?config.telemetry.endpoints(), "Telemetry export enabled.");
    }

    handle
}

/// Main entry point for the application.
fn main() {
    let args = Args::parse();

    // Load config first — errors use eprintln since tracing isn't initialized yet.
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

    match args.command.unwrap_or(Command::Run { daemonize: false }) {
        Command::Run { daemonize } => {
            if let Err(e) = fuse_check::ensure_fuse() {
                eprintln!("{e}");
                std::process::exit(1);
            }

            if daemonize {
                run_daemonized(config);
            } else {
                let _trc_handle = init_tracing(&config);
                if let Err(e) = daemon::spawn(config) {
                    error!("Daemon failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Reload => {}
    }
}

/// Run the daemon in the background. Tracing (including OTLP batch exporter
/// threads) is initialized *after* the fork so the exporter threads are
/// created in the child process and survive daemonization.
#[expect(
    clippy::exit,
    reason = "top-level helper that intentionally terminates the process"
)]
fn run_daemonized(config: Config) {
    // Pre-fork: no tracing yet — OTLP BatchSpanProcessor threads would not
    // survive the fork. Use eprintln! for error reporting.
    let pid_file_parent = config
        .daemon
        .pid_file
        .parent()
        .unwrap_or_else(|| unreachable!("Config.validate() ensures pid_file has a parent"));
    if let Err(e) = std::fs::create_dir_all(pid_file_parent) {
        eprintln!("Failed to create PID file directory: {e}");
        std::process::exit(1);
    }

    let log_file = match config.daemon.log.target.open_log_file() {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open log file: {e}");
            std::process::exit(1);
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
                eprintln!("Failed to clone log file handle: {e}");
                std::process::exit(1);
            }
        }
    }

    match daemonize.start() {
        Ok(()) => {
            // Post-fork: safe to start OTLP batch exporter threads.
            let trc_handle = init_tracing(&config);
            trc_handle.reconfigure_for_daemon(config.daemon.log.should_use_color());

            if let Err(e) = daemon::spawn(config) {
                error!("Daemon failed: {e}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Failed to spawn the daemon: {e}");
            std::process::exit(1);
        }
    }
}
