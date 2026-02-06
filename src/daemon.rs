use tokio::select;

use crate::app_config;
use crate::fs::mescloud::{MesaFS, OrgConfig};
use tracing::{debug, error, info};

mod managed_fuse {
    //! This module feels a little confusing, but it's designed to help you manage the lifecycle of
    //! fuse slightly better. fuser will not attempt to fuse unmount the filesystem when the
    //! `BackgroundSession` is dropped, and will only do a regular unmount, but we want to be
    //! aggressive and force an unmount if possible.
    use std::path::PathBuf;
    use std::time::Duration;

    use nix::errno::Errno;

    use super::{MesaFS, OrgConfig, app_config, debug, error};
    use crate::fs::fuser::FuserAdapter;
    use fuser::BackgroundSession;

    pub struct FuseCoreScope {
        _session: BackgroundSession,
    }

    impl FuseCoreScope {
        fn spawn(
            config: app_config::Config,
            handle: tokio::runtime::Handle,
        ) -> Result<Self, std::io::Error> {
            Ok(Self {
                _session: Self::spawn_fuse(config, handle)?,
            })
        }

        fn spawn_fuse(
            config: app_config::Config,
            handle: tokio::runtime::Handle,
        ) -> Result<BackgroundSession, std::io::Error> {
            let orgs = config
                .organizations
                .iter()
                .map(|(org_name, org)| OrgConfig {
                    name: org_name.clone(),
                    api_key: org.api_key.clone(),
                });
            let mesa_fs = MesaFS::new(orgs, (config.uid, config.gid));
            let fuse_adapter = FuserAdapter::new(mesa_fs, handle);
            let mount_opts = [
                fuser::MountOption::FSName("git-fs".to_owned()),
                fuser::MountOption::RO,
                fuser::MountOption::NoDev,
                fuser::MountOption::Exec,
                fuser::MountOption::AutoUnmount,
                fuser::MountOption::DefaultPermissions,
                fuser::MountOption::AllowOther,
            ];

            fuser::spawn_mount2(fuse_adapter, config.mount_point, &mount_opts)
        }
    }

    pub struct ManagedFuse {
        mount_point: PathBuf,
    }

    impl ManagedFuse {
        pub fn new(config: &app_config::Config) -> Self {
            Self {
                mount_point: config.mount_point.to_path_buf(),
            }
        }

        pub fn spawn(
            &self,
            config: app_config::Config,
            handle: tokio::runtime::Handle,
        ) -> Result<FuseCoreScope, std::io::Error> {
            _ = self; // self used for calling convention.
            FuseCoreScope::spawn(config, handle)
        }
    }

    impl Drop for ManagedFuse {
        fn drop(&mut self) {
            const UMOUNT_ATTEMPT_COUNT: usize = 10;
            const UMOUNT_ATTEMPT_DELAY: Duration = Duration::from_millis(10);

            debug!(mount_point = ?self.mount_point, "Confirming unmount of FUSE filesystem...");

            for i in 0..UMOUNT_ATTEMPT_COUNT {
                let result = {
                    #[cfg(target_os = "macos")]
                    {
                        nix::mount::unmount(&self.mount_point, nix::mount::MntFlags::MNT_FORCE)
                    }

                    #[cfg(target_os = "linux")]
                    {
                        nix::mount::umount2(&self.mount_point, nix::mount::MntFlags::MNT_DETACH)
                    }
                };

                match result {
                    Ok(()) => {
                        debug!(
                            "Successfully unmounted FUSE filesystem on attempt {}",
                            i + 1
                        );
                        break;
                    }
                    Err(Errno::EBUSY) => {
                        debug!(
                            "FUSE filesystem still busy on attempt {}. Retrying...",
                            i + 1
                        );
                        std::thread::sleep(UMOUNT_ATTEMPT_DELAY);
                    }
                    Err(Errno::EINVAL | Errno::ENOENT) => {
                        debug!("FUSE filesystem already unmounted (attempt {})", i + 1);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Failed to unmount FUSE filesystem on attempt {}: {}",
                            i + 1,
                            e
                        );
                        break;
                    }
                }
            }
        }
    }
}

/// Prepares the mount point directory.
///
/// - If the directory exists and is non-empty, returns an error.
/// - If the directory does not exist, creates it (including parents) and logs an info message.
/// - If the directory exists and is empty, does nothing.
async fn prepare_mount_point(mount_point: &std::path::Path) -> Result<(), std::io::Error> {
    match tokio::fs::read_dir(mount_point).await {
        Ok(mut entries) => {
            if entries.next_entry().await?.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "Mount point '{}' already exists and is not empty.",
                        mount_point.display()
                    ),
                ));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(mount_point).await?;
            info!(path = %mount_point.display(), "Created mount point directory.");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn wait_for_exit() -> Result<(), std::io::Error> {
    use tokio::signal;
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut sighup = signal::unix::signal(signal::unix::SignalKind::hangup())?;
    select! {
        _ = signal::ctrl_c() => {
            debug!("Received Ctrl+C signal, shutting down...");
        },
        _ = sigterm.recv() => {
            debug!("Received termination signal, shutting down...");
        },
        _ = sighup.recv() => {
            debug!("Received hangup signal, shutting down...");
        },
    }
    Ok(())
}

/// Main entry point for the daemon.
pub async fn run(
    config: app_config::Config,
    handle: tokio::runtime::Handle,
) -> Result<(), std::io::Error> {
    // Spawn the cache if it doesn't exist.
    tokio::fs::create_dir_all(&config.cache.path).await?;

    prepare_mount_point(&config.mount_point).await?;

    info!("Mounting filesystem at {}.", config.mount_point.display());

    let fuse = managed_fuse::ManagedFuse::new(&config);
    {
        let _session = fuse.spawn(config, handle.clone())?;
        info!("git-fs is running. Press Ctrl+C to stop.");

        wait_for_exit().await?;
    }
    Ok(())
}

pub fn spawn(config: app_config::Config) -> Result<(), std::io::Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("Failed to create Tokio runtime: {e}"));
    runtime.block_on(run(config, runtime.handle().clone()))
}
