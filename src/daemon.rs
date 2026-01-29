use tokio::select;

use crate::app_config;
use crate::fs::mescloud::{MesaFS, OrgConfig};
use tracing::{debug, error};

mod managed_fuse {
    //! This module feels a little confusing, but it's designed to help you manage the lifecycle of
    //! fuse slightly better. fuser will not attempt to fuse unmount the filesystem when the
    //! `BackgroundSession` is dropped, and will only do a regular unmount, but we want to be
    //! aggressive and force an unmount if possible.
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;
    use std::time::Duration;

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
                fuser::MountOption::FSName("mesafs".to_owned()),
                fuser::MountOption::RO,
                fuser::MountOption::NoDev,
                fuser::MountOption::Exec,
                fuser::MountOption::AutoUnmount,
                fuser::MountOption::DefaultPermissions,
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
                mount_point: config.mount_point.clone(),
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

            let c_path = match CString::new(self.mount_point.as_os_str().as_bytes()) {
                Ok(p) => p,
                Err(e) => {
                    error!("mount point contains null byte: {e}");
                    return;
                }
            };
            for i in 0..UMOUNT_ATTEMPT_COUNT {
                // TODO(markovejnovic): Migrate to using `nix`
                let umount_errc = {
                    #[cfg(target_os = "macos")]
                    // SAFETY: c_path is a valid CString pointing to the mount path.
                    unsafe {
                        libc::unmount(c_path.as_ptr(), libc::MNT_FORCE)
                    }

                    #[cfg(target_os = "linux")]
                    // SAFETY: c_path is a valid CString pointing to the mount path.
                    unsafe {
                        libc::umount2(c_path.as_ptr(), libc::MNT_DETACH)
                    }
                };

                // TODO(markovejnovic): This error code handling needs to be tested on linux.
                match umount_errc {
                    libc::EBUSY => {
                        debug!(
                            "FUSE filesystem still busy on attempt {}. Retrying...",
                            i + 1
                        );
                        std::thread::sleep(UMOUNT_ATTEMPT_DELAY);
                        continue;
                    }
                    0 | libc::EINVAL => {
                        debug!(
                            "Successfully unmounted FUSE filesystem on attempt {}",
                            i + 1
                        );
                    }
                    _ => {
                        error!(
                            "Failed to unmount FUSE filesystem on attempt {}: {}",
                            i + 1,
                            std::io::Error::last_os_error()
                        );
                    }
                }

                break;
            }
        }
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

    debug!(config = ?config, "Starting mesafs daemon...");

    let fuse = managed_fuse::ManagedFuse::new(&config);
    {
        let _session = fuse.spawn(config, handle.clone())?;

        wait_for_exit().await?;
    }
    Ok(())
}

pub fn spawn(config: app_config::Config) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("Failed to create Tokio runtime: {e}"));
    if let Err(e) = runtime.block_on(run(config, runtime.handle().clone())) {
        error!("Daemon failed: {e}");
        panic!("Daemon failed: {e}");
    }
}
