//! FUSE availability checks for macOS.

#[cfg(target_os = "macos")]
use std::path::Path;

#[cfg(target_os = "macos")]
mod paths {
    pub const MACFUSE_FS_BUNDLE: &str = "/Library/Filesystems/macfuse.fs";
    pub const OSXFUSE_FS_BUNDLE: &str = "/Library/Filesystems/osxfuse.fs";
    pub const MACFUSE_MOUNT_HELPER: &str =
        "/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse";
    pub const OSXFUSE_MOUNT_HELPER: &str =
        "/Library/Filesystems/osxfuse.fs/Contents/Resources/mount_osxfuse";
    pub const LIBFUSE_DYLIB: &str = "/usr/local/lib/libfuse.2.dylib";
    pub const MACFUSE_FSKIT_APPEX: &str = "/Library/Filesystems/macfuse.fs/Contents/Resources/\
         macfuse.app/Contents/Extensions/\
         io.macfuse.app.fsmodule.macfuse.appex";
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuseProvider {
    MacFuse,
    OsxFuse,
}

#[cfg(target_os = "macos")]
impl FuseProvider {
    fn detect() -> Option<Self> {
        if Path::new(paths::MACFUSE_FS_BUNDLE).is_dir() {
            Some(Self::MacFuse)
        } else if Path::new(paths::OSXFUSE_FS_BUNDLE).is_dir() {
            Some(Self::OsxFuse)
        } else {
            None
        }
    }

    const fn mount_helper_path(self) -> &'static str {
        match self {
            Self::MacFuse => paths::MACFUSE_MOUNT_HELPER,
            Self::OsxFuse => paths::OSXFUSE_MOUNT_HELPER,
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_major_version() -> Option<u32> {
    let mut buf = [0u8; 32];
    let mut len = buf.len();
    let key = c"kern.osproductversion";
    // SAFETY: read-only sysctl query with stack-allocated buffer that outlives the call.
    let ret = unsafe {
        libc::sysctlbyname(
            key.as_ptr(),
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if ret != 0 || len == 0 {
        return None;
    }
    std::str::from_utf8(&buf[..len.saturating_sub(1)])
        .ok()?
        .split('.')
        .next()?
        .parse()
        .ok()
}

/// Errors that can occur when verifying FUSE availability.
#[derive(Debug, thiserror::Error)]
#[expect(
    variant_size_differences,
    reason = "largest variant holds a &str — boxing would add indirection for no benefit"
)]
pub enum FuseCheckError {
    /// macFUSE is not installed at all.
    #[error(
        "macFUSE is not installed. git-fs requires macFUSE to mount filesystems.\n\
         Install it from: https://macfuse.github.io/"
    )]
    NotInstalled,

    /// The mount helper binary is missing.
    #[error(
        "macFUSE mount helper not found at {path}. Installation may be corrupt.\n\
         Reinstall from: https://macfuse.github.io/"
    )]
    MountHelperMissing {
        /// Path where the mount helper was expected.
        path: &'static str,
    },

    /// The libfuse shared library is missing.
    #[error(
        "macFUSE library missing at /usr/local/lib/libfuse.2.dylib. \
         macFUSE may have been partially uninstalled.\n\
         Reinstall from: https://macfuse.github.io/"
    )]
    LibfuseMissing,

    /// No kernel extension or `FSKit` appex found for the running macOS version.
    #[error(
        "No macFUSE kernel extension found for macOS {macos_version}. \
         You may need to update macFUSE or reboot.\n\
         Update from: https://macfuse.github.io/\n\
         After installing, allow the system extension in \
         System Settings > Privacy & Security."
    )]
    KextMissing {
        /// The major macOS version number (e.g. 15 for Sequoia).
        macos_version: u32,
    },
}

/// Verify that FUSE is installed and usable on the current platform.
///
/// On macOS this checks for macFUSE or osxfuse, including the mount helper,
/// libfuse dylib, and kernel extension / `FSKit` appex. On other platforms this
/// is a no-op.
#[cfg(target_os = "macos")]
pub fn ensure_fuse() -> Result<(), FuseCheckError> {
    let provider = FuseProvider::detect().ok_or(FuseCheckError::NotInstalled)?;

    let helper = provider.mount_helper_path();
    if !Path::new(helper).is_file() {
        return Err(FuseCheckError::MountHelperMissing { path: helper });
    }

    if !Path::new(paths::LIBFUSE_DYLIB).exists() {
        return Err(FuseCheckError::LibfuseMissing);
    }

    if provider == FuseProvider::MacFuse
        && let Some(major) = macos_major_version()
    {
        let kext_path =
            format!("/Library/Filesystems/macfuse.fs/Contents/Extensions/{major}/macfuse.kext");
        if !Path::new(&kext_path).is_dir() && !Path::new(paths::MACFUSE_FSKIT_APPEX).is_dir() {
            return Err(FuseCheckError::KextMissing {
                macos_version: major,
            });
        }
    }

    Ok(())
}

/// Verify that FUSE is installed and usable on the current platform.
///
/// On non-macOS platforms this is a no-op.
#[cfg(not(target_os = "macos"))]
pub fn ensure_fuse() -> Result<(), FuseCheckError> {
    Ok(())
}
