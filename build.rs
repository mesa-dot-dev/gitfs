//! Build script for git-fs that emits git metadata via vergen.

/// Emit compile-time git metadata so it is available at runtime.
///
/// If `GIT_SHA` is set (e.g. by CI from `github.sha`), use that directly.
/// Otherwise fall back to vergen-gitcl which discovers the SHA from the local
/// `.git` directory.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(sha) = std::env::var("GIT_SHA") {
        println!("cargo:rustc-env=VERGEN_GIT_SHA={sha}");
    } else {
        use vergen_gitcl::{Emitter, GitclBuilder};
        let gitcl = GitclBuilder::default().sha(true).build()?;
        Emitter::default().add_instructions(&gitcl)?.emit()?;
    }

    Ok(())
}
