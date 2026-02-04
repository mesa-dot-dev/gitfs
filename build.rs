//! Build script for git-fs that emits git metadata via vergen.

use vergen_gitcl::{Emitter, GitclBuilder};

/// Emit compile-time git metadata so it is available at runtime.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gitcl = GitclBuilder::default().sha(true).build()?;

    Emitter::default().add_instructions(&gitcl)?.emit()?;

    Ok(())
}
