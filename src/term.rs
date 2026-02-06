//! Random terminal utilities

use std::io::IsTerminal;

fn force_color() -> bool {
    std::env::var_os("FORCE_COLOR").is_some_and(|v| !v.is_empty())
}

fn no_color() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
}

pub fn should_use_color<T: IsTerminal>(stream: &T) -> bool {
    force_color() || (stream.is_terminal() && !no_color())
}
