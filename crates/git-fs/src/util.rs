/// Panics with an error message asking the user to report a bug.
#[macro_export]
macro_rules! critical_bug {
    ($($arg:tt)*) => {
        panic!("Critical error in Mesa. Please report this on https://github.com/mesa-dot-dev/git-fs: {}", format_args!($($arg)*))
    };
}

