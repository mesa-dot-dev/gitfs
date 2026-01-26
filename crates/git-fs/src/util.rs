#[macro_export]
macro_rules! critical_bug {
    ($($arg:tt)*) => {
        panic!("Critical error in Mesa. Please report this on https://github.com/mesa-dot-dev/git-fs: {}", format_args!($($arg)*))
    };
}

pub use critical_bug;
