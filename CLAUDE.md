## Verify Changes

After making code changes, always run this sequence:

```
cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet
```

## Code Style & Conventions

- Use `thiserror` for errors.
- Prefer `Result<T, E>` over `.unwrap()` / `.expect()` - handle errors explicitly
- Use `impl Trait` in argument position for simple generic bounds
- Prefer iterators and combinators over manual loops where readable
- Destructure structs at use sites when accessing multiple fields
- Use `#[must_use]` on functions whose return values should not be ignored
- You are NOT ALLOWED to add useless code separators like this:

  ```rust
  // ---------------------------------------------------------------------------
  // Some section
  // ---------------------------------------------------------------------------
  ```

  These are considered bad practice and indicate that the code is not
  well-structured. Prefer using functions and modules to organize your code.

  If you feel the need to add such separators, it likely means that your code
  is too long and should be refactored into smaller, more manageable pieces.

## Module Organization

- One public type per file when the type is complex
- Re-export public API from `lib.rs` / `mod.rs`
- Keep `mod` declarations in parent, not via `mod.rs` in subdirectories (2018 edition style)
- Group imports: std → external crates → crate-internal (`use crate::...`)

## Async / Concurrency

- Runtime: tokio (multi-threaded)
- Prefer `tokio::spawn` for concurrent tasks; use `JoinSet` for structured concurrency
- Use `tokio::select!` for racing futures; always include cancellation safety notes
- Channels: `tokio::sync::mpsc` for multi-producer, `tokio::sync::oneshot` for request-response
- Never block the async runtime — offload blocking work with `tokio::task::spawn_blocking`

## Testing

- Avoid writing tests in-line in the same file as production code; use separate `tests/` directory
  for tests.

## Dependencies

- Check for existing deps with `cargo tree` before adding new crates
- Pin major versions in `Cargo.toml` (e.g., `serde = "1"`)
- Minimize feature flags — only enable what you use
- Audit new deps: check download counts, maintenance status, and `cargo audit`
