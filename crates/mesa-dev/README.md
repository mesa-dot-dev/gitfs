# mesa-dev

Rust SDK for the [mesa.dev](https://mesa.dev) API.

## Quick start

```rust
use mesa_dev::{Mesa, models::CreateRepoRequest};

let client = Mesa::new("my-api-key");

// Create a repository
let repo = client
    .repos("my-org")
    .create(&CreateRepoRequest {
        name: "my-repo".to_owned(),
        default_branch: None,
    })
    .await?;

// List all branches
let branches = client
    .branches("my-org", "my-repo")
    .list_all()
    .collect()
    .await?;
```

## Supported resources

| Resource | Accessor | Operations |
|----------|----------|------------|
| Repos | `client.repos(org)` | create, list, get, rename, delete |
| Branches | `client.branches(org, repo)` | create, list, delete |
| Commits | `client.commits(org, repo)` | create, list, get |
| Content | `client.content(org, repo)` | get (files and directories) |
| Diffs | `client.diffs(org, repo)` | get |
| Admin | `client.admin(org)` | API key management |

Paginated endpoints expose a `list_all()` method that returns a `PageStream` with cursor-based iteration.

## HTTP backends

The SDK is generic over its HTTP transport via the `HttpClient` trait.

| Feature | Backend | Async | Default |
|---------|---------|-------|---------|
| `reqwest-client` | [reqwest](https://docs.rs/reqwest) | yes | yes |
| `ureq-client` | [ureq](https://docs.rs/ureq) | no | no |

You can also bring your own backend by implementing `HttpClient`:

```rust
use mesa_dev::{ClientBuilder, HttpClient, HttpRequest, HttpResponse, HttpClientError};

struct MyClient;

impl HttpClient for MyClient {
    async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        todo!()
    }
}

let client = ClientBuilder::new("my-api-key").build_with(MyClient);
```

## Retry

All requests are retried with exponential backoff and jitter (up to 3 attempts by default). Retryable conditions: HTTP 429, 5xx responses, timeouts, and connection errors. Retry parameters are configurable via `ClientBuilder`.
