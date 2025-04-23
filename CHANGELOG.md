## Changelog

### [0.2.0] - 2025-04-20

#### ✨ Features

#### ⚡ Optimizations

*   **Streaming Upload:** Object synchronization now uses streaming directly from `GetObject` to `UploadObject`, eliminating the need to load the entire object content into memory before uploading.
    *   Removed the use of `io.ReadAll` and `strings.NewReader` / `bytes.NewReader` for the intermediate buffer in the main synchronization flow.
    *   (Affects: `internal/sync/sync.go`)

#### ♻️ Refactoring

*   **Atomic Counters:** The counters for synchronized, skipped, and errored objects (`syncCounter`, `skipCounter`, `errorCounter`) now use `sync/atomic.Int64` to ensure safety in concurrent environments.
    *   (Affects: `internal/sync/sync.go`)
*   **Context Management:** Used `errgroup.WithContext` to propagate context cancellation to synchronization goroutines, allowing for faster shutdown in case of errors or external signals.
    *   (Affects: `internal/sync/sync.go`)