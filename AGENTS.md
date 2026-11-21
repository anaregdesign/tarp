# Repository Guidelines

## Project Structure

Keep the public `tarp` API, its package documentation, and its public API tests at the module root. Place the unit tests for each `<filename>.go` in `<filename>_test.go` in the same directory. Root public API tests use `package tarp_test` and import `github.com/anaregdesign/tarp`. Put database operations and compaction in `internal/engine/`. Under `internal/`, `format` defines on-disk encodings, `storage` handles file I/O and recovery, `cache` implements the LRU, and `platform` isolates OS-specific operations.

Keep runnable Go examples with their public API tests. Place measurement drivers in `benchmark/`, qualification commands in `benchmark/cmd/`, and guest scripts in `benchmark/scripts/`. Consult [the format specification](docs/format.md) and [the durability contract](docs/durability.md) before changing persisted data or commit behavior.

During v0, implement the current public API and formats without backward-compatibility shims, legacy readers, or dual writes. Update callers, tests and specifications together. Preserve safe rejection of unsupported input.

## Build, Test, and Development Commands

Run from the repository root with Go 1.26.0 or newer.

| Command | Purpose |
| --- | --- |
| `go build ./...` | Compile packages and tools for the current platform. |
| `go test -count=1 .` | Run the public API tests and examples. |
| `go test -run '^Example$' -v .` | Run the public API example through backup, compaction, and restore. |
| `go test -count=1 -timeout=5m ./...` | Run the full functional suite without cached results. |
| `go test -race -count=1 -timeout=5m ./...` | Check for data races. |
| `go vet ./...` | Run static checks. |

## Coding Style and Naming

Format changed Go files with `gofmt`, which uses tabs for indentation. Follow existing names such as `DB`, `ErrNotFound`, and `OpenWithIO`. Use OS suffixes such as `_linux.go` for platform-specific code. Keep the library, tools, and tests standard-library-only. Verify that `go list -m all` prints only `github.com/anaregdesign/tarp`.

## Testing Guidelines

Run targeted tests on macOS during development. Leave Linux validation to GitHub Actions. Local Linux/Docker checks and full benchmarks require an explicit user request.

Run benchmarks in a container using only the latest stable Go release available at the start of the campaign. Use the same container image digest for all comparison arms and record the digest and exact `go version` in the results.

Use Go’s `testing` package, behavioral test names, and `t.TempDir()` for database files. For decoder changes, run `go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=15s -parallel=2`. Linux CI runs functional and race tests. Run power-loss and disk-exhaustion campaigns only in the [documented disposable VM](docs/qualification.md).

## Commits and Pull Requests

Scope work through GitHub Issues, starting with the [implementation tracker](https://github.com/anaregdesign/tarp/issues/1). Write Issue titles and descriptions in English. Do not use or reinstall `spec-driven-workflow`.

Write short imperative commit subjects such as `Correct measured host-kernel provenance`. Target `main` in pull requests, link the relevant Issue, describe the behavior change, and list the checks run. For durability claims, identify the tested storage configuration and link the qualification evidence.

After each task PR merges, sync local `main` with `origin/main` and delete the completed topic branch locally and on `origin`. For squash merges, verify that each branch tip matches the merged PR head and that the merge commit is on `main`. Preserve branches with newer work, branches used by open PRs, and branches checked out in other worktrees. Prune stale remote-tracking refs and verify the remaining local and remote branches before reporting completion.

## Agent Instructions

In Japanese prose, keep technical terms such as `struct` and `function` in English. If a request conflicts with established best practice, explain the conflict and follow the best practice.
