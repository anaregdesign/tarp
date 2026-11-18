# Repository Guidelines

## Project Structure

The root Go package, `tarp`, exposes the file-backed key-value store. See `db.go` for reads/writes, `backup.go` for backup/restore, and `cleaner.go` and `compact.go` for maintenance. Under `internal/`, `format` defines on-disk encodings, `storage` handles file I/O and recovery, `cache` implements the LRU, and `platform` isolates OS-specific operations.

Tests live beside their packages in `*_test.go` files. The tools in `cmd/`, harness in `internal/qualification/`, and guest scripts in `.github/qualification/` support storage qualification. Consult [the format specification](docs/format.md) and [the durability contract](docs/durability.md) before changing persisted data or commit behavior.

## Build, Test, and Development Commands

Run from the repository root with Go 1.26.0 or newer.

| Command | Purpose |
| --- | --- |
| `go build ./...` | Compile packages and tools for the current platform. |
| `go test -run '^Example$' -v .` | Run the public API example through backup, compaction, and restore. |
| `go test -count=1 -timeout=5m ./...` | Run the full functional suite without cached results. |
| `go test -race -count=1 -timeout=5m ./...` | Check for data races. |
| `go vet ./...` | Run static checks. |

## Coding Style and Naming

Format changed Go files with `gofmt`, which uses tabs for indentation. Follow existing names such as `DB`, `ErrNotFound`, and `openWithIO`. Use OS suffixes such as `_linux.go` for platform-specific code. Keep the library, tools, and tests standard-library-only. Verify that `go list -m all` prints only `github.com/anaregdesign/tarp`.

## Testing Guidelines

Run targeted tests on macOS during development. Leave Linux validation to GitHub Actions. Local Linux/Docker checks and full benchmarks require an explicit user request.

Use Go’s `testing` package, behavioral test names, and `t.TempDir()` for database files. For decoder changes, run `go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=15s -parallel=2`. Linux CI runs functional and race tests. Run power-loss and disk-exhaustion campaigns only in the [documented disposable VM](docs/qualification.md).

## Commits and Pull Requests

Scope work through GitHub Issues, starting with the [implementation tracker](https://github.com/anaregdesign/tarp/issues/1). Do not use or reinstall `spec-driven-workflow`.

Write short imperative commit subjects such as `Correct measured host-kernel provenance`. Target `main` in pull requests, link the relevant Issue, describe the behavior change, and list the checks run. For durability claims, identify the tested storage configuration and link the qualification evidence.

## Agent Instructions

In Japanese prose, keep technical terms such as `struct` and `function` in English. If a request conflicts with established best practice, explain the conflict and follow the best practice.
