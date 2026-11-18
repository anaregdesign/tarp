# Safe qualification procedure

**Production durability qualification remains open.** Passing deterministic
fault tests, process exits, macOS tests, or Docker overlayfs tests is not proof
of OS-crash/power-loss persistence. Do not execute privileged, disk-filling,
cache-dropping, block-device, or host-power experiments on a shared machine.

## Ordinary repeatable checks

Use latest supported patches from both Go 1.26 and Go 1.27:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go list -m all
go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=15s -parallel=2
```

The only module must be `github.com/anaregdesign/tarp`. Tests/tools use standard
library imports only; the importable root remains package tarp.

`TestProcessCrashRecoveryMatrix` runs 30 bounded subprocess exits across writes,
rotation, checkpoint output/publication/remapping/unlink, backup, and restore.
It checks the acknowledged seed, whole old/new permitted pending states, and
every published artifact's recoverability. `-short` skips that subprocess sweep.
These are process crashes, not power cuts.

Other deterministic tests cover binary/empty values, copying, exact-fit
admission and no-mutation rejection, strict smaller-limit reopen, exact global
LRU and zero-I/O hits, stale miss fills, relocation pins, seeded per-key
linearizability, Close admission/error retention, duplicate/aliased owners,
short writes, corrupt committed bytes, incompatible versions, and maintenance
overlap/cancellation. Named hooks inject EIO, ENOSPC, EDQUOT, sync/publication,
unlink, and close errors without filling a filesystem.

Regression cases explicitly cover generation exhaustion across required
rotation, captured-header checksum/UUID/ID/kind corruption, and retired metadata
removal after successful unlink even if an after-hook reports failure.

## GitHub Actions

The [CI workflow](../.github/workflows/ci.yml) runs the full tests, race detector,
vet, and a single-module dependency audit in one `ubuntu-24.04` (amd64) job using
setup-go's `stable` selector for the latest stable Go release. It covers pull requests to
`main`, pushes to `main`, and manual dispatch. Official checkout/setup-go actions
are pinned to verified release commits; job permissions are read-only, jobs have
finite timeouts, module downloads are disabled, and no credentials persist in
the checkout. No privileged, disk-filling, cache-dropping, or power-cut steps run.

Retain the run URL, checked commit, resolved Go patches, and runner details when
reporting results. Hosted Linux amd64 runtime coverage closes an architecture
coverage gap; it does not qualify the selected ext4/storage stack against power
loss or real device exhaustion. Older supported Go families and macOS are not
part of automated CI; their manual development checks do not change the module
minimum or the production support boundary.

## Safe isolated Linux functional testing

With an available Docker server and Go image:

```sh
docker run --rm --network none --cpus 2 --memory 2g --pids-limit 512 \
  --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD:/src:ro" -w /src -e GOTOOLCHAIN=local \
  -e GOMAXPROCS=2 -e GOCACHE=/tmp/gocache golang:1.27.4-bookworm \
  sh -c 'go version && uname -smr && stat -f -c "%T" /tmp &&
    go test -count=1 ./... && go test -race -count=1 ./... &&
    go vet ./... && go list -m all'
```

Repeat with the corresponding latest supported Go 1.26 image. Source is
read-only, data lives in disposable container /tmp, there is no network/capability
or host-device access. Record architecture/kernel/filesystem: LinuxKit arm64
overlayfs is useful functional coverage, **not** Linux amd64/ext4 qualification.

## External disposable-VM phase-barrier harness

An opt-in `VM qualification` Actions workflow first probes a disposable QEMU
guest using a SHA-256-pinned Ubuntu 24.04 image, its 6.8-series kernel, and a
256 MiB ext4 `data=ordered` device. It is manual-only, requires the exact
`disposable-vm-only` confirmation, is time/file bounded, uses TCG without host
device access, and retains host/image/guest configuration and a verdict.
Its scripts refuse local execution. It does not change ordinary Linux/stable CI.

The initial environment probe is **not qualification**: QEMU writeback alone
does not model loss of the host page cache. Flushed-survives/unflushed-is-lost
controls on a verified volatile block backend and the full external-witness
campaign must pass before any power-loss acceptance criterion is completed.

Only a dedicated disposable Linux VM/block device and an independent durable
controller are suitable. Record the exact selected amd64 Ubuntu 24.04 6.8-series
kernel/ext4 `data=ordered` stack, mount/features, controller/device flush policy,
Go patch, machine image, phase, occurrence, and artifact digests.

Build the guest test binary and controller verifier from the same source:

```sh
go test -c -o /tmp/tarp-qualification.test .
go build -o /tmp/tarporacle ./cmd/tarporacle
```

Those binaries must match their respective execution architectures. The
controller runs **outside** the guest crash domain. Its oracle path must be
on durable storage outside that domain. Do not store it in the engine directory.

Example controller pipeline, using an already configured disposable guest:

```sh
set -o pipefail
ssh disposable-tarp-guest env \
  TARP_POWERCUT_CONFIRM=disposable-vm-only \
  TARP_POWERCUT_DIR=/mnt/disposable-tests/case-001-db \
  TARP_POWERCUT_POINT=current.rename:after \
  TARP_POWERCUT_MODE=put \
  /tmp/tarp-qualification.test \
  -test.run='^TestPowerCutGuest$' -test.v -test.timeout=0 \
  | /tmp/tarporacle -record ./case-001-oracle.jsonl
```

The guest requires a fresh, nonexistent directory in an existing test parent.
It durably seeds a known value, emits hexadecimal `TARP_ACK` records, emits at
most one pending `TARP_INTENT`, and pauses indefinitely at `TARP_PHASE`.
`tarporacle` creates a new 0600 JSONL log, syncs its file and parent before use,
then syncs every event **before** printing `durable ...`. It never overwrites
an oracle or invokes a hypervisor/power command.

Only after the controller reports the **durable phase** may an operator use
the approved VM controller to abruptly power off that guest. Do not target the
development host. The VM/block-device model must discard volatile unflushed
state while honoring flushes; graceful shutdown, SIGKILL, or resetting a guest
while preserving all host write caches does not model power loss adequately.
The broken SSH/pipeline exit is expected and must be recorded, not called PASS.

After restarting the same disk image, copy the immutable controller log to a
verification location outside the engine directory, then run in the guest:

```sh
/tmp/tarporacle -verify /oracle/case-001-oracle.jsonl \
  -dir /mnt/disposable-tests/case-001-db
```

The engine first recovers from its files alone. The oracle then checks every
observed acknowledgment, allowing only old/absent or whole new state for one
pending Put. It is test evidence, never recovery input. The supplied protocol
is deliberately phase-controlled: it is not a general distributed workload
controller and does not claim arbitrary random-cut intent handshakes.

Sweep before/after points for `data.write`, `data.sync`, `current.write`,
`current.sync`, `current.rename`, and `current.dirsync`. Additional guest modes:

| Mode | Additional setup/output | Useful boundaries |
| --- | --- | --- |
| `rotate` | Acknowledged padding forces the pending Put to rotate | `segment.write`, `segment.sync`, `catalog.write`, `catalog.sync`, `catalog.dirsync`, frontier |
| `compact` | Seeded obsolete version, no pending logical change | `compact.segment.write`, `compact.write`, `compact.sync`, second `current.rename`, `compact.remap:after`, `cleanup.unlink` |
| `backup` | Set `TARP_POWERCUT_OUTPUT` to a new external destination | `backup.copy`, `backup.file.sync`, `backup.manifest.sync`, `backup.rename`, `backup.parent.sync` |
| `restore` | Creates a stable source backup before arming; set new output path | `restore.copy`, `restore.file.sync`, `restore.stage.sync`, `restore.rename`, `restore.parent.sync` |

Use `TARP_POWERCUT_N=2` where the second occurrence is intended (for example
checkpoint publication after its preliminary rotation). A missing phase is an
explicit failure. Every case uses a new directory and fresh output/oracle paths.
Backup/restore output may be absent before an unacknowledged publication; if
present it must independently restore/open and match the journal's seeded state.
Validate backups with the public Restore API into another new destination,
then use tarporacle against that restored database.

Initialization, tail-truncation, acknowledged deletes/reinsertion histories,
concurrent readers/writers/slow backups, and every filesystem/storage boundary
still require composed VM campaign cases. The deterministic/subprocess harness
is a starting matrix, not evidence that these external campaigns happened.

## Bounded-device exhaustion campaign

Use only a dedicated disposable bounded filesystem in that VM. Injected space
errors are already ordinary tests; actual delayed allocation/quota/writeback
behavior must also be qualified there. Exercise writes and syncs, checkpoint
headroom, pinned backup generations, cleanup failure/retry, and recovery after
adding space. Record observed causes/outcomes, source health, selected files,
and recovered acknowledged state. Never fill the shared host or delete live
engine files to manufacture headroom.

## Release gate

Retain raw logs, source revision/tree digest, seeds, oracle journals, exact
environment/storage parameters, observed acknowledgments, allowed outcomes,
and reopened/restore verdicts. Distinguish:

| Evidence class | What it establishes |
| --- | --- |
| Deterministic errors/model interleavings | Explicit error/state/concurrency contracts |
| Process exits | File-only recovery of process-visible persisted states |
| Docker/macOS runtime checks | Development portability and functional behavior |
| Actual modeled VM power cuts | Only the precisely tested durability configuration |
| Actual bounded-device exhaustion | Only the recorded allocation/writeback/headroom behavior |

No selected production power-cut campaign or real bounded-device exhaustion
has been performed by the ordinary development checks. Keep
[#8](https://github.com/anaregdesign/tarp/issues/8) and dependent release
qualification criteria open until real, reviewable evidence exists.
