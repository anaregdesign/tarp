# Safe qualification procedure

**The recorded Linux/ext4 crash and exhaustion campaigns cover the previous
storage implementation.** The final phase-cut matrix used revision `347a1a1`,
the exact source fingerprint and virtual device described in the
[execution matrix](#final-selected-target-execution-matrix). The unversioned
format, incremental publication and default worker require their own evidence
for any VM power-loss or real capacity claim. Ordinary process exits and
deterministic error injection exercise recovery and concurrency on the local
filesystem. Run privileged or disk-filling experiments only on a dedicated,
disposable guest.

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
short writes, corrupt committed bytes, unsupported signatures, and maintenance
overlap/cancellation. Named hooks inject EIO, ENOSPC, EDQUOT, sync/publication,
unlink, and close errors without filling a filesystem.

The cleaner suite uses small fixtures, actor-scoped I/O hooks and barriers for
interrupted replacement, catalog and CURRENT publication, remapping and
retirement. Check that the reopened image, committed user sequence and next
append agree with the model. Include a pinned reader/backup, concurrent
write or rotation, insufficient staging space and a queued writer when
publication is uncertain. These checks are part of the ordinary test suite.

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

## Optional isolated Linux functional testing

Use macOS for local development and GitHub Actions for routine Linux coverage.
Use this Docker procedure only when explicitly requested.

```sh
docker run --rm --network none --cpus 2 --memory 2g --pids-limit 512 \
  --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD:/src:ro" -w /src -e GOTOOLCHAIN=local \
  -e GOMAXPROCS=2 -e GOCACHE=/tmp/gocache golang:1.27.4-bookworm \
  sh -c 'go version && uname -smr && stat -f -c "%T" /tmp &&
    go test -count=1 ./... && go test -race -count=1 ./... &&
    go vet ./... && go list -m all'
```

Select the Go image for the requested reproduction. Source is
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
The pinned Ubuntu kernel/initramfs boot a small guarded static PID 1 and mount
the matching Ubuntu root read-only, without systemd, cloud-init or networking.
This avoids network-wait delays and records the exact minimal guest setup.
The verdict parser accepts complete raw or cloud-init-framed protocol lines
and requires both an explicit pass and a subsequent successful exit; shell
traces, embedded marker text, missing exits and failed exits are not passes.

The optional `controls` mode boots a raw, unmounted 256 MiB data disk behind
the standard-library NBD test backend. Ordinary block writes live only in its
RAM overlay. A successful FLUSH syncs all earlier writes; a successful FUA
syncs only its own range, without persisting unrelated dirty bytes even on the
same 4096-byte cached page. A subsequent FLUSH cannot restore stale bytes over
that FUA range. Unit controls independently read the backing file to cover
these distinctions;
page-cache accounting reports allocated overlay pages, not exact dirty-byte
counts. The guest uses an aligned O_DIRECT write followed by fsync, then a
different O_DIRECT write without sync while retaining the raw descriptor.
After durably recording its ready marker outside the guest, the controller
SIGKILLs the **whole QEMU guest**, waits for NBD quiescence and discards the RAM
overlay. It independently checks the backing file for the surviving flushed
pattern and absent unflushed pattern. The guest-only helper additionally
requires the guarded PID 1 and exact disposable device size before opening it.
This tests the actual virtual-device path, not merely a QEMU cache-mode label.
Its `block-controls.jsonl` and `block-serial.log` are retained alongside the
verdict; it is still not the complete tarp qualification campaign.

Recorded controls:

| Run | Evidence | Limit |
| --- | --- | --- |
| [44279373672](https://github.com/anaregdesign/tarp/actions/runs/44279373672) | Offline Ubuntu 24.04.5 / amd64 6.8.0-147 / ext4 ordered probe passed, with pass **and** successful-exit markers | Environment availability only |
| [44281246556](https://github.com/anaregdesign/tarp/actions/runs/44281246556) | Actual raw-device path: synced pattern survived, acknowledged unsynced pattern was discarded after whole-QEMU SIGKILL and NBD quiescence | One FLUSH, two 512-byte writes, 4096 overlay-page bytes discarded; frontend FUA count zero, not an observed guest FUA test |

### Bounded engine phase groups

`tarpqual -list-cases all` prints the named matrix without starting a guest.
The optional `campaign` mode first reruns the independent device controls,
then executes a selected group (`smoke`, `initialize`, `mutate`, `checkpoint`,
`snapshots`, `recovery`, `exhaustion`, or `all`). A single exact case name can be selected
for reproduction. Each case has a disclosed seed and occurrence, a fresh
256 MiB data disk, and separate cut/recovery guests. Initial formatting uses
4 KiB ext4 blocks, an internal journal, zero reserved-block percentage, and
non-lazy inode/journal initialization; the actual features and ordered mount
are recorded. The recovery guest never reformats.

```sh
gh workflow run qualification.yml --ref REVIEWED_REF \
  -f confirmation=disposable-vm-only -f mode=campaign \
  -f go-version=stable -f group=smoke
```

The manual qualification selector also permits `1.26.x` to resolve that
supported family's latest patch. This is **not** a PR/push CI matrix. Each
manual job is limited to 60 minutes, its controller to 50 minutes/192 cases,
and each guest to four minutes; only one guest runs at a time in a job.
Same-ref jobs serialize per selected mode/family/group. Independent bounded groups
can run on separate disposable hosted runners. Ordinary CI remains one
Linux/latest-stable job.

The external controller syncs every Put/Delete intent, nil acknowledgment and
terminal phase to its own newly created journal before a cut. Acknowledged
deletes remain explicit absent states, so older values cannot resurrect.
Definitive `not-applied` and `committed` error outcomes resolve the pending
intent rather than incorrectly retaining an old/new window. A genuinely unknown
mutation permits its old state at the acknowledged sequence or its whole new
state at the next sequence, not an arbitrary independent combination. Capture markers
bind a backup's separate acknowledged prefix and sequence before later writes.
Reboot verification calls Open using files alone **before reading the oracle**,
then checks values, absence, exact sequence/content binding, complete live-index
cardinality and any published backup or
restore. An acknowledged publication must be present. Early initialization
without CURRENT may instead be explicitly uninitialized; this is never
permitted after the initial frontier's successful directory sync or API return.
Overlapping cases hold a slow backup while reads, updates/deletes and
checkpoint/remapping/reclamation run.

`campaign.jsonl`, immutable per-case oracle/capture files and both console logs
retain the actual phase, seed, device counters, SIGKILL/EOF outcome and recovered
state digest. Successful private raw disks are removed only after validation;
the first failed disk is preserved in the bounded evidence directory. A missing
phase or malformed/partial verdict fails the run. Implementing this matrix does
not itself establish that all its cases have passed.

The first 11-mode actual cut/reboot smoke passed in
[44287402298](https://github.com/anaregdesign/tarp/actions/runs/44287402298).
The full 63-case mutation/rotation group then passed on Go 1.27.4
([44289411337](https://github.com/anaregdesign/tarp/actions/runs/44289411337)) and
Go 1.26.11
([44289418766](https://github.com/anaregdesign/tarp/actions/runs/44289418766)).
These initial runs predate the stricter sequence/cardinality verifier: retain
their raw journals/digests and explicitly distinguish retrospective
sequence/digest reconciliation from a live-index cardinality observation.
They are not, alone, completion of the full qualification matrix.

### Final selected-target execution matrix

The final homogeneous matrix at
[`347a1a1f53be13100a8c83da7378b9af26e4657c`](https://github.com/anaregdesign/tarp/commit/347a1a1f53be13100a8c83da7378b9af26e4657c)
passed **164 phase-cut cases per Go family**, with the strict live verifier in
every case. A separate actual-exhaustion case adds one cut/reboot per family.
These are real whole-QEMU SIGKILLs followed by NBD EOF and the controller's
discard-any-RAM-overlay step, not engine-only process exits. All 328 phase
cuts and both post-exhaustion cuts observed zero dirty/discarded NBD overlay
bytes; complete guest RAM/page-cache loss still occurred. The separate raw
controls actually discarded 4,096 overlay-page bytes while retaining flushed A
and losing unflushed B. Each job reran those actual flushed/
unflushed device controls before testing the engine.

| Group | Cases/family | Go 1.27.4 | Go 1.26.11 |
| --- | ---: | --- | --- |
| Initialization/publication | 25 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44295992231) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44295999801) |
| Put/Delete/reinsert/rotation | 63 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296007208) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296017571) |
| Checkpoint/copy/remap/retirement | 28 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296024514) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296031858) |
| Backup/restore/slow overlap | 34 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296042505) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296049558) |
| Recovery/tail cleanup | 14 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296055896) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44296063691) |
| Actual ENOSPC/headroom/pins/reopen | 1 | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44294167628) | [PASS](https://github.com/anaregdesign/tarp/actions/runs/44294180860) |
| Full eight-package race + guided fuzz | — | [PASS: 3,044 fuzz executions](https://github.com/anaregdesign/tarp/actions/runs/44294173336) | [PASS: 7,588 fuzz executions](https://github.com/anaregdesign/tarp/actions/runs/44294188153) |

The frozen ordinary-library fingerprint is
`52f6ccde9bd3ca02be8e98465994a7130ab50b9fca763e5963b5e9d50c5bf8c8`.
Each artifact contains its relative-path source manifest, the manifest's hash,
exact test-binary and boot hashes, and the checked Git revision. The measurement
revisions recorded in the [historical baseline](measurement-baseline.md) have
this same ordinary-library fingerprint.

Exact target: Ubuntu 24.04.5, amd64 `6.8.0-147-generic`
(`#147-Ubuntu`, package `6.8.0-147.147-generic`, underlying 6.8.12),
QEMU 8.2.2 (`1:8.2.2+ds-0ubuntu1.18`), q35/TCG/CPU max, two virtual CPUs,
1 GiB guest RAM, no NIC/KVM/host block devices, 256 MiB data disk, 4 KiB
ext4 blocks, internal journal and `rw,relatime,data=ordered`; barriers are not
disabled and DAX/quotas are not enabled. Features include `has_journal`,
`ext_attr`, `resize_inode`, `dir_index`, `filetype`, `extent`, `64bit`,
`flex_bg`, `sparse_super`, `large_file`, `huge_file`, `dir_nlink`,
`extra_isize`, and `metadata_csum`. Mounted `needs_recovery` is the normal
journal state. Lazy inode/journal initialization is disabled. SHA-pinned Ubuntu
image/kernel/initramfs URLs and hashes, actual superblock parameters, and mounts
are retained in every raw artifact. The host kernel is not the guest kernel.

The honest block model is part of this qualification boundary. It retains
ordinary writes only in externally controlled RAM, syncs completed pre-FLUSH
writes before replying, and loses the remaining overlay after quiescence.
Actual guest traffic used FLUSH; no guest FUA traffic is claimed. Independent
backing-file and wire tests cover FUA-only persistence across pages, within
one page, and overlapping cached bytes without an implicit unrelated flush.
This models the stated honest-device assumptions; it does not certify any
particular physical SSD/controller's cache or firmware.

Both exhaustion runs observed zero available bytes, a real errno-28 filler
failure, constrained 128 KiB checkpoint headroom with a healthy/NotApplied
failure, and successful retry after releasing 2 MiB. A slow backup retained
1,048,384 obsolete file bytes; completion/cleanup reclaimed exactly those
logical lengths. A subsequent actual admitted-write ENOSPC uniformly fail-stopped
hits/misses/missing reads, preserved the cause across repeated Close, released
ownership, and reopened after removing only the filler. The final cold reboot
verified sequence 256, 128 live keys and capture/restore sequence 128, digest
`d0abd8addaf476805e3cd7a8d6c67cdf2b4459514aea9d7f7b4c0600f21922d1`.
EDQUOT and sync/close/unlink failure semantics have deterministic injected
coverage. In particular, sync-time ENOSPC is injected evidence; the real
bounded-device failures above were observed on writes. No quota-enabled
physical device was tested or is implied.

Retained `qualification-RUN_ID-1` artifacts contain `campaign.jsonl`,
per-case `NNN-oracle.jsonl`, cut/recovery console logs, device controls and
verdicts. The runtime artifacts additionally retain race-binary manifests and
the actual baseline/exec fuzz output. Download artifacts while available;
the manual workflow retains them for 30 days. Preserve raw archives, not just
this summary, for any subsequent release qualification record.

### Selected-target full runtime and fuzz suite

The separate manual `runtime` mode builds every package's race test binary on
the selected Go family and records a bounded package/binary/SHA-256 manifest.
The format package is additionally **compiled** with `-fuzz '^FuzzDecoders$'`;
merely passing `-test.fuzz` to an ordinary binary is not coverage guidance.
Inside the exact verified guest, all full suites run with temporary files on
the disposable ext4 mount, followed by 15 seconds of coverage-guided fuzzing.
Baseline completion and positive execution counts are required and retained.
Opt-in helper environment variables are removed from child suites to prevent
recursive VM/campaign execution. Child commands have three-minute bounds;
the complete guest has a fifteen-minute bound. Host-side vet/module audits
are recorded for the same selected family. This remains a manual qualification
operation, not an older-Go or macOS normal-CI matrix.

The diagnostic-only `tarpprofile` build tag records bounded acquired-lock to
Unlock-entry spans and queued-writer counts for resource characterization.
Probe recording extends actual holds and is excluded from that measured span.
Do not treat instrumented timings as the primary latency baseline. The ordinary
build aliases these mutexes directly to `sync.Mutex`, with no timing/counter
instructions or extra mutex state.

The [selected-target measurement baseline](measurement-baseline.md) records the
completed six-profile, five-window workload matrix on this same stack, including
guest-cold reads/reopens, resource/index/pending-memory probes and maintenance
interference. Those timing results do not broaden the durability boundary.

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
go test -c -o /tmp/tarp-qualification.test ./internal/engine
go build -o /tmp/tarporacle ./benchmark/cmd/tarporacle
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
It durably seeds known values and update/delete history, emits hexadecimal
`TARP_ACK`/`TARP_ACK_DELETE` records, emits at most one pending
`TARP_INTENT`/`TARP_INTENT_DELETE`, and pauses indefinitely at `TARP_PHASE`.
`TARP_CAPTURE`, reader/pin counters and API-return markers are typed metadata.
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

The bounded groups additionally compose initialization, tail-truncation,
acknowledged deletes/reinsertion histories and overlapping readers/writers/slow
backups. Their actual execution and the separate filesystem-exhaustion campaign
are recorded in the final execution matrix above; a deterministic/subprocess
pass does not replace them.

## Bounded-device exhaustion campaign

The manual `campaign` mode's `exhaustion` group implements a real fixture,
not injected ENOSPC. It holds a captured backup, acknowledges a 128-key
4 KiB-value working set and updates, then fills only a regular filler file
on the guarded 256 MiB guest device. Actual ENOSPC and allocated/free bytes
are recorded. Releasing measured 128–256 KiB headroom must make checkpoint
copy fail without publishing or poisoning the source. After releasing another
2 MiB, checkpoint retry must preserve pinned old generations; completing the
backup and retrying cleanup must reclaim them. A second real exhaustion must
surface an admitted mutation's cause/outcome, uniformly fail-stop hot/miss
reads, retain the repeated-Close error, and permit file-only reopen after
removing only the filler. An externally witnessed final whole-VM cut and
separate cold reboot validate all acknowledgments and the captured backup.
The structured exhaustion verdict is mandatory before that phase can pass.

Use only a dedicated disposable bounded filesystem in that VM. Injected space
errors are ordinary tests, not actual exhaustion. The selected no-quota mount's
real allocation/writeback/headroom behavior is recorded above; quota-enabled
filesystems require separate qualification before making such a claim.
Exercise writes and syncs, checkpoint
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

The historical modeled power-cut and actual bounded-filesystem campaigns above
apply to their recorded source fingerprint. The [measurement acceptance](https://github.com/anaregdesign/tarp/issues/5)
and [baseline](measurement-baseline.md) record the earlier measured workload.
Any claims for automatic incremental compaction must identify a new checked
revision, environment, workload, and run evidence.
