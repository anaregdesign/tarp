# Current-engine durability qualification

Tracking: [Issue #168](https://github.com/anaregdesign/tarp/issues/168).

## Latest selected candidate: 50b140a

Updated 2026-11-23. **Power-cut matrix passed; aggregate v1 durability gate
remains incomplete because the Go 1.26 guided-fuzz job failed twice.**
The six previously missing dedicated scenarios have now been executed.
This is not a release authorization or a claim that every check passed.

The checked engine and harness source is
[`50b140ad873d1cbf8f9f01fe0fc900578e83ff77`](https://github.com/anaregdesign/tarp/commit/50b140ad873d1cbf8f9f01fe0fc900578e83ff77).
Every dispatch and run SHA was checked. Later documentation-only commits do
not change the tested source identity. The resolved Go patches were 1.27.4
and 1.26.11. All 24 bounded power-cut campaigns passed independent
reconciliation: **584 engine cuts and separate cold reboots per family,
1,168 total**, including four post-exhaustion cuts. The extension adds
427 cases per family to the 157-case baseline. The 26 selected runs include
two runtime jobs; 25 runs passed and one runtime job failed on both attempts.

| Verification item | Cases/family | Go 1.27.4 | Go 1.26.11 | Comment |
| --- | ---: | --- | --- | --- |
| Initialization | 25 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121643573) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121742526) | Explicit uninitialized outcome only before the initial durable frontier. |
| Single-key mutations and rotation | 57 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121651099) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121750052) | Acknowledged values/deletes and permitted pending endpoints. |
| Source-order DATA checkpoint | 26 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121658839) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121757149) | Output/publication/remap/retirement. |
| Backup/restore | 34 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121666561) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121764792) | Captured outputs verified at the witnessed sequence. |
| Recovery and read/write/backup overlap | 14 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121673849) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121773663) | Tail recovery and pinned concurrent activity. |
| Actual DATA/checkpoint ENOSPC | 1 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121682335) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121782312) | Real errno 28, fail-stop/healthy-failure distinctions, retry and cold boot. |
| BatchPut/BatchDelete | 100 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121690515) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121790966) | Whole-batch external oracle, duplicates, delete/reinsert and rotation. |
| Automatic cleaner/grouped cleaning/repacking | 138 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121698348) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121799818) | Actual scheduler modes, concurrent tail/checked reads, backup pins and retirement. |
| Ordered initial/delta/reused-range checkpoint | 114 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121705441) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121807566) | Sorted output, exact original sequences and unchanged DATA identities. |
| INDEX publication and Indexed reopen | 42 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121712830) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121815326) | Explicit CheckpointIndex and B+tree Close; independent Full/Indexed copies. |
| Online DATA/PARITY repair interruption | 32 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121720494) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121823121) | Controlled fsynced corruption; DATA cases explicitly retry file-only repair before Open. |
| Actual B+tree private-pager ENOSPC | 1 | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121727918) | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121831528) | Scratch-write errno 28, NotApplied/healthy rollback, retry and cold boot. |
| Full 15-package race suite | - | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45121735072) | [Package suites passed; enclosing job failed](https://github.com/anaregdesign/tarp/actions/runs/45121839197/attempts/2) | Both failed 1.26 runtime attempts completed every package suite before fuzz failed. |
| Guided decoder fuzz, 15 seconds | - | Passed: 4,040 executions | **Failed twice**: 4,250 / 4,064 executions | Both 1.26 attempts reported `context deadline exceeded`; not accepted as runtime passes. |
| Vet/module audit | - | Passed | Passed | Same-source runtime setup; standard library only. |
| Ordinary Linux CI | - | [Passed](https://github.com/anaregdesign/tarp/actions/runs/45122036340) | - | Exact checked source, separate from power-loss evidence. |
| Aggregate v1 durability gate | - | **Incomplete** | **Incomplete** | Resolve the repeatable 1.26 fuzz failure before claiming the complete gate. |

### Extended acceptance and experimental boundary

Each selected run passed an independent raw-device control: synced A survives,
unsynced B disappears. These 26 controls are not engine cases. The additional
failed-runtime retry has its own control and is not added to the selected
campaign totals. The stack remains Ubuntu 24.04.5 amd64, guest kernel
6.8.0-147-generic, QEMU 8.2.2 TCG, two CPUs/1 GiB RAM, guarded 256 MiB ext4
`data=ordered`, normal barriers/journal, no DAX/quotas, and honest-flush
volatile NBD. Image SHA-256:
`04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94`.
Host kernels are recorded separately from the guest kernel.

The new batch, cleaner, ordered, explicit INDEX and repair modes exercise both
Memory and B+tree. INDEX-on-Close and private-pager exhaustion intentionally
use B+tree only; Memory Close does not publish an INDEX checkpoint. The original
157-case matrix still uses its original default-index fixtures. This is not
a complete cross-product of every original phase with both index modes.

The independent verifier rejects intermediate batch sequences and mixed
old/new states and recomputes complete live cardinality/state digests.
Extended recovery verifies independent copies with Full and Indexed Open,
before either Open/Close can alter the other copy. Across the new scenarios,
84 cold recoveries actually selected an INDEX checkpoint. Cleaner cases
validate their captured backup output and overlap checked reads and an
acknowledged tail write. Completed ordered delta/reuse cases witness retained
DATA identities. There are 32 explicit offline DATA-repair retries, not a
claim that Full Open automatically heals damaged files.

Both pager campaigns require an actual `tarp-index-*` scratch-file write
ENOSPC, positive preparation count, zero DATA writes, unchanged sequence,
healthy NotApplied outcome and no staged visible keys. Freeing space permits
the 1,536-record retry, followed by external cut/cold recovery. The legacy
`device_exhaustion` field refers only to the separate DATA exhaustion fixture;
the pager proof is the dedicated `pager_verdict`, not that legacy field.

Eighteen accepted engine cuts discarded nonzero volatile overlay, totaling
270,336 allocated overlay-page bytes (not distinct user-value bytes).
All 1,168 cuts lost complete guest RAM/page cache. Observed guest FUA traffic
was zero. Backend termination was EOF in 1,153 cases, exact case-socket
connection reset after SIGKILL in 10, and exact case-socket closed-connection
after cancellation in five. The controller joins the backend before dropping
overlay state. The independent verifier recognizes only those observed,
case-matched forms, with tests rejecting other sockets and arbitrary I/O
errors; it does not turn a live backend failure into a successful cut.

No physical SSD/controller/firmware certification, lying-flush tolerance,
other filesystem/kernel, quota-enabled storage or generic macOS power-loss
guarantee is implied. Automatic-maintenance ENOSPC is not established by the
manual-checkpoint exhaustion fixture. These scope exclusions are not
silently treated as passed tests.

### Failed attempts and unresolved blocker

The preliminary [pager run at de0dd8c](https://github.com/anaregdesign/tarp/actions/runs/45121370098)
failed because the controller nested its pager success-marker handler inside
an unrelated exhaustion branch. Guest scratch ENOSPC/rollback/retry succeeded,
but that run is **not** accepted as a completed cut/cold boot. Commit
`50b140a` corrected routing; both selected pager campaigns then passed.
The [focused cleaner retirement run](https://github.com/anaregdesign/tarp/actions/runs/45121374657)
also passed at `de0dd8c`. Neither preliminary run is in the 1,168 total.

Go 1.26 runtime [attempt 1](https://github.com/anaregdesign/tarp/actions/runs/45121839197/attempts/1)
and [identical-source attempt 2](https://github.com/anaregdesign/tarp/actions/runs/45121839197/attempts/2)
failed guided fuzz after 17.45 and 17.14 seconds, respectively, with
`context deadline exceeded`. Both reached coverage-guided fuzzing after all
15 package race suites passed. No decoder panic or failing corpus input was
reported. The source, assertions and time limits were unchanged on retry.
**The root cause is unresolved; this record does not dismiss the error as
an infrastructure flake or report an all-green runtime suite.**
Further diagnosis of this repeatable failure remains a release blocker.

### Latest evidence retention

All downloaded archives were checked against GitHub artifact digests.
Independent Python reconciliation accepts 25 selected runs, including all
24 engine campaigns and the stable runtime job; its one expected error
explicitly preserves the failed 1.26 runtime job. A separate failed-runtime
verifier checks both original archives, source manifests, controls, executed
package/binary identities, all package PASS results and the fuzz failure.
Nine focused oracle/termination verifier tests passed.

Common accepted library-source manifest fingerprint:
`5b25d4a553f7cea56720c0955c9f9730ecf0fc22c68b20a1d6645ed55a64d01d`.
Retained checked-source tarball SHA-256:
`44238d3eba89cbdcd96df4db0aa06deca367c6f1cc5a2bcafb7ea043e7156f79`.

The session-retained `raw-evidence-50b140a.zip` contains original archives
and logs for every selected run, both failed runtime attempts, both preliminary
runs, source archives, dispatch/run/artifact metadata, case catalog,
per-file hashes and the independent verifiers/results. Its sibling `.sha256`
records the final bundle digest. `bundle-manifest.json` explicitly says the
aggregate release gate is incomplete. Actions retention remains 30 days;
retain the downloaded originals beyond that window.

| Selected raw archive run | SHA-256 |
| --- | --- |
| 45121643573 | `2de98cef1c4d4841b265bc0b74501e696c8e8c2ca6ed35112948a53e78788cf5` |
| 45121651099 | `a4b7dfbda628719b449ded000f1e0f8913f884f8c48a89a87b4bc87e91a645d2` |
| 45121658839 | `111e80b930ebf4f25f0f4d4df0840d951caac720e770052c1f63f119c9cf35ce` |
| 45121666561 | `54786d4ee16fd4d494aa6b9859773a08a3412a5f30f37a10d20cc42316e8d6c6` |
| 45121673849 | `146680a39d4dea232020cb06dfdd933a3fb24047dfc2665ee73c7261ce07ed83` |
| 45121682335 | `d5a11eb2689d210bf0b6087d574b6149264ed385d99c4c5b19e1227dd8d68379` |
| 45121690515 | `28ef77ba9c41da953d9342d9e4ccdddebebb79b39d390552f5a4dd2fd7ed9482` |
| 45121698348 | `cdcb9d7e9612ab822b6bc0b7b5a2d6d506e1fea43eda9a0e333f80a21dca5f66` |
| 45121705441 | `f9e47745af18431dbb0f45bc2e531138aa9465df9c85b8a418be27846aa1e08f` |
| 45121712830 | `abdcde439bb1b1224f07ad9ec6d89860a4ccd285700e5acd2da4215ca22f7d72` |
| 45121720494 | `730eb04cfc65046980f57397f1bc1c5dcdfd4fdf4ab241e6c69d9e14b82d51e4` |
| 45121727918 | `d779151f5709d9b6422f7ef8e871908f115b5320a516b3d5f40e0a8babebd731` |
| 45121735072 | `5cbb107a7973d85a9384c639ac33426f19b6ac67625fdfd549e77bf569294670` |
| 45121742526 | `003ac1c36426ebb7c7ad22ca34ae730c83ac58992c81499aec0c7befb6b40ecc` |
| 45121750052 | `b9a82780b49703098e78286cc258912466a122535cd6d9bc5ec542b54ff4f672` |
| 45121757149 | `3e46882c150f346520704f0e65b0a6c708e07003f071fd5f516a3b07921fdbdb` |
| 45121764792 | `159d542e7bdea32e8ce92b01c26c04918e85b3aa1637b5a2497df1847ccfcc47` |
| 45121773663 | `b2938dab817491e2a24fa9aeb898f6be428649199f2b1d1448b987ff096355cb` |
| 45121782312 | `6292472b0af1366c516b837113ab1ff6ec9401d2f2d168cc0378b6e885ee6fa7` |
| 45121790966 | `14d9c097e7c6ea656be751e54c4c743f887392efd50838db29452c68d96dee46` |
| 45121799818 | `55f268f354b5bc26af28646c6007ac28288a8ebabb78621740c2e83ddae1957b` |
| 45121807566 | `923a4d11cc5a56e837759f37f8ad317d5cf9eed12f26d7e58b842e03b06a32ea` |
| 45121815326 | `e8661e5c934ab47d2e4f900a967b010100157ced1966427ab73102d7f8d75378` |
| 45121823121 | `26116fa5ff99940cdab36c8e6fc0c0e07df2dddea4359909da01eb8e35c87975` |
| 45121831528 | `671af43d69e1c25c0c1c8e2ae729cc55e0c8570b9a5e159cb13e5f1824e846df` |
| 45121839197 attempt 2 (failed) | `b42eafd055422ebec09ede80f5f6b4581778e86583b9e6cdcc70e5d0a76c8a8c` |
| 45121839197 attempt 1 (failed, retained separately) | `907e77fb1e88eb0beac7ea4c95a11d932637e65dbb2b9a3ed4a497280a375983` |

## Historical baseline: 73d5cb8

The sections below preserve the original baseline results and its then-missing
coverage. They are historical, not the current status table; the six dedicated
gaps below were executed in the latest campaign above.

### Checked source and scope

This campaign checks published revision
[`73d5cb8f73e78cb7701f5cb0bec5a3f8108e8dfc`](https://github.com/anaregdesign/tarp/commit/73d5cb8f73e78cb7701f5cb0bec5a3f8108e8dfc),
not an uncommitted working tree, a moving branch tip, or the historical
`347a1a1` implementation. All dispatches selected
`feature/compact-by-key-150`; the branch's exact SHA was checked before each
dispatch, and each run's actual `head_sha` must match the checked revision.
The source archive, raw Actions artifacts and archive SHA-256 values are
retained separately from this summary.

The existing phase matrix is exercised on both supported Go families, plus
the complete race-instrumented test suite and coverage-guided decoder fuzzing
inside the disposable guest. This does **not** by itself qualify every new
publication path for a v1 release. Dedicated missing paths are listed below.
Public API and compatibility-policy decisions are outside this campaign.

## Execution record

All 14 runs completed successfully on 2026-11-23. Independent reconciliation
of the downloaded artifacts also passed. The resolved toolchains were
Go 1.27.4 (`stable`) and Go 1.26.11 (`1.26.x`).

| Group | Cases per family | Go 1.27.4: pass | Go 1.26.11: pass |
| --- | ---: | --- | --- |
| Initialization | 25 | [45109494865](https://github.com/anaregdesign/tarp/actions/runs/45109494865) | [45109535761](https://github.com/anaregdesign/tarp/actions/runs/45109535761) |
| Mutations and rotation | 57 | [45109501156](https://github.com/anaregdesign/tarp/actions/runs/45109501156) | [45109541546](https://github.com/anaregdesign/tarp/actions/runs/45109541546) |
| Source-order DATA checkpoint | 26 | [45109507048](https://github.com/anaregdesign/tarp/actions/runs/45109507048) | [45109546866](https://github.com/anaregdesign/tarp/actions/runs/45109546866) |
| Backup and restore | 34 | [45109513350](https://github.com/anaregdesign/tarp/actions/runs/45109513350) | [45109552440](https://github.com/anaregdesign/tarp/actions/runs/45109552440) |
| Tail recovery and reader/writer/backup overlap | 14 | [45109519059](https://github.com/anaregdesign/tarp/actions/runs/45109519059) | [45109557770](https://github.com/anaregdesign/tarp/actions/runs/45109557770) |
| Actual DATA/checkpoint ENOSPC and post-retry cut | 1 | [45109524448](https://github.com/anaregdesign/tarp/actions/runs/45109524448) | [45109563324](https://github.com/anaregdesign/tarp/actions/runs/45109563324) |
| Full guest race suite and guided decoder fuzz | 15 packages + 15s fuzz | [45109529841](https://github.com/anaregdesign/tarp/actions/runs/45109529841) | [45109568975](https://github.com/anaregdesign/tarp/actions/runs/45109568975) |

This is **157 engine cuts and separate cold reboots per family, 314 total**,
including the two post-exhaustion cuts. The current list was enumerated and
checked against each executed case; the historical 330-cut count is not
reused. Fourteen independent device controls add 14 raw-device cuts, not
14 additional engine cases. Both 15-package full race suites passed.
Guided fuzz completed baseline gathering and 3,365 executions on Go 1.27.4,
and 4,963 executions on Go 1.26.11. Vet and the single-module audit passed
in both runtime jobs.

The common library-source manifest fingerprint is
`99912968b43b0dae2c0b4260a59201ff799715f58ebfaef82547c2772544f301`.
The retained GitHub source tarball has SHA-256
`ee6b8e5b9f749b26d092d4c2e662eeac9ee8d77e2d6fa6ad55c3dd22ae8e16dc`.
Every archive's library-source manifest was reconciled against that source.

Each job reruns the independent raw-device control: synced A must survive,
unsynced B must be discarded after a whole-QEMU SIGKILL and NBD termination.
An engine case is accepted only when its exact phase is witnessed externally,
the guest is killed, the backend is quiescent, the volatile overlay is lost,
and a separate cold boot validates file-only recovery.

The selected stack was Ubuntu 24.04.5 amd64, guest kernel
6.8.0-147-generic, QEMU 8.2.2, TCG with two guest CPUs and 1 GiB RAM,
and a guarded 256 MiB ext4 `data=ordered` device with normal barriers,
no DAX and no quotas. The Ubuntu image/kernel hashes and full superblock
features/mounts are retained in the raw records. Hosted Azure/AWS kernels
are recorded separately and are **not** the guest kernel.

Eighteen engine cuts discarded nonzero volatile overlay: 196,608
overlay-page bytes in total (allocated 4 KiB-page accounting, not a count
of distinct user-value bytes). Other engine cuts still lost the full
guest RAM/page cache. Device controls discarded another 57,344 overlay-page
bytes in total. Observed guest FUA traffic was zero.
For engine cuts the NBD server terminated with EOF in 298 cases and the
expected Unix-socket connection reset after SIGKILL in 16 cases.
The controller joins the server before discarding overlay state.
Neither connection reset nor a zero-dirty-overlay cut is silently treated
as data loss or used as proof that every device acknowledged only stable writes.

The witness checks acknowledged values, acknowledged Deletes, the permitted
whole old/new outcome of one pending mutation, its actual selected sequence,
the complete live-key cardinality and a SHA-256 state digest. Required
backup/restore outputs must validate at the captured sequence. Pre-commit
initialization can report an explicit uninitialized database; that outcome
is forbidden after the initial frontier's completed directory sync.

The exhaustion group fills only a regular file on a guarded 256 MiB guest
device. It checks pre-publication checkpoint failure with 128-256 KiB
headroom, source health, retry after freeing space, backup pins and reclamation,
admitted-write ENOSPC, uniform fail-stop behavior, repeated Close errors,
reopen, and an externally witnessed final cut/reboot.
Both runs observed zero available bytes and real write-time errno 28.
Exactly 131,072 available bytes produced a healthy `NotApplied` checkpoint
failure. Retry after releasing 2 MiB succeeded; the slow backup retained
1,048,384 retired DATA bytes and releasing its pins reclaimed exactly that
logical length. The subsequent admitted Put returned `NotApplied` with
ENOSPC and fail-stopped hot/miss requests. Reopen and the final cold boot
verified the acknowledged state and captured output.

## Evidence retention and reconciliation

Raw artifacts retain source and boot fingerprints, exact resolved Go patches,
host/QEMU details, guest image/kernel hashes, mounts/features, external
oracle journals, cut/recovery console logs, NBD counters and final verdicts.
The runtime jobs also retain every test binary's package/hash/build manifest
and actual guided-fuzz output.

The downloaded archives are checked against GitHub's artifact digests.
An independent standard-library Python verifier replays every oracle,
recomputes the state digest and cardinality at the reported recovered sequence,
checks whole-guest cut/reboot outcomes, and reconciles the source manifest
with the retained source archive. Its source and focused tests are retained
with the evidence. An Actions green check alone is not the evidence record.

| Raw archive run | SHA-256 |
| --- | --- |
| 45109494865 | `685a5468d6e86a6ebb8ec68bd63db4cd09d6bf2b78444f5f1a28f63e0d0ae8cb` |
| 45109501156 | `3e69ffe27ce2753cbe0015143b208787a518129bdec8dacdc1d24e816e17d563` |
| 45109507048 | `137d13e5bbe5eb862fb7806b2460ee57dd68d68e1ff4c4beaf492cffaa42f658` |
| 45109513350 | `93fe3de7f54269b24fa2aa3622b8224dcc987f4f55c0ee5a174e50d9bde00295` |
| 45109519059 | `08a31ef525063f4a64f8a933ed69cf3deef0bf0b4ea6be8bd0eb72905a622175` |
| 45109524448 | `3c0fb65c94019b0f79b99e33184098ad3af1b63c08c27882cd967a7960bc0f6f` |
| 45109529841 | `71d69900ff460a0102b30641b1578c7600c0566bdf52b1e344c70bcca05038fc` |
| 45109535761 | `48235c6d9e2188ab475b7dece97d4e46f79af7643913da8ccbe168da8f146cfa` |
| 45109541546 | `1958b68e4a6d2392025aedf23f8ad6d7c0f14b1418882861066dfbe11b7d873b` |
| 45109546866 | `d93b5421f1d353e843469632024e0ba438d902fd3a1944e4f7c3b08605e93a90` |
| 45109552440 | `c2d0de930bc411fae28ab9b3f70e126a8e37da0047e4fdeb4d78e2406a0fd6dd` |
| 45109557770 | `1bf084097ff5931205cadb92d9199224acc9f439958294206fabaef54e830baf` |
| 45109563324 | `0d8045d909a6fb8a8bec0f080ed8cbebe63e5656a2c6912c3449d1f25325d550` |
| 45109568975 | `fd61cde62e7d4565db2d4da99de896c4df486bde90d6b763d4e41ec027ed4e3a` |

Actions retention is 30 days. The downloaded raw archives and source archive
must be retained beyond that window for release provenance. The final bundle
contains a run manifest, per-file hashes and independent-verification results.

## Coverage limits and remaining v1 gate

The existing full guest suite includes deterministic fault injection and
process-crash coverage of current engine behavior. These are valuable but
are not equivalent to dedicated whole-VM power cuts.

| Path | Existing evidence exercised by this campaign | Dedicated whole-VM work still required |
| --- | --- | --- |
| BatchPut and BatchDelete | Atomic outcome, rotation/accounting, overlapping scan/checkpoint/backup and injected publication failures | An external batch-aware witness must reject every intermediate sequence and mixed old/new key set; cut append, rotation and CURRENT publication, including duplicates and delete/reinsert |
| Automatic incremental compaction | Cleaner publication process-crash matrix, fault injection, concurrent-tail rebasing and grouped/repacked recovery | Cut single-segment, grouped-log and small-segment-repacking output/PARITY, catalog/CURRENT publication, remapping and retirement; include a pinned reader/backup and concurrent tail |
| Key-ordered checkpoint | Initial/delta sorting, unchanged-range reuse, concurrent batches, corruption and failed-publication/retry tests | Cut initial and delta builds, reused-range publication, remapping and cleanup; independently check exact cutoff state and reused-file ownership |
| INDEX checkpoint and Indexed reopen | Packing/reopen, concurrent rotations, corruption/repair and injected publication failures | Cut INDEX output and selected descriptor publication, explicit CheckpointIndex and Close, then verify both Full and Indexed reopen |
| Online DATA repair and PARITY | Verified repair, uncertain replacement/retry, corruption and source/output ownership tests | Cut protected replacement/PARITY publication and directory sync with one damaged block; verify the acknowledged logical state after cold boot |
| B+tree private-pager exhaustion | Earlier focused current-format campaign is linked in the main qualification record | Repeat private-pager ENOSPC on the selected release candidate, including NotApplied batch retry and cold boot |

The matrix's ordinary cases use the default B+tree with a 16 MiB budget.
The full guest suites exercise both index modes. Those full-suite results
must not be described as a complete whole-VM phase matrix for both modes.
The exhaustion fixture parks automatic maintenance to attribute staging
failure, retry and reclamation to its manual checkpoints; it does not prove
automatic-maintenance ENOSPC behavior by itself.

Extending the remote matrix requires a reviewed, published harness on a fixed
revision. This campaign leaves those dedicated paths explicitly unqualified.
This is an evidence gap, not an observed corruption verdict. The tracking
issue remains open; the passing existing matrix is not a completed v1 gate.

Only the precisely recorded disposable Ubuntu amd64, ext4 `data=ordered`,
honest-flush volatile NBD stack is in scope. No physical SSD/controller
certification, other filesystem/kernel guarantee, quota-enabled filesystem,
lying-flush tolerance or generic macOS power-loss guarantee is implied.
See the [durability contract](durability.md) and
[qualification procedure](qualification.md).
