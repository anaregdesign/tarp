#!/usr/bin/env bash
# Reproduce issue #150 using committed sources and an isolated Docker volume.
set -euo pipefail
if [[ $# -lt 1 || $# -gt 2 || -z $1 ]]; then
    echo "usage: $0 <empty-or-new-results-directory> [experiment-commit]" >&2
    exit 2
fi
repo=$(git rev-parse --show-toplevel)
tip=$(git -C "$repo" rev-parse --verify --end-of-options "${2:-HEAD}^{commit}")
reader=678dd2c9096e6cac3a12e6cc737a91dde8cf0e41
git -C "$repo" cat-file -e "$reader^{commit}"
image=docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea
cpus=${TARP_LOCALITY_CPUSET:-2,3}
mkdir -p -- "$1"
out=$(cd -- "$1" && pwd -P)
[[ -z $(ls -A "$out") ]] || { echo "results directory must be empty: $out" >&2; exit 2; }
mkdir "$out/.running"
tmp=$(mktemp -d)
cleanup() {
    if [[ -s $tmp/cid ]]; then
        docker rm -fv "$(cat "$tmp/cid")" >/dev/null 2>&1 || true
    fi
    rm -rf "$tmp"
    rmdir "$out/.running"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$tmp/experiment" "$tmp/reader"
git -C "$repo" archive "$tip" | tar -x -C "$tmp/experiment"
git -C "$repo" archive "$reader" | tar -x -C "$tmp/reader"
for f in benchmark/{locality_policy_test.go,locality_delta_policy_test.go,locality_foreground_test.go,locality_native_linux_test.go,testdata/locality/sorted_checkpoint.go}; do
    test -f "$tmp/experiment/$f" || { echo "missing from $tip: $f" >&2; exit 1; }
done
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
[[ $platform == linux/* ]] || { echo "expected a Linux Docker daemon, got $platform" >&2; exit 1; }
docker pull --platform "$platform" "$image" > "$out/image-pull.log"
{
    echo "experiment=$tip"
    echo "reader=$reader"
    echo "container=$image"
    echo "platform=$platform cpuset=$cpus GOMAXPROCS=2"
    docker context show
    docker image inspect "$image" --format 'image_id={{.Id}} {{json .RepoDigests}}'
    docker info --format 'daemon={{.ServerVersion}} os={{.OperatingSystem}} kernel={{.KernelVersion}} cpus={{.NCPU}} memory_bytes={{.MemTotal}}'
} > "$out/provenance.txt"

echo "Key locality $tip; results $out"
if ! docker run -i --rm --cidfile "$tmp/cid" --pull=never --platform "$platform" --network none --cpuset-cpus "$cpus" \
    -v "$tmp/experiment:/experiment:ro" -v "$tmp/reader:/reader:ro" -v "$out:/out" --mount type=volume,dst=/data \
    -e GOMAXPROCS=2 -e GOTOOLCHAIN=local -e GOPROXY=off -e TMPDIR=/data/tmp -e GOCACHE=/data/gocache \
    "$image" bash -seu > "$out/campaign.log" 2>&1 <<'GUEST'
set -o pipefail
set -x
trap 'if [[ -d /out/foreground ]]; then chown -R --reference=/out /out/foreground; fi' EXIT
mkdir -p "$TMPDIR" "$GOCACHE"
{
    date -u
    go version
    uname -srmo
    grep -E '^(Cpus_allowed_list|Mems_allowed_list):' /proc/self/status
    findmnt --noheadings --target /data --output SOURCE,FSTYPE,OPTIONS
    df -B1 /data
} > /out/environment.txt
test "$(go env GOVERSION)" = go1.27.4
test "$(findmnt -n -T /data -o FSTYPE)" = ext4

for arm in physical staged sorted; do
    cp -a /reader "/data/$arm"
    cp /experiment/benchmark/locality_native_linux_test.go "/data/$arm/benchmark/"
done
for arm in staged sorted; do
    cd "/data/$arm/internal/storage"
    cp /experiment/benchmark/testdata/locality/sorted_checkpoint.go locality_sorted.go
    sorted=false
    if [[ $arm == sorted ]]; then sorted=true; fi
    SORT_RECORDS="$sorted" perl -0777 -pe '
        $old = q{return c.capture.replayLive(ctx, c.store.IO, history, live, emit)};
        $new = "return c.localityReplayStaged(ctx, history, live, emit, $ENV{SORT_RECORDS})";
        $n = s/\Q$old\E/$new/g;
        die "expected exactly one checkpoint hook, got $n\n" unless $n == 1;
    ' checkpoint.go > checkpoint.go.new
    mv checkpoint.go.new checkpoint.go
    diff -u /reader/internal/storage/checkpoint.go checkpoint.go > "/out/$arm-checkpoint.patch" || test "$?" -eq 1
done
{
    sha256sum /experiment/benchmark/locality_*_test.go /experiment/benchmark/testdata/locality/sorted_checkpoint.go
    for arm in physical staged sorted; do
        sha256sum "/data/$arm/benchmark/locality_native_linux_test.go" "/data/$arm/internal/storage/checkpoint.go"
    done
    sha256sum /data/{staged,sorted}/internal/storage/locality_sorted.go
} > /out/sources.sha256

for arm in experiment physical staged sorted; do
    case "$arm" in experiment) cd /experiment ;; *) cd "/data/$arm" ;; esac
    go test -buildvcs=false -trimpath -c -o "/data/$arm.test" ./benchmark
done
sha256sum /data/{experiment,physical,staged,sorted}.test > /out/binaries.sha256

run_test() {
    local log=$1
    shift
    "$@" > "$log" 2>&1 || { tail -60 "$log"; return 1; }
}
run_test /out/policy.log env TARP_LOCALITY_POLICY=1 /data/experiment.test -test.run '^TestLocalityPolicy$' -test.v -test.timeout 45m
sed -n 's/^.*LOCALITY_POLICY //p' /out/policy.log > /out/policy.jsonl
test "$(wc -l < /out/policy.jsonl)" -eq 560
run_test /out/delta.log env TARP_LOCALITY_DELTA=1 /data/experiment.test -test.run '^TestLocalityDeltaPolicy$' -test.v -test.timeout 45m
sed -n 's/^.*LOCALITY_DELTA //p' /out/delta.log > /out/delta.jsonl
test "$(wc -l < /out/delta.jsonl)" -eq 80
run_test /out/foreground.log env TARP_LOCALITY_FOREGROUND=1 TARP_LOCALITY_FOREGROUND_ROUNDS=8 TARP_LOCALITY_FOREGROUND_OUTPUT=/out/foreground \
    /data/experiment.test -test.run '^TestLocalityForeground$' -test.v -test.timeout 60m

for trial in 1 2 3; do
    case "$trial" in
        1) arms=(physical staged sorted) ;;
        2) arms=(staged sorted physical) ;;
        3) arms=(sorted physical staged) ;;
    esac
    for arm in "${arms[@]}"; do
        log="/out/native-$arm-$trial.log"
        run_test "$log" env TARP_LOCALITY_NATIVE="$arm" "/data/$arm.test" -test.run "^TestLocalityNative$/^${trial}$" -test.v -test.timeout 45m
        sed -n 's/^.*LOCALITY_NATIVE //p' "$log" >> "/out/native-$arm.jsonl"
    done
done
GUEST
then
    tail -65 "$out/campaign.log" >&2
    exit 1
fi

python3 - "$out" <<'VALIDATE'
import collections
import itertools
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
def load_lines(name):
    return [json.loads(line) for line in (out / name).read_text().splitlines()]

def coverage(label, actual, expected):
    if collections.Counter(actual) != collections.Counter(expected):
        raise SystemExit(f"incomplete or duplicate {label} results in {out}")

policy = load_lines("policy.jsonl")
coverage("policy", ((r["trace"], r["policy"], r["epoch"]) for r in policy),
         itertools.product(("first", "last", "hot", "uniform", "churn"),
                           ("append", "full", "bounded12p5", "bounded25", "bounded50", "heat25", "heat25budget"), range(1, 17)))
delta = load_lines("delta.jsonl")
coverage("delta", ((r["trace"], r["policy"], r["epoch"]) for r in delta),
         itertools.product(("first", "last", "hot", "uniform", "churn"), ("delta50budget",), range(1, 17)))
for arm in ("physical", "staged", "sorted"):
    rows = load_lines(f"native-{arm}.jsonl")
    for trial in (1, 2, 3):
        group = [r for r in rows if r["arm"] == arm and r["trial"] == trial]
        coverage(f"{arm} trial {trial}", ((r["kind"], r.get("trace")) for r in group),
                 [("checkpoint", None)] + [("read", t) for t in ("unchanged", "first", "last", "spread") for _ in range(3)])
        if len({(r.get("trace"), r.get("tail")) for r in group}) != 13:
            raise SystemExit(f"duplicate native read in {arm}, trial {trial}")
    if len(rows) != 39:
        raise SystemExit(f"expected 39 native rows for {arm}")
fg = [json.loads(path.read_text()) for path in sorted((out / "foreground").glob("*.json"))]
coverage("foreground", ((r["trial"], r["trace"], r["arm"]) for r in fg),
         itertools.product(range(8), ("hot-first", "spread"), ("default", "side-near", "side-stress", "compact")))
with (out / "foreground.jsonl").open("x") as stream:
    for row in fg:
        print(json.dumps(row, separators=(",", ":")), file=stream)
print("Verified 560 policy, 80 delta, 64 foreground, and 39 observations for each of physical, staged, and sorted")
VALIDATE
