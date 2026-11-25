#!/usr/bin/env bash
# Compare committed sources in one private Linux volume, with identical drivers.
set -euo pipefail
if [[ $# -lt 1 || $# -gt 4 || -z $1 ]]; then
    echo "usage: $0 <empty-or-new-result-directory> [baseline-ref [candidate-ref [rounds]]]" >&2
    exit 2
fi
rounds=${4:-3}
cpu=${TARP_PREFIX_CPU:-2}
[[ $rounds =~ ^[1-9][0-9]*$ && $cpu =~ ^[0-9]+$ ]] || { echo 'expected positive rounds and a single CPU number' >&2; exit 2; }
repo=$(git rev-parse --show-toplevel)
tip=$(git -C "$repo" rev-parse --verify --end-of-options "${3:-HEAD}^{commit}")
base=$(git -C "$repo" rev-parse --verify --end-of-options "${2:-$tip}^{commit}")
mkdir -p "$1"
out=$(cd "$1" && pwd -P)
[[ -z $(ls -A "$out") ]] || { echo "results directory must be empty: $out" >&2; exit 2; }
mkdir "$out/.running"
tmp=$(mktemp -d)
cleanup() {
    if [[ -s $tmp/cid ]]; then docker rm -fv "$(cat "$tmp/cid")" >/dev/null 2>&1 || true; fi
    rm -rf "$tmp"
    rmdir "$out/.running"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$tmp/filter" "$tmp/prefix"
git -C "$repo" archive "$base" | tar -x -C "$tmp/filter"
git -C "$repo" archive "$tip" | tar -x -C "$tmp/prefix"
drivers=(edge_prefix_benchmark_test.go typed_edge_prefix_benchmark_test.go edge_prefix_io_linux_test.go
        edge_prefix_working_set_test.go batch_get_container_io_linux_test.go
        batch_get_container_fadvise{,_other}_linux_test.go)
for arm in filter prefix; do
    mkdir "$tmp/$arm/benchmark/edgecomparison"
    for file in "${drivers[@]}"; do
        cp "$tmp/prefix/benchmark/$file" "$tmp/$arm/benchmark/edgecomparison/$file"
    done
done
{
    echo "filter=$base"
    echo "prefix=$tip"
    for file in "${drivers[@]}"; do
        echo "driver $(git -C "$repo" rev-parse "$tip:benchmark/$file") benchmark/$file"
    done
} > "$out/sources.txt"
curl -fsSL --retry 2 'https://go.dev/dl/?mode=json' -o "$out/go-releases.json"
version=$(python3 -c 'import json,sys
print(max((v["version"] for v in json.load(open(sys.argv[1])) if v["stable"]), key=lambda v: tuple(map(int, v[2:].split(".")))))' "$out/go-releases.json")
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
docker pull --platform "$platform" "golang:${version#go}-bookworm" > "$out/image-pull.log"
digest=$(awk '/^Digest: sha256:/ {print $2}' "$out/image-pull.log")
[[ $digest =~ ^sha256:[a-f0-9]{64}$ ]]
image=docker.io/library/golang@$digest
{
    echo "$image"
    docker image inspect "$image" --format '{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}'
    docker context show
    docker info --format 'daemon={{.ServerVersion}} kernel={{.KernelVersion}} cpus={{.NCPU}} memory_bytes={{.MemTotal}}'
} > "$out/image.txt"

echo "Measuring $base vs $tip on CPU $cpu, $rounds rounds. Results: $out"
if ! docker run -i --rm --cidfile "$tmp/cid" --pull=never --platform "$platform" --network none --cpuset-cpus "$cpu" \
    -v "$tmp/filter:/filter:ro" -v "$tmp/prefix:/prefix:ro" -v "$out:/out" --mount type=volume,dst=/bench \
    -e GOMAXPROCS=1 -e GOTOOLCHAIN=local -e GOPROXY=off -e TMPDIR=/bench/tmp -e GOCACHE=/bench/gocache \
    -e TARP_EDGE_PREFIX_FIXTURES=/bench/seeds -e "ROUNDS=$rounds" -e "EXPECTED_GO_VERSION=$version" "$image" bash -seu > "$out/campaign.log" 2>&1 <<'GUEST'
set -o pipefail
trap 'chown --reference=/out /out/*' EXIT
mkdir -p "$TMPDIR"
test "$(go env GOVERSION)" = "$EXPECTED_GO_VERSION"
{
    date -u
    go version
    uname -srmo
    grep -E '^(Cpus_allowed_list|Mems_allowed_list):' /proc/self/status
    echo "GOMAXPROCS=$GOMAXPROCS rounds=$ROUNDS"
    findmnt --noheadings --target /bench --output SOURCE,FSTYPE,OPTIONS
} > /out/environment.txt
for arm in filter prefix; do
    (cd "/$arm" && go test -buildvcs=false -c -o "/bench/$arm" ./benchmark/edgecomparison)
done
bench() {
    local family=$1 pattern=$2 time=$3 leaves=$4 log="/out/r$round-$arm-$1.txt"
    echo "round $round: $arm $family"
    "/bench/$arm" -test.run '^$' -test.bench "$pattern/^$arm$" -test.benchtime "$time" -test.benchmem -test.cpu 1 -test.timeout 30m > "$log" 2>&1 || { cat "$log"; return 1; }
    [[ $(grep -c '^Benchmark' "$log") -eq $leaves ]] || { echo "expected $leaves results in $log"; return 1; }
}
for ((round=1; round<=ROUNDS; round++)); do
    arms=(filter prefix)
    if ((round % 2 == 0)); then arms=(prefix filter); fi
    for arm in "${arms[@]}"; do
        bench raw '^BenchmarkEdgePrefix$/./././.' 100ms 48
        bench typed '^BenchmarkTypedEdgePrefix$/./.' 100ms 4
        bench working-set '^BenchmarkEdgePrefixWorkingSet$/./.' 256x 4
    done
done
cold() {
    local arm=$1 binary=$2 log="/out/io-$1.txt"
    TARP_EDGE_PREFIX_IO="$arm" "/bench/$binary" -test.run '^TestEdgePrefixDataIO$' -test.v -test.timeout 10m > "$log" 2>&1 || { cat "$log"; return 1; }
}
cold seed filter
for arm in filter prefix; do
    cold "$arm" "$arm"
    sed -n 's/^.*EDGE_PREFIX_IO //p' "/out/io-$arm.txt" > "/out/io-$arm.jsonl"
    [[ $(wc -l < "/out/io-$arm.jsonl") -eq 24 ]] || { echo "expected 24 I/O results for $arm"; exit 1; }
done
GUEST
then
    tail -50 "$out/campaign.log" >&2
    exit 1
fi
