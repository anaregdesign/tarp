#!/usr/bin/env bash
# Run five opt-in scenarios against the exact committed source in a private volume.
set -euo pipefail

if [[ $# != 1 || -z $1 ]]; then
    echo "usage: $0 <empty-or-new-results-directory>" >&2
    exit 2
fi
rounds=${TARP_STORAGE_SCALE_ROUNDS:-3}
[[ $rounds =~ ^[1-9][0-9]*$ ]] || { echo 'expected a positive round count' >&2; exit 2; }
repo=$(git rev-parse --show-toplevel)
revision=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$1"
out=$(cd "$1" && pwd -P)
[[ -z $(ls -A "$out") ]] || { echo "results directory must be empty: $out" >&2; exit 2; }
mkdir "$out/.running"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"; rmdir "$out/.running"' EXIT
mkdir "$tmp/src" "$tmp/probe"
git -C "$repo" archive "$revision" | tar -x -C "$tmp/src"
python3 "$tmp/src/benchmark/scripts/storage-scale-overlay.py" "$tmp/src" "$tmp/probe"

curl -fsSL --retry 2 'https://go.dev/dl/?mode=json' -o "$out/go-releases.json"
version=$(python3 -c 'import json,sys
print(max((v["version"] for v in json.load(open(sys.argv[1])) if v["stable"]), key=lambda v: tuple(map(int, v[2:].split(".")))))' "$out/go-releases.json")
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
docker pull --platform "$platform" "golang:${version#go}-bookworm" > "$out/image-pull.log"
digest=$(awk '/^Digest: sha256:/ {print $2}' "$out/image-pull.log")
[[ $digest =~ ^sha256:[a-f0-9]{64}$ ]]
image=docker.io/library/golang@$digest
docker context show > "$out/docker-context.txt"
docker info --format '{{.OperatingSystem}} {{.KernelVersion}} {{.ServerVersion}} CPUs={{.NCPU}} memory={{.MemTotal}}' >> "$out/docker-context.txt"
echo "Revision $revision; $version ($platform); $rounds rounds; results $out"
if ! docker run --rm --pull=never --platform "$platform" --cpus 4 --network none \
    --volume "$tmp/src:/src:ro" --volume "$tmp/probe:/probe:ro" --volume "$out:/out" \
    --mount type=volume,dst=/data --workdir /src -e GOMAXPROCS=4 -e GOTOOLCHAIN=local -e GOPROXY=off \
    -e TMPDIR=/data/tmp -e "REVISION=$revision" -e "IMAGE=$image" -e "VERSION=$version" -e "ROUNDS=$rounds" "$image" bash -ec '
        set -o pipefail
        mkdir -p "$TMPDIR"
        test "$(go env GOVERSION)" = "$VERSION"
        {
            date -u
            go version
            uname -srmo
            echo "revision=$REVISION image=$IMAGE GOMAXPROCS=$GOMAXPROCS rounds=$ROUNDS"
            findmnt --noheadings --target /data --output SOURCE,FSTYPE,OPTIONS
            df -B1 /data
        } > /out/environment.txt
        sha256sum /probe/* > /out/probe.sha256
        go test -buildvcs=false -overlay /probe/overlay.json -c -o /data/suite ./benchmark
        scenarios=(BTree Verify Open Overlap Cleaner)
        for ((r=1; r<=ROUNDS; r++)); do
            export TARP_STORAGE_SCALE_ROUND=$r
            for ((i=0; i<5; i++)); do
                name=${scenarios[$(((i+r-1)%5))]}
                echo "Starting round $r: $name"
                /data/suite -test.run "^TestStorageScale${name}$" -test.timeout 45m -test.v
            done
        done
    ' > "$out/test.log" 2>&1; then
    grep -v '^TARP_SCALE_RESULT ' "$out/test.log" | tail -50 >&2
    exit 1
fi
python3 "$tmp/src/benchmark/scripts/storage-scale-results.py" "$out" "$rounds"
