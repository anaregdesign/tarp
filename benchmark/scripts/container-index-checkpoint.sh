#!/usr/bin/env bash
# One immutable official Go image runs every arm on the same private volume.
set -euo pipefail
out=${1:?usage: container-index-checkpoint.sh RESULTS_DIRECTORY}
mkdir -p "$out"
out=$(cd "$out" && pwd -P)
repo=$(git rev-parse --show-toplevel)
revision=$(git rev-parse HEAD)
curl -fsSL --retry 2 'https://go.dev/dl/?mode=json' -o "$out/go-releases.json"
version=$(python3 -c 'import json,sys
print(max((v["version"] for v in json.load(open(sys.argv[1])) if v["stable"]), key=lambda v: tuple(map(int,v[2:].split(".")))))' "$out/go-releases.json")
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
docker pull --platform "$platform" "golang:${version#go}-bookworm" > "$out/image-pull.log"
digest=$(awk '/^Digest: sha256:/ {print $2}' "$out/image-pull.log")
[[ $digest =~ ^sha256:[a-f0-9]{64}$ ]]
image=docker.io/library/golang@$digest
docker run --rm --pull=never --platform "$platform" --cpus 2 --network none \
    --volume "$repo:/src:ro" --volume "$out:/out" --mount type=volume,dst=/data \
    --workdir /src -e GOTOOLCHAIN=local -e GOWORK=off -e GOPROXY=off -e GOMAXPROCS=2 \
    -e TMPDIR=/data/tmp -e TARP_INDEX_CHECKPOINT_MEASURE=1 \
    -e "VERSION=$version" -e "IMAGE=$image" -e "REVISION=$revision" "$image" bash -euc '
    set -o pipefail
    mkdir -p "$TMPDIR"
    test "$(go env GOVERSION)" = "$VERSION"
    {
        echo "revision=$REVISION image=$IMAGE"
        go version
        uname -smr
        findmnt -T /data
    } | tee /out/environment.txt
    go test -buildvcs=false -count=1 -timeout=8m -v -run "^TestIndexCheckpointMeasurement$" ./benchmark | tee /out/results.txt
    grep -q "^--- PASS: TestIndexCheckpointMeasurement" /out/results.txt
'
