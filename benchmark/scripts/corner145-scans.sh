#!/usr/bin/env bash
set -euo pipefail
output=${1:?supply an empty output directory}
mkdir -p "$output"
out=$(cd "$output" && pwd -P)
repo=$(git rev-parse --show-toplevel)
version=${CORNER145_GO_VERSION:?resolve stable Go release for the campaign}
image=${CORNER145_GO_IMAGE:?supply official pinned image}
[[ $image =~ ^docker.io/library/golang@sha256:[a-f0-9]{64}$ ]]
rounds=${TARP_SCAN_RELEASE_ROUNDS:-6}
pairs=${TARP_SCAN_RELEASE_PAIRS:-40}
steps=${TARP_SCAN_RELEASE_STEPS:-16}
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
test ! -e "$out/raw.jsonl"
docker run --rm --pull=never --platform "$platform" --cpus=4 --network=none \
  --mount type=volume,dst=/data --mount "type=bind,src=$repo,dst=/src,readonly" \
  --mount "type=bind,src=$out,dst=/out" -w /src \
  -e "TARP_SCAN_RELEASE_STEPS=$steps" -e "TARP_SCAN_RELEASE_ROUNDS=$rounds" -e "TARP_SCAN_RELEASE_PAIRS=$pairs" \
  -e TARP_SCAN_RELEASE_OUT=/out/raw.jsonl -e GOTOOLCHAIN=local -e GOWORK=off \
  -e GOPROXY=off -e GOMAXPROCS=4 -e TMPDIR=/data/tmp \
  -e "VERSION=$version" -e "IMAGE=$image" -e "REVISION=$(git rev-parse HEAD)" \
  "$image" bash -euc '
    set -o pipefail
    mkdir -p "$TMPDIR"
    test "$(go env GOVERSION)" = "$VERSION"
    {
      echo "revision=$REVISION image=$IMAGE rounds=$TARP_SCAN_RELEASE_ROUNDS pairs=$TARP_SCAN_RELEASE_PAIRS steps=$TARP_SCAN_RELEASE_STEPS"
      go version
      uname -smr
      findmnt -T /data
      cat /sys/fs/cgroup/cpu.max
    } | tee /out/environment.txt
    go test -buildvcs=false -tags corner145 -count=1 -timeout=60m -v -run "^TestCorner145ScanRelease$" ./benchmark | tee /out/results.txt
    grep -q "^--- PASS: TestCorner145ScanRelease" /out/results.txt
  '
