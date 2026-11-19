#!/bin/sh
# Run from the tarp checkout. Keep the measurement volume private to this run.
set -eu

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
    echo "usage: sh benchmark/scripts/container-batch-get.sh <empty-or-new-result-directory>" >&2
    exit 2
fi
repo=$(git rev-parse --show-toplevel)
case $1 in
    /*) out=$1 ;;
    *) out=$PWD/$1 ;;
esac
mkdir -p "$out"
out=$(cd "$out" && pwd -P)
if [ -n "$(ls -A "$out")" ]; then
    echo "result directory must be empty: $out" >&2
    exit 2
fi
# Two campaigns sharing a new directory must not overwrite each other's logs.
mkdir "$out/.running"
trap 'rmdir "$out/.running"' EXIT

curl -fsSL --retry 2 'https://go.dev/dl/?mode=json' -o "$out/go-releases.json"
version=$(python3 -c 'import json, sys
versions = [v["version"] for v in json.load(open(sys.argv[1])) if v["stable"]]
print(max(versions, key=lambda v: tuple(map(int, v.removeprefix("go").split(".")))))' "$out/go-releases.json")
tag="docker.io/library/golang:${version#go}-bookworm"
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
docker pull --platform "$platform" "$tag" > "$out/image-pull.log"
digest=$(awk '/^Digest: sha256:/ {print $2}' "$out/image-pull.log")
if [ "${#digest}" -ne 71 ]; then
    echo "docker pull did not report a unique image digest; see $out/image-pull.log" >&2
    exit 1
fi
image="docker.io/library/golang@$digest"
docker image inspect "$image" --format '{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}' > "$out/image.txt"
context=$(docker context show)
server=$(docker info --format 'daemon={{.ServerVersion}} os={{.OperatingSystem}} arch={{.Architecture}} kernel={{.KernelVersion}} cpus={{.NCPU}} memory_bytes={{.MemTotal}}')
revision=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" status --porcelain --untracked-files=all -- '*.go' go.mod go.sum ':(exclude)benchmark/**' ':(exclude)*_test.go' > "$out/engine-changes.txt"
if [ -s "$out/engine-changes.txt" ]; then
    revision="$revision (modified engine sources)"
fi
echo "Running BatchGet measurements with $version ($platform), $image"
echo "Results: $out"
if ! docker run --rm --pull=never --platform "$platform" --network none \
    --volume "$repo:/src:ro" --volume "$out:/out" --mount type=volume,dst=/data \
    --workdir /src -e GOMAXPROCS=8 -e GOTOOLCHAIN=local -e GOPROXY=off \
    -e TARP_CONTAINER_BATCH_GO="$version" -e TARP_CONTAINER_BATCH_IMAGE="$image" \
    -e TARP_CONTAINER_BATCH_REVISION="$revision" -e TARP_CONTAINER_BATCH_BACKEND="context=$context $server" \
    -e TARP_CONTAINER_BATCH_ROOT=/data -e TARP_CONTAINER_BATCH_OUTPUT=/out/batch-get.jsonl \
    "$image" sh -ec '
        test "$(go env GOVERSION)" = "$TARP_CONTAINER_BATCH_GO"
        mount=$(findmnt --noheadings --target /data --output SOURCE,FSTYPE,OPTIONS)
        export TARP_CONTAINER_BATCH_BACKEND="$TARP_CONTAINER_BATCH_BACKEND; volume=$mount"
        # Output files on a rootful Linux daemon should belong to the results directory owner.
        fix_owner() {
            if [ -f "$TARP_CONTAINER_BATCH_OUTPUT" ]; then
                chown --reference=/out "$TARP_CONTAINER_BATCH_OUTPUT"
            fi
        }
        trap fix_owner EXIT
        {
            date -u
            go version
            uname -srmo
            echo "$TARP_CONTAINER_BATCH_IMAGE"
            echo "$TARP_CONTAINER_BATCH_REVISION"
            echo "$TARP_CONTAINER_BATCH_BACKEND"
            df -B1 /data
        } > /out/environment.txt
        { find . -maxdepth 1 -name "*.go" -print0; find internal benchmark -type f -name "*.go" -print0; } |
            sort -z | xargs -0 sha256sum > /out/source.sha256
        sha256sum go.mod benchmark/scripts/container-batch-get.sh >> /out/source.sha256
        go test -buildvcs=false -count=1 -timeout=45m ./benchmark -run "^TestContainerBatchGet$" -v
        test -s "$TARP_CONTAINER_BATCH_OUTPUT"
    ' > "$out/test.log" 2>&1; then
    cat "$out/test.log" >&2
    exit 1
fi
