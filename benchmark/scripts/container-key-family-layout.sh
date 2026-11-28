#!/usr/bin/env bash
# Run the bounded key-family comparison on private Linux storage.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo 'usage: container-key-family-layout.sh <new-or-empty-results-directory>' >&2
    exit 2
fi
repo=$(git rev-parse --show-toplevel)
revision=$(git -C "$repo" rev-parse HEAD)
mkdir -p "$1"
out=$(cd "$1" && pwd -P)
[[ -z $(ls -A "$out") ]] || { echo "results directory must be empty: $out" >&2; exit 2; }
mkdir "$out/.running"
trap 'rmdir "$out/.running"' EXIT
mkdir "$out/src"
git -C "$repo" archive "$revision" | tar -x -C "$out/src"
python3 "$out/src/benchmark/scripts/key-family-layout-overlay.py" "$out/src" "$out/probe"

curl -fsSL --retry 2 'https://go.dev/dl/?mode=json' -o "$out/go-releases.json"
version=$(python3 -c 'import json,sys
print(max((v["version"] for v in json.load(open(sys.argv[1])) if v["stable"]), key=lambda v: tuple(map(int, v[2:].split(".")))))' "$out/go-releases.json")
platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
docker pull --platform "$platform" "golang:${version#go}-bookworm" > "$out/image-pull.log"
digest=$(awk '/^Digest: sha256:/ {print $2}' "$out/image-pull.log")
[[ $digest =~ ^sha256:[a-f0-9]{64}$ ]]
image=docker.io/library/golang@$digest
docker context show > "$out/docker-context.txt"
docker info --format '{{.OperatingSystem}} {{.KernelVersion}} {{.ServerVersion}} CPUs={{.NCPU}}' >> "$out/docker-context.txt"
docker image inspect --format 'docker_config={{.Id}} platform={{.Os}}/{{.Architecture}}' "$image" >> "$out/docker-context.txt"
tar --no-xattrs -C "$out" -cf - src probe | docker run --rm -i --pull=never --platform "$platform" --cpus 2 --network none \
    --cap-drop ALL --security-opt no-new-privileges \
    --mount type=volume,dst=/data \
    -e CGO_ENABLED=0 -e GOTOOLCHAIN=local -e GOWORK=off -e GOPROXY=off -e GOMAXPROCS=2 \
    -e TMPDIR=/data/tmp -e TARP_KEY_FAMILY_LAYOUT=1 \
    -e "REVISION=$revision" -e "IMAGE=$image" -e "VERSION=$version" "$image" bash -euc '
        tar --no-same-owner -xf - -C /
        mkdir -p "$TMPDIR"
        cd /src
        test "$(go env GOVERSION)" = "$VERSION"
        {
            date -u
            go version
            uname -smr
            echo "revision=$REVISION image=$IMAGE GOMAXPROCS=$GOMAXPROCS"
            findmnt --noheadings --target "$TMPDIR" --output SOURCE,FSTYPE,OPTIONS
            df -B1 "$TMPDIR"
            sha256sum /probe/* ./benchmark/key_family_layout_linux_test.go
        } | sed "s/^/KEY_FAMILY_ENV /"
        go test -buildvcs=false -overlay /probe/overlay.json -count=1 -timeout=10m -v \
            -run "^TestKeyFamily(Layout|CommitControl)$" ./benchmark
    ' 2>&1 | tee "$out/test.log"
grep -q '^--- PASS: TestKeyFamilyLayout ' "$out/test.log"
grep -q '^--- PASS: TestKeyFamilyCommitControl ' "$out/test.log"
sed -n 's/^KEY_FAMILY_ENV //p' "$out/test.log" > "$out/environment.txt"
sed -n 's/^.*KEY_FAMILY_LAYOUT //p' "$out/test.log" > "$out/results.jsonl"
test "$(wc -l < "$out/results.jsonl")" -eq 75
