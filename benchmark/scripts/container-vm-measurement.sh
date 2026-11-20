#!/usr/bin/env bash
# Run the existing disposable guest measurement under the campaign's Go image.
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true && ${RUNNER_ENVIRONMENT:-} == github-hosted &&
   ${RUNNER_OS:-} == Linux && ${RUNNER_ARCH:-} == X64 &&
   ( ${TARP_QUAL_MODE:-} == measure || ${TARP_QUAL_MODE:-} == compact-compare ) &&
   ${TARP_QUAL_CONFIRM:-} == disposable-vm-only ]]
version=$(go env GOVERSION)
if [[ -n ${TARP_MEASUREMENT_DIGEST:-} ]]; then
    [[ $TARP_MEASUREMENT_DIGEST =~ ^sha256:[0-9a-f]{64}$ ]]
    image="docker.io/library/golang@$TARP_MEASUREMENT_DIGEST"
    # An explicitly pinned campaign can span a new upstream patch release.
    version=""
    docker pull "$image"
else
    tag="docker.io/library/golang:${version#go}-bookworm"
    docker pull "$tag"
    image=$(docker image inspect --format '{{index .RepoDigests 0}}' "$tag")
fi
run_name="tarp-qualification-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
scratch=$(mktemp -d "$RUNNER_TEMP/tarp-measure.XXXXXX")
# The normal workflow uploads this evidence, including partial logs after failures.
retain_evidence() {
    if [[ -d "$scratch/$run_name/evidence" ]]; then
        mkdir -p "$RUNNER_TEMP/$run_name"
        mv "$scratch/$run_name/evidence" "$RUNNER_TEMP/$run_name/"
        printf '%s\n' "$image" > "$RUNNER_TEMP/$run_name/evidence/container-image.txt"
        printf '%s\n' "$GITHUB_RUN_ID/$GITHUB_RUN_ATTEMPT" > "$RUNNER_TEMP/$run_name/evidence/workflow-run.txt"
    fi
}
trap retain_evidence EXIT
docker run --rm --pull=never \
    --mount "type=bind,src=$PWD,dst=/src,readonly" \
    --mount "type=bind,src=$scratch,dst=/work" --workdir /src \
    -e GITHUB_ACTIONS -e GITHUB_RUN_ID -e GITHUB_RUN_ATTEMPT \
    -e RUNNER_ENVIRONMENT -e RUNNER_OS -e RUNNER_ARCH -e RUNNER_TEMP=/work \
    -e TARP_QUAL_CONFIRM -e TARP_QUAL_MODE -e TARP_QUAL_VALUE_BYTES \
    -e EXPECT_GO="$version" -e RUN_UID="$(id -u)" -e RUN_GID="$(id -g)" \
    "$image" bash -euc '
        if [[ -n $EXPECT_GO ]]; then test "$(go env GOVERSION)" = "$EXPECT_GO"; fi
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq
        apt-get install -y --no-install-recommends qemu-system-x86 qemu-utils python3
        export HOME=/work GOPATH=/work/go GOCACHE=/work/go-cache
        exec setpriv --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups bash benchmark/scripts/probe.sh
    '
