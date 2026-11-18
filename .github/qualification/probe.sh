#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" != true || "${RUNNER_OS:-}" != Linux ||
      "${RUNNER_ARCH:-}" != X64 || "${RUNNER_ENVIRONMENT:-}" != github-hosted ||
      "${TARP_QUAL_CONFIRM:-}" != disposable-vm-only ]]; then
  echo "This probe requires explicit opt-in on a disposable GitHub-hosted Linux amd64 runner." >&2
  exit 1
fi
mode="${TARP_QUAL_MODE:-probe}"
[[ "$mode" == probe || "$mode" == controls || "$mode" == campaign ]]
[[ "$RUNNER_TEMP" == /* && "$GITHUB_RUN_ID" =~ ^[0-9]+$ &&
   "$GITHUB_RUN_ATTEMPT" =~ ^[0-9]+$ ]]
test "$(df -Pk "$RUNNER_TEMP" | awk 'NR == 2 { print $4 }')" -ge 8388608
work="$RUNNER_TEMP/tarp-qualification-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
mkdir "$work"
mkdir "$work/evidence" "$work/input"

if ! command -v qemu-system-x86_64 >/dev/null || ! command -v qemu-img >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends qemu-system-x86 qemu-utils
fi
{
  uname -smr
  qemu-system-x86_64 --version
  go version
  git rev-parse HEAD
  dpkg-query -W qemu-system-x86 qemu-utils
  df -Pk "$RUNNER_TEMP"
} >"$work/evidence/host.txt"
git ls-files -- '*.go' go.mod | while IFS= read -r source; do
  case "$source" in
    *_test.go|cmd/*|internal/qualification/*) continue ;;
  esac
  sha256sum "$source"
done >"$work/evidence/library-sources.sha256"
sha256sum "$work/evidence/library-sources.sha256" >"$work/evidence/library-tree.sha256"

image=https://cloud-images.ubuntu.com/releases/noble/release-20261017/ubuntu-24.04-server-cloudimg-amd64.img
curl --fail --location --retry 2 --max-time 600 "$image" -o "$work/base.qcow2"
printf '%s  %s\n' 04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94 \
  "$work/base.qcow2" | sha256sum --check
sha256sum "$work/base.qcow2" >"$work/evidence/base-image.sha256"
printf '%s\n' "$image" >"$work/evidence/base-image.url"
qemu-img info --output=json "$work/base.qcow2" >"$work/evidence/base-image.json"
python3 - "$work/evidence/base-image.json" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    image = json.load(source)
assert image["format"] == "qcow2"
assert image["virtual-size"] <= 4 * 1024**3
PY

unpacked=https://cloud-images.ubuntu.com/releases/noble/release-20261017/unpacked
curl --fail --location --retry 2 --max-time 120 \
  "$unpacked/ubuntu-24.04-server-cloudimg-amd64-vmlinuz-generic" -o "$work/vmlinuz"
curl --fail --location --retry 2 --max-time 120 \
  "$unpacked/ubuntu-24.04-server-cloudimg-amd64-initrd-generic" -o "$work/ubuntu-initrd"
printf '%s  %s\n' 41ba36d860aa0b0cd995f4ca8856d80249b244577e704929b55be13fbd1ebd2f \
  "$work/vmlinuz" bf276ffc12872256080890698b42db0dc9c5a469c896d8b59af6bc4e723cd186 \
  "$work/ubuntu-initrd" | sha256sum --check
export GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly GOPROXY=off
go build -o "$work/tarpqual" ./cmd/tarpqual
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$work/tarp-init" ./cmd/tarpguestinit
"$work/tarpqual" -append-initrd "$work/ubuntu-initrd" -init "$work/tarp-init" -output "$work/initrd"
sha256sum "$work/vmlinuz" "$work/ubuntu-initrd" "$work/initrd" "$work/tarp-init" \
  >"$work/evidence/boot.sha256"
# Bound generated files in both probe and controller modes.
ulimit -f 4194304
if [[ "$mode" == controls || "$mode" == campaign ]]; then
  cp .github/qualification/controls-guest.sh "$work/input/guest.sh"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o "$work/input/tarp.test" .
  sha256sum "$work/input/tarp.test" >"$work/evidence/guest-binary.sha256"
  "$work/tarpqual" -block-controls "$work" -confirm disposable-vm-only \
    >"$work/evidence/control-verdict.json"
  cat "$work/evidence/control-verdict.json"
  if [[ "$mode" == campaign ]]; then
    cp .github/qualification/campaign-guest.sh "$work/input/guest.sh"
    "$work/tarpqual" -campaign "$work" -confirm disposable-vm-only \
      -group "${TARP_QUAL_GROUP:-smoke}" -case "${TARP_QUAL_CASE:-}" \
      >"$work/evidence/campaign-verdict.json"
    cat "$work/evidence/campaign-verdict.json"
  fi
  exit 0
fi
cp .github/qualification/probe-guest.sh "$work/input/guest.sh"
qemu-img create -f qcow2 -F qcow2 -b "$work/base.qcow2" "$work/root.qcow2"
truncate -s 256M "$work/data.raw"

# The test filesystem exists only inside the VM.
timeout --signal=TERM --kill-after=10s 240s qemu-system-x86_64 \
  -machine q35,accel=tcg -cpu max -smp 2 -m 1024 \
  -display none -serial stdio -monitor none -nic none -no-reboot \
  -kernel "$work/vmlinuz" -initrd "$work/initrd" \
  -append 'console=ttyS0 rdinit=/tarp-init -- disposable-vm-only' \
  -drive "file=$work/root.qcow2,format=qcow2,if=virtio,cache=writeback" \
  -drive "file=$work/data.raw,format=raw,if=virtio,cache=writeback" \
  -virtfs "local,path=$work/input,mount_tag=tarpinput,security_model=none,readonly=on" \
  </dev/null >"$work/evidence/serial.log" 2>&1
"$work/tarpqual" -verify-probe "$work/evidence/serial.log" >"$work/evidence/probe-verdict.json"
cat "$work/evidence/probe-verdict.json"
