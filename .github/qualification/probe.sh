#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" != true || "${RUNNER_OS:-}" != Linux ||
      "${RUNNER_ARCH:-}" != X64 || "${RUNNER_ENVIRONMENT:-}" != github-hosted ||
      "${TARP_QUAL_CONFIRM:-}" != disposable-vm-only ]]; then
  echo "This probe requires explicit opt-in on a disposable GitHub-hosted Linux amd64 runner." >&2
  exit 1
fi
[[ "$RUNNER_TEMP" == /* && "$GITHUB_RUN_ID" =~ ^[0-9]+$ &&
   "$GITHUB_RUN_ATTEMPT" =~ ^[0-9]+$ ]]
test "$(df -Pk "$RUNNER_TEMP" | awk 'NR == 2 { print $4 }')" -ge 8388608
work="$RUNNER_TEMP/tarp-qualification-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
mkdir "$work"
mkdir "$work/evidence" "$work/input"

if ! command -v qemu-system-x86_64 >/dev/null || ! command -v cloud-localds >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends qemu-system-x86 qemu-utils cloud-image-utils
fi
{
  uname -smr
  qemu-system-x86_64 --version
  dpkg-query -W qemu-system-x86 qemu-utils cloud-image-utils
  df -Pk "$RUNNER_TEMP"
} >"$work/evidence/host.txt"

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

cp .github/qualification/probe-guest.sh "$work/input/guest.sh"
printf 'instance-id: tarp-probe-%s-%s\nlocal-hostname: tarp-probe\n' \
  "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT" >"$work/meta-data"
cloud-localds "$work/seed.img" .github/qualification/cloud-init.yml "$work/meta-data"
qemu-img create -f qcow2 -F qcow2 -b "$work/base.qcow2" "$work/root.qcow2"
truncate -s 256M "$work/data.raw"

# Limit any single generated file; the test filesystem is only inside the VM.
ulimit -f 4194304
timeout --signal=TERM --kill-after=10s 240s qemu-system-x86_64 \
  -machine q35,accel=tcg -cpu max -smp 2 -m 1024 \
  -display none -serial stdio -monitor none -nic none -no-reboot \
  -drive "file=$work/root.qcow2,format=qcow2,if=virtio,cache=writeback" \
  -drive "file=$work/data.raw,format=raw,if=virtio,cache=writeback" \
  -drive "file=$work/seed.img,format=raw,media=cdrom,readonly=on" \
  -virtfs "local,path=$work/input,mount_tag=tarpinput,security_model=none,readonly=on" \
  </dev/null >"$work/evidence/serial.log" 2>&1
python3 - "$work/evidence/serial.log" "$work/evidence/probe-verdict.json" <<'PY'
import json, pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines()
if "TARP_ENVIRONMENT_PASS" not in [line.strip() for line in lines]:
    raise SystemExit("Guest environment probe did not pass; inspect serial.log")
pathlib.Path(sys.argv[2]).write_text(json.dumps({
    "environment_probe": "pass",
    "power_loss_qualification": "not_run",
    "volatile_cache_controls": "not_run",
    "device_exhaustion": "not_run",
}) + "\n")
PY
cat "$work/evidence/probe-verdict.json"
