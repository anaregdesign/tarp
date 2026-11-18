#!/usr/bin/env bash
set -euo pipefail
if [[ "${TARP_QUAL_GUEST_CONFIRM:-}" != disposable-vm-only ||
      "$(cat /sys/block/vdb/size)" != 524288 ||
      "$(findmnt -rn -o FSTYPE /mnt/tarp-input)" != 9p ]]; then
  echo "Disposable 256 MiB guest device and read-only input are required." >&2
  exit 1
fi
source /etc/os-release
test "$ID:$VERSION_ID" = ubuntu:24.04
[[ "$(uname -r)" == 6.8.* ]]
uname -a
cat /sys/block/vdb/queue/write_cache
export TARP_VM_BLOCK_CONTROL=disposable-vm-only
exec /mnt/tarp-input/tarp.test -test.run '^TestVMBlockControls$' -test.v -test.timeout=4m
