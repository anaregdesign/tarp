#!/usr/bin/env bash
set -euo pipefail
if [[ "${TARP_QUAL_GUEST_CONFIRM:-}" != disposable-vm-only ]]; then
  echo "This script requires explicit disposable-guest confirmation." >&2
  exit 1
fi

source /etc/os-release
test "$ID" = ubuntu
test "$VERSION_ID" = 24.04
test "$(uname -m)" = x86_64
[[ "$(uname -r)" == 6.8.* ]]
test "$(cat /sys/block/vdb/size)" = 524288
test -b /dev/vdb
test "$(findmnt -rn -o FSTYPE /mnt/tarp-input)" = 9p
trap 'status=$?; echo "TARP_PROBE_EXIT $status"; sync; poweroff -f' EXIT

cat /etc/os-release
uname -a
cat /proc/cpuinfo
cat /proc/meminfo
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS
cat /sys/block/vdb/queue/write_cache
mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 /dev/vdb
mkdir /mnt/tarp-data
mount -t ext4 -o data=ordered /dev/vdb /mnt/tarp-data
options="$(findmnt -rn -o OPTIONS /mnt/tarp-data)"
[[ ",$options," == *,data=ordered,* ]]
[[ ",$options," != *,nobarrier,* && ",$options," != *,dax,* ]]
findmnt /mnt/tarp-data
tune2fs -l /dev/vdb
df -B1 /mnt/tarp-data
sync
echo TARP_ENVIRONMENT_PASS
