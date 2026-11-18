#!/bin/sh
set -eu
test "${TARP_QUAL_GUEST_CONFIRM:-}" = disposable-vm-only
export TARP_VM_MEASURE_CONFIRM=disposable-vm-only
export GOMAXPROCS=2
binary=/mnt/tarp-input/tarp.test
if test -f /mnt/tarp-input/use-profile; then
  binary=/mnt/tarp-input/tarp.profile.test
fi
"$binary" -test.run='^TestVMMeasurementsGuest$' -test.v -test.timeout=29m
