#!/bin/sh
set -eu
test "${TARP_QUAL_GUEST_CONFIRM:-}" = disposable-vm-only
export TARP_VM_RUNTIME_CONFIRM=disposable-vm-only
export GOMAXPROCS=2
/mnt/tarp-input/tarp.test -test.run='^TestVMRuntimeGuest$' -test.v -test.timeout=14m
