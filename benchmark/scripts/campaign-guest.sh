#!/usr/bin/env bash
set -euo pipefail
test "${TARP_QUAL_GUEST_CONFIRM:-}" = disposable-vm-only
export LC_ALL=C TARP_VM_CAMPAIGN_CONFIRM=disposable-vm-only
exec /mnt/tarp-input/tarp.test -test.run '^TestVMCampaignGuest$' -test.v -test.timeout=4m
