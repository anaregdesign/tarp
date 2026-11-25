#!/bin/sh
set -eu
test "${TARP_QUAL_GUEST_CONFIRM:-}" = disposable-vm-only
export GOMAXPROCS=2 TMPDIR=/mnt/tarp-data

run_case() {
  TARP_COMPARE_ARM="$1" TARP_COMPARE_MODE="$2" TARP_COMPARE_ROUND="$3" \
    "/mnt/tarp-input/$1${4:-}.test" -test.run='^TestCompactCompare$' -test.count=1 -test.v -test.timeout=3m
}

# Seed once on the guarded guest device. Both arms clone the same fixture.
run_case base seed 0
read -r pair </mnt/tarp-input/compact-pair
for round in 1 2 3 4 5 6; do
  if [ "$((round % 2))" -eq 1 ]; then
    first=base second=next
    modes='overlap parked'
  else
    first=next second=base
    modes='parked overlap'
  fi
  if [ "$pair" = candidate-fixed ]; then modes=fixed; fi
  for mode in $modes; do
    run_case "$first" "$mode" "$round"
    run_case "$second" "$mode" "$round"
  done
done

if [ "$pair" = candidate-fixed ]; then
  echo TARP_COMPARE_PASS
  exit 0
fi

run_case base baseline 0
run_case next baseline 0
run_case base pinned 0
run_case next pinned 0

# Diagnostic windows run in separate binaries after the uninstrumented windows.
for round in 1 2 3 4; do
  if [ "$((round % 2))" -eq 1 ]; then
    run_case base lock-profile "$round" .diag
    run_case next lock-profile "$round" .diag
  else
    run_case next lock-profile "$round" .diag
    run_case base lock-profile "$round" .diag
  fi
done
echo TARP_COMPARE_PASS
