#!/usr/bin/env bash
# Prepare one current-format comparison arm with the selected revision's
# measurement code. Only the library and its internal dependencies vary by arm.
set -euo pipefail
benchmark=$(cd "$(dirname "$0")/.." && pwd)
target=${1:?expected a comparison source tree}/benchmark/compactcomparison
mkdir "$target"
cp "$benchmark"/measurement{,_fixture,_layout,_pins}_test.go \
  "$benchmark"/{compaction_benchmark,repack_benchmark,repack_fixture,repack_trace}_test.go \
  "$benchmark"/testdata/{compactcompare,compact_compare_guest_linux,compact_lock_profile}_test.go "$target/"
