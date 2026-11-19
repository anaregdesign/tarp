#!/bin/sh
# Run from the repository root. Each leaf uses its own temporary database.
set -eu
export GOMAXPROCS=4
binary=$(mktemp "${TMPDIR:-/tmp}/tarp-api-bench.XXXXXX")
trap 'rm -f "$binary"' EXIT
go test -c -o "$binary" ./benchmark

run() {
    "$binary" -test.run '^$' -test.bench "$1" -test.benchtime "$2" -test.benchmem
}

for round in 1 2 3 4 5; do
    echo "Round $round"
    case $round in
        1|3|5) reads='BatchGet Get-loop'; writes='batch serial'; scrapes='baseline 1Hz continuous' ;;
        2|4)   reads='Get-loop BatchGet'; writes='serial batch'; scrapes='continuous 1Hz baseline' ;;
    esac
    for arm in $reads; do
        run "^BenchmarkBatchGet$/././^${arm}$" 200ms
    done
    for arm in $writes; do
        run "^BenchmarkBatchWrite$/./././^${arm}$" 3x
    done
    run '^BenchmarkStatsSnapshot$' 300ms
    for arm in $scrapes; do
        run "^BenchmarkStatsDuringGet$/^${arm}$" 2100ms
    done
done

for round in 1 2 3 4 5; do
    echo "Boundary round $round"
    case $round in
        1|3|5) modes='empty rotate' ;;
        2|4) modes='rotate empty' ;;
    esac
    for mode in $modes; do
        run "^BenchmarkBatchPutSegmentBoundary$/^${mode}$" 3x
    done
done
