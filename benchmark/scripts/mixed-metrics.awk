# Convert raw Go benchmark logs to one CSV row per completed mixed workload sample.
BEGIN {
    print "source,workload,iterations,ns_per_workload,calls_per_second,get_p50_ns,get_p99_ns,put_p50_ns,put_p99_ns"
    count = split("ns/op calls/s ns/get-p50 ns/get-p99 ns/put-p50 ns/put-p99", units, " ")
}

/^BenchmarkMixedWriters\// && $2 ~ /^[0-9]+$/ {
    for (key in metric) delete metric[key]
    for (i = 3; i < NF; i += 2) metric[$(i+1)] = $i
    for (i = 1; i <= count; i++) {
        if (!(units[i] in metric)) {
            print FILENAME ":" FNR ": missing " units[i] > "/dev/stderr"
            failed = 1
            next
        }
    }
    source = FILENAME
    gsub(/"/, "\"\"", source)
    printf "\"%s\",%s,%s", source, $1, $2
    for (i = 1; i <= count; i++) printf ",%s", metric[units[i]]
    print ""
    rows++
}

END {
    if (!rows || failed) exit 1
}
