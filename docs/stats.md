# Database statistics

Call `db.Stats()` once per scrape. Both `DB` and `TypedDB` return a
`tarp.Stats` value for the underlying open DB. All fields are `uint64`.
The snapshot is coherent across the fields, including concurrent batch and
catalog publications. Taking it uses a short state lock and constant work,
performs no file I/O, and preserves LRU membership, recency and counters.

## Gauges

| Field | Suggested Prometheus metric | Measures |
| --- | --- | --- |
| `LiveKeys` | `tarp_live_keys` | Keys in the live index |
| `LiveRecordBytes` | `tarp_live_record_bytes` | Encoded length of the current Put for every live key, including its record header |
| `SelectedSegments` | `tarp_selected_segments` | Physical DATA files in the selected catalog and its active log |
| `SelectedDataBytes` | `tarp_selected_data_bytes` | Sum of selected DATA lengths, using the committed end of the active log |
| `ObsoletePutBytes` | `tarp_obsolete_put_bytes` | Superseded or deleted Put records still present in selected DATA |
| `TombstoneBytes` | `tarp_tombstone_bytes` | Encoded Delete records retained in selected DATA |
| `Cache.UsedBytes` | `tarp_cache_used_bytes` | Logical charges of cached keys, values and entry metadata |
| `Cache.LimitBytes` | `tarp_cache_limit_bytes` | Effective logical cache budget after option normalization |

Each selected segment contributes a 64-byte header. The remaining selected
bytes equal `LiveRecordBytes + ObsoletePutBytes + TombstoneBytes`. A newly
created DB has one empty segment and 64 selected bytes. Physical disk use also
includes filesystem allocation, metadata, temporary files and retired files.
Obsolete records can reside in the active log or in segments below the
automatic cleaner's threshold. For its most recent observation of retired
bytes held by pins, use `MaintenanceStatus().PinnedBytes`.

## Counters

| Field | Suggested Prometheus metric | Counts since Open |
| --- | --- | --- |
| `Cache.Hits` | `tarp_cache_hits_total` | Requested key positions found in the LRU |
| `Cache.Misses` | `tarp_cache_misses_total` | Requested key positions found only in the live index |
| `MissingKeys` | `tarp_missing_keys_total` | Requested key positions absent from the live index |
| `Cache.Evictions` | `tarp_cache_evictions_total` | LRU entries evicted to meet the byte limit |
| `WriteCommits` | `tarp_write_commits_total` | Nonempty foreground write calls published to the in-memory index |
| `WrittenRecords` | `tarp_written_records_total` | Put and Delete records published by those calls |

Get counts its first lookup once, including when a later record read fails or
a concurrent update requires a retry. BatchGet counts every input position at
snapshot capture. A cold `[A, A, missing]` with a live A counts two misses and
one missing key and reads A's record once. An invalid batch, Scan, Backup and
Compact contribute zero read lookups. Typed codec failures before a lookup
have no effect. Decode failures after a lookup retain its counts.

BatchPut counts one commit and every input record, including repeated keys.
BatchDelete counts one commit and one record per distinct deleted key.
Empty and rejected writes contribute zero. After a publication error the
snapshot reflects the last state made visible in memory. Recovery establishes
the selected disk state on the next Open. Each Open reconstructs storage
gauges, starts with a cold cache and resets all counters. Close retains the
final numeric snapshot for callers holding that DB handle.

## Collecting from a Go server

This collector exposes two of the fields above. Place it in the server module
with imports for `github.com/anaregdesign/tarp` and
`github.com/prometheus/client_golang/prometheus`.

```go
var (
	liveKeys = prometheus.NewDesc("tarp_live_keys", "Keys in the live index.", nil, nil)
	commits  = prometheus.NewDesc("tarp_write_commits_total", "Foreground writes published since Open.", nil, nil)
)

type dbCollector struct{ db *tarp.DB }

func (c dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- liveKeys
	ch <- commits
}

func (c dbCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(liveKeys, prometheus.GaugeValue, float64(s.LiveKeys))
	ch <- prometheus.MustNewConstMetric(commits, prometheus.CounterValue, float64(s.WriteCommits))
}
```

Register `dbCollector{db}` with the registry served at `/metrics`.
Emit each cumulative counter directly on every scrape. If a server hosts
multiple databases, identify them with a bounded set of configured names.
Record request latency with a server-side histogram around the operation.

Use `rate(tarp_written_records_total[5m]) / rate(tarp_write_commits_total[5m])`
for records per commit over a five-minute window with writes. For cache hit
ratio, divide the hit rate by the sum of hit and miss rates over a window
with lookups of live keys. Missing keys have their own counter. A growing
obsolete-byte gauge alongside stable live bytes indicates retained history
from overwrites and deletes.

See the [runnable Stats example](../stats_test.go) and
[Prometheus custom collector documentation](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Collector).
