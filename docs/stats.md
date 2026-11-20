# Database statistics

Call `db.Stats()` once per scrape. Both `DB` and `TypedDB` return a
`tarp.Stats` value for the underlying open DB. Each metric is a `uint64`.
Cache metrics are grouped under `Stats.Cache` in a `tarp.CacheStats` value.
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
| `Cache.Hits` | `tarp_cache_hits_total` | Distinct keys per read call found in the LRU |
| `Cache.Misses` | `tarp_cache_misses_total` | Distinct keys per read call found only in the live index |
| `MissingKeys` | `tarp_missing_keys_total` | Distinct keys per read call absent from the live index |
| `BatchGetDuplicateInputs` | `tarp_batch_get_duplicate_inputs_total` | Input positions beyond the first for each encoded key in a captured BatchGet |
| `Cache.Evictions` | `tarp_cache_evictions_total` | LRU entries evicted to meet the byte limit |
| `WriteCommits` | `tarp_write_commits_total` | Nonempty foreground write calls published to the selected key index |
| `WrittenRecords` | `tarp_written_records_total` | Put and Delete records published by those calls |

Get counts its first lookup once, including when a concurrent update requires
a retry. At snapshot capture, BatchGet classifies each distinct key once as a
hit, miss or missing key. Every additional occurrence in the same call adds
one to `BatchGetDuplicateInputs`. Keys are compared by their encoded bytes,
including when a TypedDB key codec maps different inputs to the same bytes.

For `BatchGet([A, B, A, C, C])`, suppose A is cached, B is live but cold and
C is absent. The call adds one hit, one miss, one missing key and two duplicate
inputs. It reads B's record once. BatchGet preserves LRU contents and recency,
so a second identical call adds the same counts if the DB is otherwise idle.
For a cold live A, `BatchGet([A, A, A])` adds one miss and two duplicate
inputs. Three separate `Get(A)` calls add one miss and two hits if the first
call caches A and it stays cached.

An empty or invalid batch, a failure before snapshot capture, Scan, Backup
and Compact contribute zero to these four counters. A subsequent DATA read
or typed value decode error retains the captured counts. The duplicate
counter also covers cached and absent keys, both of which require no DATA read.

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
with lookups of live keys. Missing keys and duplicate inputs each have their
own counter and contribute to neither side of this ratio. The mixed batch
above has a hit ratio of 50%. A growing
obsolete-byte gauge alongside stable live bytes indicates retained history
from overwrites and deletes.

### Migrating lookup metrics from v0.2.x

In v0.2.x each input position contributed to hits, misses or missing keys.
Starting in v0.3.0, a batch with duplicates contributes fewer lookups, and
hit ratios can change for the same workload. For a rolling upgrade, add a
constant `lookup_unit="distinct_key"` label to the three lookup metrics in
the upgraded collector. Select that label in dashboards, recording rules and
alerts so each aggregation uses the same counting unit. If retaining the
unlabelled metric names, compare rates only within windows in which every
included instance used the same definition. Adding the duplicate count to
the sum of the three lookup counts recovers the total number of classified
input positions. The individual v0.2.x hit, miss and missing totals cannot
be reconstructed from that sum.

See the [runnable Stats example](../stats_test.go) and
[Prometheus custom collector documentation](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Collector).
