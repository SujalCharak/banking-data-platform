# Benchmark results

Generated 2026-09-28 22:04 UTC on 111,980,440 benchmark rows. Result cache off; the warehouse is suspended before each variant, so the first run is cold. Warm is the median of the remaining 2 runs.

Clustering depth on txn_date: unclustered 48.0, clustered 2.0 (lower is better; 1.0 is perfect).

## clustering

How much does clustering on txn_date reduce the data scanned by a one month report?

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| unclustered | XSMALL | 0.62 | 0.29 | 48 / 48 | 0.00008 |
| clustered by txn_date | XSMALL | 0.55 | 0.21 | 1 / 74 | 0.00006 |

## sargable_predicate

Does wrapping the filter column in a function defeat pruning on a clustered table?

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| to_char(txn_date, 'YYYY-MM') = '2024-03' | XSMALL | 3.56 | 3.32 | 72 / 74 | 0.00092 |
| txn_date between ... | XSMALL | 0.44 | 0.22 | 1 / 74 | 0.00006 |

## point_lookup

How fast is a single transaction lookup by id with and without search optimization?

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| no search optimization | XSMALL | 1.11 | 0.57 | 47 / 48 | 0.00016 |
| search optimization | XSMALL | 0.81 | 0.39 | n/a | 0.00011 |

## running_balance_rewrite

Running balance per account over one year: triangular self join versus a window function.

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| self join | XSMALL | 81.86 | 83.26 | 16 / 148 | 0.02313 |
| window function | XSMALL | 3.12 | 2.46 | 8 / 74 | 0.00068 |

## approximate_distinct

Monthly active accounts: exact COUNT(DISTINCT) versus HyperLogLog.

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| count(distinct) | XSMALL | 2.49 | 2.16 | 74 / 74 | 0.00060 |
| approx_count_distinct | XSMALL | 1.91 | 1.45 | 74 / 74 | 0.00040 |

## warehouse_sizing

Full history scan on each warehouse size: does doubling the size halve the time at the same cost?

| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |
|---|---|---:|---:|---:|---:|
| XSMALL | XSMALL | 1.95 | 1.51 | 48 / 48 | 0.00042 |
| SMALL | SMALL | 1.52 | 1.10 | 48 / 48 | 0.00061 |
| MEDIUM | MEDIUM | 1.29 | 0.95 | 48 / 48 | 0.00106 |

Estimated credits = warm seconds / 3600 x credits per hour for the size, ignoring the 60 second minimum per resume.
