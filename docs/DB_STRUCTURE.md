# SQLite structure (MOEX API)

## Core tables
- `bonds_cache`: daily CSV snapshot cache.
- `details_cache`: JSON cache for details endpoints by `(endpoint, secid)`.
- `details_rows`: flattened rows per endpoint block (JSON row-level backup).
- `endpoint_health_history`: request health log (status, latency, source, errors).
- `endpoint_circuit_breaker`: per-endpoint breaker state (`closed/half_open/open`).
- `bonds_enriched`: materialized final table (base CSV + details enrichment columns).

## Materialized analytics
- `endpoint_health_mv`: pre-aggregated endpoint stability metrics for windows `1d` and `7d`:
  - total requests
  - error requests + error rate
  - avg latency
  - p95 latency

## Export files
- `Moex_Bonds.xlsx`: base export from rates CSV.
- `Moex_Bonds_Details.xlsx`: endpoint/block raw sheets + `details_transposed` (human-readable wide view).
- `Moex_Bonds_Finish.xlsx`: final merged dataset (CSV + details columns).
