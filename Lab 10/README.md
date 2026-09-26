# Lab 10: Lakehouse Federation & Change Data Capture

## Overview

This lab consists of two notebooks:

1. **`01_lakehouse_federation`** — Demonstrates Databricks Lakehouse Federation: querying a remote PostgreSQL (Neon) database through a foreign catalog, enriching with local Delta tables, materializing data locally, and comparing federated vs. local query plans.

2. **`02_change_data_capture`** — Demonstrates Delta Lake Change Data Feed (CDF): enabling CDF on a source table, simulating business changes (INSERT/UPDATE/DELETE), auditing the change log with `table_changes()`, and incrementally syncing a target table via `MERGE INTO` (SCD Type 1).

---

## Theoretical Questions

### 1. Federation vs Ingestion

**Federation** (Lakehouse Federation) allows querying external data sources directly from Databricks without copying the data. A foreign catalog is registered via a connection, and remote tables appear as Unity Catalog objects. The Spark query engine pushes filters and projections to the source where possible (as demonstrated by the `PushedFilters: [rating IS NOT NULL, rating > 8.0]` in Cell 13's EXPLAIN plan).

**Ingestion** (ETL/ELT) physically copies data from the source into Databricks storage (Delta Lake). The data is then queried locally with full access to Photon optimization, caching, and statistics.

| Aspect | Federation | Ingestion |
| --- | --- | --- |
| Data location | Stays in source system | Copied into Databricks storage |
| Latency | Real-time (query-time) | Batch or micro-batch |
| Storage cost | No duplicate storage | Requires storage for copies |
| Query performance | Depends on source + network | Optimized (Parquet/Delta, caching, statistics) |
| Source load | Queries hit the source directly | Source only touched during ingestion |
| Best for | Ad-hoc analysis, exploration, small datasets | Production workloads, large datasets, repeated queries |

**When to use Federation:**

* Exploratory analysis against external databases
* Avoiding data duplication and storage costs
* When real-time freshness is required
* Small to medium datasets where source performance is adequate

**When to use Ingestion:**

* Large-scale analytics requiring performance
* Workloads with complex joins and aggregations
* When the source system cannot handle analytical query load
* When you need full control over data quality, indexing, and caching

**In this lab:** The federated query (`neon_pg_catalog.public.movie_box_office`) reads directly from PostgreSQL, while the materialized `local_movie_box_office` table is an ingestion — copied into Delta Lake for faster local access.

---

### Security Implications: Federation vs Ingestion

Data security and access governance differ significantly between direct federation and local ingestion:

| Security Dimension | Lakehouse Federation | Ingestion (Delta Lake) |
| --- | --- | --- |
| **Credential Management** | Connection objects use shared service credentials (`neondb_owner`). Credential leaks risk direct production DB access. | Storage access governed centrally; credentials isolated to automated ingestion pipelines. |
| **Access Control (RBAC)** | Governed via Unity Catalog grants on the foreign catalog, but queries run under the connection's database role. | Granular Unity Catalog controls: row filters, column masks, and attribute-based access control (ABAC). |
| **Network Attack Surface** | Requires network connectivity (open outbound ports, VPN/DirectConnect, or SSL whitelisting) from Databricks to operational DB. | Network boundaries strictly contained within the cloud storage perimeter (S3 VPC Endpoints). |
| **Operational Blast Radius** | Unoptimized analytical queries (large full table scans) can exhaust transactional DB connection pools and CPU, impacting users. | Zero impact on production OLTP operations; compute workloads isolated to Databricks warehouses. |
| **Audit & Lineage** | Tracks access to the foreign catalog, but granular table manipulation within the external DB relies on the source system's logs. | Full Unity Catalog data lineage tracking, system tables auditing, and Delta transaction log history. |

---

### 2. Latency

Latency differs significantly across data access patterns:

| Pattern | Latency | Staleness | Notes |
| --- | --- | --- | --- |
| Federation (JDBC) | Real-time (seconds) | Zero | Every query hits the source; no staleness |
| Materialized table | Batch (minutes to hours) | Equals time since last refresh | Data is a snapshot from the last copy |
| CDC (Change Data Feed) | Near real-time (seconds to minutes) | Equals time since last MERGE | Changes captured immediately; applied on next sync |
| Batch ETL | Hours (scheduled) | Hours | Full reprocessing on a schedule |

**In this lab:**

* The federated query in `01_lakehouse_federation` has **zero staleness** — it reads directly from PostgreSQL at query time.
* The materialized `local_movie_box_office` table has staleness equal to the time since Cell 10 was last executed.
* The CDC pipeline in `02_change_data_capture` has **near-real-time latency** — changes are captured in the CDF log immediately at commit time, and the MERGE in Step 5 applies them incrementally on the next run.

**Key trade-off:** Federation gives you the freshest data but at the cost of query performance and source system load. Materialization gives you fast queries but stale data. CDC bridges the gap — it captures changes in real-time but applies them in near-real-time batches, balancing freshness with performance.

---

### 3. Batch vs CDC

**Batch processing** copies entire datasets (or partitions) on a schedule. It is simple but inherently inefficient — it reprocesses unchanged data every run.

**CDC (Change Data Capture)** captures only row-level changes (inserts, updates, deletes) as they occur, enabling incremental processing. Delta Lake's Change Data Feed (CDF) records each change with metadata (`_change_type`, `_commit_version`, `_commit_timestamp`).

| Aspect | Batch | CDC |
| --- | --- | --- |
| What's processed | Entire dataset or partition | Only changed rows |
| Latency | Hours (scheduled) | Seconds to minutes |
| Efficiency | Low (reprocesses unchanged data) | High (processes only deltas) |
| Complexity | Simple | Requires CDF setup + MERGE logic |
| Source impact | High (full table scans) | Low (reads change log) |
| Late data handling | Re-run the full batch | Process since last checkpoint |
| Auditability | Limited (last-write-wins) | Full change history (`update_preimage`, `update_postimage`, `delete`) |

**In this lab:**

* The batch approach (Cell 10 in `01_lakehouse_federation`) copies the entire `movie_box_office` table with `CREATE OR REPLACE TABLE AS SELECT *` — every row is reprocessed regardless of whether it changed.
* The CDC approach (`02_change_data_capture`) uses `table_changes('cdc_source_movies', 1)` to read only the 4 changed rows (1 insert, 1 update_preimage, 1 update_postimage, 1 delete) and applies them via `MERGE INTO` — the 3 unchanged rows are never touched.

**CDC change types observed in the lab:**

| `_change_type` | Description | MERGE action |
| --- | --- | --- |
| `insert` | New row added | `WHEN NOT MATCHED → INSERT` |
| `update_preimage` | Row values before update | Skipped (not needed for SCD Type 1) |
| `update_postimage` | Row values after update | `WHEN MATCHED → UPDATE` |
| `delete` | Row removed | `WHEN MATCHED → DELETE` |

---

### 4. Handling Late-Arriving Data

Late-arriving data — records that arrive after their logical event time or after a processing window has closed — is a common challenge in data pipelines.

**Strategies:**

#### 4.1 CDC with Version Checkpointing

Using `table_changes('table', since_version)`, each MERGE run processes all changes since the last checkpoint. Late changes appear in the CDF log at their **commit time** (not event time), so they are always picked up on the next run. The `since_version` parameter acts as a watermark — incrementing it after each successful MERGE ensures no changes are missed.

*In this lab:* Step 5 uses `table_changes('cdc_source_movies', 1)` to process changes since version 1. In a production pipeline, the last processed version would be stored in a metadata table and incremented after each successful MERGE.

#### 4.2 Reprocessing Windows (REPLACE WHERE)

Materialized views or scheduled `INSERT OVERWRITE` with `REPLACE WHERE` can recompute a bounded time window (e.g., last 7 days) to catch late arrivals. Only the affected partition is overwritten — the rest of the table remains untouched.

*Example:* Overwriting the last 7 days of data each night ensures late-arriving records are captured without reprocessing the entire table.

#### 4.3 SCD Type 2 for Full History

Instead of SCD Type 1 (overwrite with latest values), SCD Type 2 retains full history with `effective_from` and `effective_to` timestamps. Late corrections are applied as new versions — the original record is closed (not deleted), preserving a complete audit trail.

*In this lab:* The MERGE implements SCD Type 1 — the latest `update_postimage` wins, overwriting the previous value. For late-arriving corrections, this means the most recent change always takes precedence, which is correct for current-state tracking.

#### 4.4 Watermarking in Streaming

Structured Streaming uses watermarks to define how long to wait for late data before finalizing a window aggregation. `WITH WATERMARK` lets you balance latency vs. completeness — a longer watermark waits for more late data but increases latency.

**Summary table:**

| Strategy | Handles late data? | Complexity | Best for |
| --- | --- | --- | --- |
| CDC version checkpointing | Yes (naturally) | Medium | Incremental sync pipelines |
| REPLACE WHERE window | Yes (bounded reprocess) | Low | Scheduled reprocessing of recent partitions |
| SCD Type 2 | Yes (preserves history) | High | Audit and compliance requirements |
| Streaming watermark | Yes (within threshold) | High | Real-time streaming aggregations |

---

## Lab Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Lab 10: Two Notebooks                        │
│                                                                 │
│  ┌─────────────────────────────┐  ┌──────────────────────────┐ │
│  │  01_lakehouse_federation     │  │  02_change_data_capture   │ │
│  │                             │  │                          │ │
│  │  Neon PostgreSQL (remote)   │  │  Source (CDF enabled)    │ │
│  │       ↓ foreign catalog     │  │       ↓ table_changes()  │ │
│  │  Federation query (JDBC)     │  │  Change log audit        │ │
│  │       ↓                     │  │       ↓                  │ │
│  │  Cross-source JOIN          │  │  MERGE INTO (SCD Type 1) │ │
│  │       ↓                     │  │       ↓                  │ │
│  │  Materialize locally (Delta) │  │  Target table (synced)   │ │
│  │       ↓                     │  │                          │ │
│  │  EXPLAIN: federated vs local │  │                          │ │
│  └─────────────────────────────┘  └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Security Note

The Neon PostgreSQL connection password in `01_lakehouse_federation` has been replaced with a placeholder (`npg_placeholder`) to prevent exposure via GitHub Secret Scanner. In production, credentials should be stored in Databricks secrets and referenced via `dbutils.secrets.get()`.