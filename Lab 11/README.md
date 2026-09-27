# Lab 11: Zero-Bus Streaming with Zerobus

> **Production-grade event-driven ingestion pipeline without a centralized message broker** —
> implementing Databricks Zerobus (push-based single-sink model) with idempotent Delta Lake ingestion.

## 1. Lab Overview

### Objective

This lab implements an **event-driven ingestion pipeline** that streams user movie consumption
events directly into a Delta Lake table — **without a centralized message broker** (e.g., Kafka,
Kinesis, Pulsar). The approach leverages **Databricks Zerobus**, a push-based single-sink model
where the producer writes events directly to a Unity Catalog Delta table via a serverless
managed endpoint.

### Implementation Notebook

| Artifact | Path |
| --- | --- |
| `01_zerobus_producer_and_idempotency.ipynb` | `/Users/kostia6@gmail.com/databricks-assignment/Lab 11/` |

### Business Scenario

A streaming movie platform emits user consumption events of four types:

| Event Type | Description |
| --- | --- |
| `movie_started` | User begins playing a movie |
| `watch_progress` | Periodic progress checkpoint (watch-time in seconds) |
| `rating_submitted` | User submits a rating for a watched movie |
| `watchlist_added` | User adds a movie to their watchlist |

These events are ingested into **`main.lab_data.movie_stream_events`** — a Delta table with
**Change Data Feed (CDF)** enabled, allowing downstream consumers to process row-level changes
(inserts, updates, deletes) as a CDC stream.

### Key Challenges Addressed

1. **Network unreliability** — Producers may not receive ACKs in time and re-send events
   (at-least-once delivery semantics), causing duplicates.
2. **Idempotent ingestion** — The pipeline must guarantee that replayed events do not produce
   duplicate rows in the target Delta table.
3. **Operational simplicity** — Eliminate the need for always-on broker infrastructure (Kafka
   clusters, consumer groups, partition management) while maintaining reliable delivery.

### Notebook Cell Map

| Part | Title | Purpose |
| --- | --- | --- |
| **Prerequisites** | SDK Install & Configuration | Install `databricks-zerobus-ingest-sdk`, configure service principal credentials, grant Unity Catalog permissions |
| **Part 1** | Setup — Delta Table with CDF | Create `movie_stream_events` table with `delta.enableChangeDataFeed = true` using `CREATE TABLE IF NOT EXISTS` |
| **Part 2** | Event Generation with Duplicates | Generate 5 unique + 2 duplicate events (7 total) using Unix microsecond timestamps |
| **Part 3** | Push-Based Ingestion via Zerobus SDK | Push events directly to Delta via `ZerobusSdk.create_stream()` and `ingest_record_offset()` over gRPC |
| **Part 4** | Idempotency via Deduplication View | Create `movie_stream_events_dedup` view using `ROW_NUMBER() PARTITION BY event_id` for exactly-once semantics |
| **Part 5** | Testing Network Retry Behavior | Re-send the same batch via Zerobus → verify raw table grows but dedup view stays at 5 rows |
| **Part 6** | Architecture Comparison | Compare Kafka vs Zerobus across cost, operations, and use cases with TCO estimates |

---

## 2. Architecture Comparison: Kafka-Style (Bus) vs Zero-Bus (Direct Ingest)

### Traditional Bus-Based Architecture (Multi-Sink)

```
                        ┌──────────────────────────────────────────────────┐
                        │              Kafka Cluster (KRaft)                │
                        │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
                        │  │Partition 0│  │Partition 1│  │Partition 2│      │
                        │  └──────────┘  └──────────┘  └──────────┘      │
                        └──────┬───────────────┬───────────────┬─────────┘
                               │               │               │
          ┌────────────────────┘               │               └────────────────────┐
          │                                    │                                    │
          ▼                                    ▼                                    ▼
┌──────────────────┐              ┌──────────────────┐              ┌──────────────────┐
│  24/7 Spark      │              │  Microservice A   │              │  Microservice B   │
│  Structured      │              │  (e.g. Billing)   │              │  (e.g. RecSys)    │
│  Streaming       │              │                   │              │                   │
│  Compute (ALWAYS │              │                   │              │                   │
│  ON)             │              │                   │              │                   │
└────────┬─────────┘              └──────────────────┘              └──────────────────┘
         │
         ▼
┌──────────────────┐
│  Delta Lake       │
│  (UC Table)       │
└──────────────────┘

  Producer ──► Kafka Brokers (Partitions / KRaft) ──► [Spark Streaming 24/7] ──► Delta Lake
                                            ──► [Microservice A]  (fan-out)
                                            ──► [Microservice B]  (fan-out)
```

### Zero-Bus Architecture (Single-Sink Direct Ingest)

```
                         ┌─────────────────────────────┐
                         │  Databricks Serverless       │
                         │  Managed Ingest Endpoint     │
                         │  (HTTPS / Ingest API)         │
                         │  (pay-per-ingested-byte)      │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │  Unity Catalog Delta Lake    │
                         │  main.lab_data.               │
                         │  movie_stream_events          │
                         │  (CDF enabled)                │
                         └─────────────────────────────┘

  Producer ──► Serverless Direct Push (HTTPS) ──► Delta Lake (UC Table)
  No brokers. No partitions. No consumer groups. No always-on compute.
```

### Detailed Comparison Table

| Dimension | Kafka-Style (Bus) | Zero-Bus (Direct Ingest) |
| --- | --- | --- |
| **Architectural Pattern** | Multi-Sink: producer → broker → N independent consumers | Single-Sink: producer → serverless endpoint → Delta Lake |
| **Infrastructure & Operational Complexity** | High — manage Kafka broker clusters, partitions, consumer groups, offset tracking, KRaft/ ZooKeeper quorum, schema registry, monitoring | Low — fully managed serverless endpoint; no brokers, partitions, or consumer groups to operate |
| **Total Cost of Ownership (TCO)** | Always-on compute (24/7 Spark Streaming) + broker EC2/ECS instances + network + storage. Costs accrue even during idle periods with zero events | Pay-per-ingested-byte on serverless compute. No idle costs. Cost scales linearly with actual event volume |
| **System Decoupling & Consumer Flexibility** | Strong decoupling — multiple transactional systems (billing, recommendation, notification) consume the same stream independently via separate consumer groups. Fan-out is a first-class feature | Centralized lakehouse ingestion — all events land in Delta Lake first. Downstream consumers read from the Delta table via batch or structured streaming queries. Decoupling is achieved at the storage layer, not the broker layer |
| **Latency** | Sub-millisecond to low single-digit milliseconds (intra-cluster, Kafka-to-consumer). End-to-end producer-to-Delta depends on Spark Streaming micro-batch interval (typically 100ms–seconds) | Latency determined by the HTTPS ingest API round-trip + Delta commit. Typically low single-digit seconds. Not suited for <10ms microservice-to-microservice communication |
| **Throughput** | Very high — millions of events/sec across partitioned topics. Throughput scales horizontally by adding partitions and brokers | Moderate-to-high — bounded by serverless endpoint capacity. Sufficient for lakehouse analytics pipelines but not designed for ultra-high-throughput inter-service messaging |
| **Delivery Semantics** | Configurable: at-most-once, at-least-once, exactly-once (via transactions / idempotent producers) | At-least-once (network retries may re-send events). Idempotency achieved via downstream deduplication view (ROW_NUMBER() PARTITION BY event_id) |
| **Replay Capability** | Native replay via Kafka offsets — consumers can rewind to any position in the stream | Replay via Delta Lake time travel / CDF — `DESCRIBE HISTORY` or CDF reads can reconstruct prior states |
| **Schema Evolution** | Requires external Schema Registry (Confluent) for Avro/Protobuf schema management | Native schema evolution through Delta Lake — `ALTER TABLE ADD COLUMNS`, schema-on-read, `MERGE WITH SCHEMA EVOLUTION` |
| **Governance** | Broker-level ACLs, separate from lakehouse governance | Native Unity Catalog governance — row-level security, column masks, audit logs, lineage, tags, all in one place |
| **Operational Maintenance** | Patch brokers, monitor lag, rebalance partitions, handle consumer group rebalances, manage retention policies | Zero maintenance — serverless endpoint auto-scales and is fully managed by Databricks |

---

## 3. Idempotent Processing & Deduplication Strategy

### Why Network Retries Cause Duplicates

In real-world streaming systems, the network between the producer and the ingestion endpoint
is inherently unreliable. The typical failure mode:

1. Producer sends an event with `event_id = X`.
2. The Zerobus endpoint receives and commits the event to Delta Lake.
3. The network ACK is lost or delayed (timeout, DNS blip, connection reset).
4. The producer **does not know** whether the event was persisted.
5. The producer **re-sends** the same event (same `event_id = X`).

This results in **at-least-once delivery** — the event is delivered one or more times. Without
idempotent handling, each re-send creates a duplicate row in the target table.

### Downstream Deduplication Strategy

**Key Insight:** Zerobus Ingest provides **at-least-once delivery** and only performs INSERT operations
(no MERGE/upsert capability). Instead of deduplicating during ingestion, we achieve idempotency at the
**consumption layer** via a deduplication view.

#### The Two-Layer Pattern

1. **Raw Table** (`movie_stream_events`): Contains all records including duplicates from network retries
   - Provides at-least-once delivery semantics
   - Full audit trail of every ingestion attempt
   - Enables replay and debugging

2. **Dedup View** (`movie_stream_events_dedup`): Provides exactly-once semantics for consumers
   - Uses `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at)` to keep only the first occurrence
   - All downstream consumers query the view, not the raw table
   - Zero maintenance overhead (automatically reflects raw table updates)

#### Implementation

```python
# Zerobus ingestion (INSERT-only, at-least-once)
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties

sdk = ZerobusSdk(ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL)
stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, TableProperties(TARGET_TABLE))

for event in events_data:
    record = event.copy()
    record["ingested_at"] = int(datetime.now().timestamp() * 1000000)  # Unix microseconds
    stream.ingest_record_offset(record)

stream.flush()  # Block until all records are durable
stream.close()
```

```sql
-- Deduplication view (exactly-once for consumers)
CREATE OR REPLACE VIEW movie_stream_events_dedup AS
SELECT event_id, user_id, movie_title, event_type, watch_time_seconds, event_timestamp, ingested_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
    FROM movie_stream_events
)
WHERE rn = 1
```

**Why this matters:** When the producer re-sends a previously committed batch (network retry),
Zerobus INSERTs the duplicate records into the raw table. The dedup view automatically filters
them out, keeping only the first occurrence of each `event_id`. Consumers see exactly-once
semantics without any changes to their queries.

### Verification Test Results

The notebook includes a deliberate stress test:

| Phase | Events Pushed | Raw Table Rows | Dedup View Rows |
| --- | --- | --- | --- |
| **Initial Ingest** | 7 (5 unique + 2 intentional duplicates) | 7 | **5** |
| **Network Retry** | 7 (same batch re-sent via Zerobus) | 14 | **5** (unchanged) |

**Result:** The raw table grows to 14 rows (all duplicates preserved), but the dedup view shows
exactly **5 unique records** throughout. This proves the system handles network retries gracefully
at the consumption layer.

### Idempotency Guarantee

> For any batch `B` and any positive integer `k`, ingesting `B` exactly `k` times produces the
> same dedup view state as ingesting `B` exactly once.

This is the defining property of an idempotent system. The implementation satisfies it because:

1. Zerobus Ingest provides reliable at-least-once delivery (no data loss)
2. The raw table preserves every ingestion attempt (full audit trail)
3. The dedup view uses `ROW_NUMBER() ... ORDER BY ingested_at` to deterministically select the first occurrence
4. Delta Lake's ACID guarantees ensure consistent reads

---

## 4. Architectural Trade-offs & Discussion

### When to Choose Kafka / Message Bus

| Criterion | Details |
| --- | --- |
| **Low-latency inter-service communication** | Kafka delivers sub-10ms producer-to-consumer latency for microservice-to-microservice messaging. Zero-Bus adds HTTPS round-trip + Delta commit latency (seconds). |
| **Complex fan-out** | When multiple transactional systems (billing, recommendation engine, notification service, fraud detection) must consume the same event stream **independently** and **in real time**, each with its own consumer group, offset tracking, and replay position. |
| **High-throughput event mesh** | Kafka can handle millions of events/sec across partitioned topics with horizontal scaling. Zero-Bus is not designed for this scale of inter-service messaging. |
| **Existing Kafka infrastructure** | If the organization already operates Kafka clusters with mature tooling (schema registry, Kafka Connect, monitoring), the marginal cost of adding a new consumer is low. |
| **Strict ordering guarantees** | Kafka preserves partition-level ordering with per-partition consumer offsets. Zero-Bus ordering depends on Delta commit order, which is coarser. |

### When to Choose Zero-Bus / Direct Lakehouse Ingest

| Criterion | Details |
| --- | --- |
| **Lakehouse-centric pipelines** | When the primary consumer is the lakehouse itself (BI dashboards, ML feature engineering, ad-hoc analytics), there is no need for a broker intermediary — the producer can write directly to Delta Lake. |
| **Cost-sensitive environments** | Zero-Bus eliminates always-on compute (24/7 Spark Streaming clusters, Kafka broker instances). Serverless pricing means you pay only for bytes ingested, with zero idle cost. |
| **Minimal operational maintenance** | No brokers to patch, no partitions to rebalance, no consumer group lag to monitor, no retention policies to manage. The serverless endpoint is fully managed by Databricks. |
| **Unified governance** | All events land directly in a Unity Catalog Delta table — row-level security, column masks, audit logs, lineage, and governed tags are available immediately, without a separate governance layer. |
| **Change Data Feed as a streaming source** | With CDF enabled on the target Delta table, downstream structured streaming jobs can consume row-level changes (inserts, updates, deletes) — effectively turning the Delta table into a CDC source that replaces the Kafka topic. |
| **Simpler developer experience** | The producer uses a simple HTTPS ingest API. No Kafka client libraries, no serialization format negotiation, no partition key strategy, no consumer group configuration. |

### Decision Framework

```
                    ┌─────────────────────────────┐
                    │  Do multiple independent      │
                    │  systems need real-time       │
                    │  access to the same stream?  │
                    └──────────┬──────────────────┘
                               │
                    ┌──────────▼──────────────────┐
                    │   YES          │     NO        │
                    │      │         │       │       │
                    ▼      ▼         │       ▼       │
           ┌──────────────┐         │  ┌──────────────┐
           │  Is <10ms     │         │  │ Is the       │
           │  latency      │         │  │ lakehouse    │
           │  required?   │         │  │ the primary  │
           └──────┬───────┘         │  │ consumer?    │
                  │                 │  └──────┬───────┘
          ┌───────▼───────┐         │  ┌───────▼───────┐
          │ YES    NO     │         │  │ YES    NO     │
          │  │      │     │         │  │  │      │     │
          ▼  ▼      ▼     │         │  ▼  ▼      ▼     │
   ┌──────────┐ ┌──────┐ │         │ ┌──────────┐ ┌──────┐
   │  KAFKA   │ │KAFKA │ │         │ │ ZERO-BUS │ │KAFKA │
   │ (bus)    │ │(bus) │ │         │ │ (direct) │ │(bus) │
   └──────────┘ └──────┘ │         │ └──────────┘ └──────┘
```

---

## 5. Critical Implementation Notes

### Timestamp Format Requirements

**CRITICAL:** Zerobus Ingest requires TIMESTAMP columns to be provided as **Unix microseconds (integer format)**,
not as formatted timestamp strings. This is a strict schema validation requirement.

```python
# ✅ CORRECT: Unix microseconds (integer)
current_time_us = int(datetime.now().timestamp() * 1000000)
event = {
    "event_id": str(uuid.uuid4()),
    "event_timestamp": current_time_us,  # Integer: 1727451440690000
    # ... other fields
}

# ❌ WRONG: Formatted timestamp strings (will fail with "invalid digit found in string")
event = {
    "event_timestamp": "2026-09-27 16:27:20",  # String format rejected
    "event_timestamp": "2026-09-27T16:27:20Z",  # ISO8601 rejected
    "event_timestamp": "2026-09-27T16:27:20.690000",  # With microseconds still rejected
}
```

**Error symptom:** `Record decoder/encoder error: invalid digit found in string at line 1 column XXX`
always points to a timestamp field position.

### Table Requirements

- Must be a **managed Delta table** (not external)
- Use `CREATE TABLE IF NOT EXISTS` — Zerobus does not support recreating target tables
- Change Data Feed (CDF) is optional but recommended for downstream CDC consumers

### Service Principal Permissions

The service principal requires three Unity Catalog permissions. Credentials are stored in
Databricks Secrets (scope: `zerobus-lab11`) — no secrets are hardcoded in the notebook.

**One-time secret scope setup** (run in a notebook or via SDK):

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope("zerobus-lab11", initial_manage_principal="users")
w.secrets.put_secret("zerobus-lab11", "client-id", string_value="<service-principal-uuid>")
w.secrets.put_secret("zerobus-lab11", "client-secret", string_value="<service-principal-secret>")
```

**Notebook loads credentials via:**
```python
CLIENT_ID = dbutils.secrets.get(scope="zerobus-lab11", key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope="zerobus-lab11", key="client-secret")
```

**Grant permissions** (run once, uses `CLIENT_ID` from secrets):

```python
for stmt in [
    f"GRANT USE CATALOG ON CATALOG main TO `{CLIENT_ID}`",
    f"GRANT USE SCHEMA ON SCHEMA main.lab_data TO `{CLIENT_ID}`",
    f"GRANT MODIFY, SELECT ON TABLE main.lab_data.movie_stream_events TO `{CLIENT_ID}`",
]:
    spark.sql(stmt)
```

*Lab 11 — Zero-Bus Streaming with Zerobus · Databricks Assignment · 2026*