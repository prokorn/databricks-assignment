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

| Cell | Title | Purpose |
| --- | --- | --- |
| 1 | Setup — Catalog & Schema Context | Set `CATALOG = main`, `SCHEMA = lab_data` |
| 2 | Setup — Delta Table with CDF | Create `movie_stream_events` table with `delta.enableChangeDataFeed = true` |
| 3 | Step 1 — Event Producer with Intentional Duplicates | Generate 5 unique + 2 duplicate events (7 total) |
| 4 | Step 2 — Idempotent Ingestion via MERGE INTO | Two-tier dedup: `dropDuplicates` + `MERGE INTO ... WHEN NOT MATCHED` |
| 5 | Step 3 — Idempotency Test (Network Retry Simulation) | Re-send the same batch → verify row count stays at 5 |

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
| **Delivery Semantics** | Configurable: at-most-once, at-least-once, exactly-once (via transactions / idempotent producers) | At-least-once (network retries may re-send events). Idempotency must be implemented at the ingestion layer (MERGE INTO) |
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
2. The serverless endpoint receives and commits the event to Delta Lake.
3. The network ACK is lost or delayed (timeout, DNS blip, connection reset).
4. The producer **does not know** whether the event was persisted.
5. The producer **re-sends** the same event (same `event_id = X`).

This results in **at-least-once delivery** — the event is delivered one or more times. Without
idempotent handling, each re-send creates a duplicate row in the target table.

### Two-Tier Deduplication Implementation

The notebook implements deduplication at two levels:

#### Tier 1 — Intra-Batch Deduplication (`dropDuplicates`)

Before writing to the target table, the incoming DataFrame is de-duplicated within the batch
itself. This handles the case where the producer sends the same `event_id` multiple times in a
single batch:

```python
# Convert the event list to a Spark DataFrame and add ingestion timestamp
incoming_df = (spark.createDataFrame(events_data, schema=schema)
                   .withColumn("ingested_at", F.current_timestamp()))

# Drop intra-batch duplicates before writing
deduped_incoming = incoming_df.dropDuplicates(["event_id"])
deduped_incoming.createOrReplaceTempView("staged_incoming_events")
```

**Why this matters:** If a batch contains 7 events where 2 share `event_id` with earlier entries
in the same batch, `dropDuplicates(["event_id"])` collapses them to 5 unique rows before the
MERGE ever runs.

#### Tier 2 — Inter-Batch Idempotency (`MERGE INTO ... WHEN NOT MATCHED`)

The staged, de-duplicated batch is merged into the target Delta table using a conditional insert.
If the `event_id` already exists in the target (from a prior batch or retry), the row is silently
skipped:

```sql
MERGE INTO main.lab_data.movie_stream_events AS target
USING staged_incoming_events AS source
  ON target.event_id = source.event_id
WHEN NOT MATCHED THEN
  INSERT (event_id, user_id, movie_title, event_type,
          watch_time_seconds, event_timestamp, ingested_at)
  VALUES (source.event_id, source.user_id, source.movie_title, source.event_type,
          source.watch_time_seconds, source.event_timestamp, source.ingested_at)
```

**Why this matters:** When the producer re-sends a previously committed batch (network retry),
the MERGE's `WHEN NOT MATCHED` clause ensures that every `event_id` already present in the
target is ignored. The table converges to the same final state regardless of how many times a
batch is replayed.

### Verification Test Results

The notebook includes a deliberate stress test (Cell 3 → Cell 5):

| Phase | Events Generated | Events Sent to Target | Unique Rows in Table |
| --- | --- | --- | --- |
| **Batch 1 — Initial Ingest** | 7 (5 unique + 2 intentional duplicates) | 7 | **5** |
| **Batch 2 — Network Retry (same batch)** | — | 7 (re-sent) | **5** (unchanged) |

**Result:** The table contains exactly **5 unique records** after both ingestions. The 2
intra-batch duplicates were eliminated by `dropDuplicates`, and the 7 re-sent events in the
retry batch were all matched against existing rows and skipped by the `MERGE WHEN NOT MATCHED`
clause.

### Idempotency Guarantee

> For any batch `B` and any positive integer `k`, ingesting `B` exactly `k` times produces the
> same final table state as ingesting `B` exactly once.

This is the defining property of an idempotent write operation. The implementation satisfies it
because:

1. `dropDuplicates(["event_id"])` collapses intra-batch duplicates → each `event_id` appears at
   most once in the staged view.
2. `MERGE INTO ... ON target.event_id = source.event_id WHEN NOT MATCHED THEN INSERT` is a
   conditional insert keyed on the natural primary key (`event_id`).
3. Delta Lake's ACID transaction guarantees ensure the MERGE is atomic — no partial writes.

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

### Summary

| | Kafka-Style (Bus) | Zero-Bus (Direct Ingest) |
| --- | --- | --- |
| **Best for** | Real-time inter-service messaging, complex fan-out, ultra-low latency | Lakehouse analytics, cost optimization, operational simplicity |
| **Worst for** | Cost-sensitive workloads, teams without Kafka ops expertise | Sub-second inter-service communication, multi-consumer real-time fan-out |
| **This lab** | — | ✓ Implemented and verified |

---

*Lab 11 — Zero-Bus Streaming with Zerobus · Databricks Assignment · 2026*