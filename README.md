# flink-connector-s2

> [!WARNING]
> This connector is in alpha release.

This repo contains a Flink connector for reading from (soon) and writing to (currently) S2 streams.

It relies on the [S2 Java SDK](https://github.com/s2-streamstore/s2-sdk-java).

## Prerequisites

- Java 17 or higher
- Gradle 8.5 or higher
- An S2 account and bearer token

### Building from Source

1. Clone the repository:

```bash
git clone \
  https://github.com/s2-streamstore/flink-connector-s2
```

2. Build the project:

```bash
./gradlew build
```

3. Install to local Maven repository:

```bash
./gradlew publishToMavenLocal
```

## Sink

The `S2Sink` can be used with
the [DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/overview/).

A sink is always associated with a single S2 stream.

The parallelism of the sink directly corresponds to the number of `S2SinkWriter` instances, and also
the number of active S2 `AppendSession` RPCs against the underlying stream.

If the sink is used with a `parallelism=1`,
then the order in which elements are received by the sink should be identical to the order in which
they become persisted on the S2 stream. If retries are configured, however, **there could be
duplicates**, as the sink does not currently support a mechanism for idempotent appends.

If the sink is run with `parallelism>1`, then the appends from multiple sink writers will be
interleaved on the stream. The sink will not stamp the records with the writer id, or anything like
that -- so if it is important to preserve information about ordering, that should be manually
injected in the streamed elements.

See the example
in [S2DataStream](https://github.com/s2-streamstore/flink-connector-s2/blob/main/app/src/main/java/org/example/app/S2DataStream.java).

### Dynamic table sink (Table / SQL)

The `S2DynamicTableSink` wraps the `S2Sink` above, for use with
the [Table and SQL APIs](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/overview/).

This dynamic table only supports inserts.

See the example
in [S2InsertOnlyTable](https://github.com/s2-streamstore/flink-connector-s2/blob/main/app/src/main/java/org/example/app/S2InsertOnlyTable.java).

### Upsert sink

An upsert-compatible dynamic table sink is also provided (`S2UpsertDynamicTableSink`). This is
modelled directly off of Kafka's
[upsert SQL connector](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/upsert-kafka/).

This sink supports update / delete / insert changelog actions. In order to achieve "upsert"
semantics, it does require that elements (instances of `RowData`) have a key. This can be configured
by using a schema with a `PRIMARY_KEY` defined. When reconstructing materialized state from the
underlying S2 stream, the latest row pertaining to a key is what should be considered as the current
value.

This sink only works with `RowData`, and operates by mapping all update/insert/delete rows into an
insert row that can be appended to an S2 stream. The corresponding action is encoded as a header in
the resulting record, as is the key.

You can test this out by inspecting records from a stream that is written to from the upsert sink
using the [s2-cli](https://github.com/s2-streamstore/s2-cli):

```bash
s2 read s2://my-basin/my-change-log --start-seq-num 43042 --format json
```

...where individual rows look like:

```json
{
  "seq_num": 43042,
  "headers": [
    [
      "@key",
      "{\"index\":\"265\"}"
    ],
    [
      "@action",
      "u"
    ]
  ],
  "body": "{\"content\":4}"
}
```

See the example
in [S2UpsertTable](https://github.com/s2-streamstore/flink-connector-s2/blob/main/app/src/main/java/org/example/app/S2UpsertTable.java).

### Configuration

| name                       | required | unit   | type   | constraints                | via |
|----------------------------|----------|--------|--------|----------------------------|-----|
| s2.basin                   | yes      | -      | string |                            |     |
| s2.stream                  | yes      | -      | string |                            |     |
| s2.auth-token              | yes      | -      | string |                            |     |
| s2.append-retry-policy     |          | -      | string | "ALL" or "NO_SIDE_EFFECTS" |     |
| s2.endpoints.account       |          | -      | string | valid url                  |     |
| s2.endpoints.basin         |          | -      | string | valid url                  |     |
| s2.max-retries             |          | count  | int    |                            |     |
| s2.retry-delay-ms          |          | millis | int    |                            |     |
| sink.batch.max-size        |          | count  | int    | 0 <= 1000                  |     |
| sink.flush-buffer.size     |          | bytes  | long   |                            |     |
| sink.flush-buffer.timeout  |          | millis | long   |                            |     |
| sink.requests.max-buffered |          | count  | int    |                            |     |
| sink.requests.max-inflight |          | count  | int    |                            |     |

## Source

_In progress!_

