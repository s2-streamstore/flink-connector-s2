# flink-connector-s2

> [!WARNING]
> This connector is in alpha release.

This repo contains a Flink connector for reading from and writing to [S2 streams](https://s2.dev).

## Features

- Source
    - Read from a fixed set of s2 streams, or periodically discover streams matching a configured
      prefix
        - Streams are all unbounded, and will attempt to be consumed for the lifetime of the job;
          there is no current way to supply a limit
    - Can be used with DataStream, Table, and SQL Flink APIs
    - An optional "upsert-style" table connector
    - Metadata from original split (S2 stream) can be captured via implementors of
      `S2ContextDeserializationSchema`, which gets access to original stream name, and sequence
      number, in addition to each record's body (
      see [this example job](app/src/main/java/org/example/app/eventstream/EventStreamWithContextJob.java))
- Sink
    - Write to a single S2 stream
        - Partitioning across multiple streams needs to be done upstream, as there is currently a
          one sink : one stream relationship
    - Can be used with DataStream, Table, and SQL Flink APIs
    - Optional "upsert-style" table connector

This connector relies on the [S2 Java SDK](https://github.com/s2-streamstore/s2-sdk-java).

## Prerequisites

- Java 11 or higher
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
interleaved on the stream. The sink will not automatically write records with the writer id -- so if
it is important to preserve information about original ordering, that should be manually
injected in the streamed elements.

### Dynamic table sink (Table / SQL)

The `S2DynamicTableSink` wraps the `S2Sink` above, for use with
the [Table and SQL APIs](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/overview/).

This dynamic table only supports inserts.

See the example
in [S2InsertOnlyTable](https://github.com/s2-streamstore/flink-connector-s2/blob/main/app/src/main/java/org/example/app/S2InsertOnlyTable.java).

### Upsert table sink (Table / SQL)

An upsert-compatible dynamic table sink is also provided (`S2UpsertDynamicTableSink`). This is
modeled directly off of Kafka's
[upsert SQL connector](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/upsert-kafka/).

This sink supports update / delete / insert changelog actions. In order to achieve "upsert"
semantics, it does require that elements (instances of `RowData`) have a key. This can be configured
by using a schema with a `PRIMARY_KEY` defined. When reconstructing materialized state from the
underlying S2 stream, the latest row pertaining to a key is what should be considered as the current
value.

This sink only works with `RowData`, and operates by mapping all upsert rows into an
update row that can be appended to an S2 stream. The corresponding original action (`RowKind`) is
encoded as a header in
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

## Source

Splits are S2 streams. A list of streams can be provided to a source via the `s2.source.streams`
property, or they can be discovered dynamically by listing streams within a basin, and selecting
based on a prefix (`s2.source.discovery-prefix`). If discovery is used, an optional refresh
frequency can be supplied (`s2.source.discovery-interval-ms`).

Splits, when initially loaded, will either start reading from the earliest sequence number, or wait
for the next sequence number (determined at the time of split loading). This can be configured via
the `s2.source.start-behavior` property.

## Configuration

The sink and source, respectively, surface many knobs for configuration:

- `s2.client.*` properties affect the S2 SDK client
    - An `auth-token` is required for either sink or source use
- `s2.sink.*` properties are only relevant if using the sink
- `s2.source.*` properties, similarly, only relevant if using a source

The sink implementation extends Flink's `AsyncSinkBase` (
see [here](https://flink.apache.org/2022/03/16/the-generic-asynchronous-base-sink/)), and also
exposes some properties.

A list of all `s2.*` configurations is below:

| namespace | name                      | required                          | about | const                          | value                                    |
|-----------|---------------------------|-----------------------------------|-------|--------------------------------|------------------------------------------|
| s2.client | auth-token                | yes                               |       | s2.flink.config.S2ClientConfig | String                                   |
| s2.client | endpoints-cloud           | no                                |       | s2.flink.config.S2ClientConfig | String                                   |
| s2.client | endpoints-account         | no                                |       | s2.flink.config.S2ClientConfig | String                                   |
| s2.client | endpoints-basin           | no                                |       | s2.flink.config.S2ClientConfig | String                                   |
| s2.client | append-retry-policy       | no                                |       | s2.flink.config.S2ClientConfig | s2.config.AppendRetryPolicy              |
| s2.client | max-retries               | no                                |       | s2.flink.config.S2ClientConfig | int                                      |
| s2.client | retry-delay-ms            | no                                |       | s2.flink.config.S2ClientConfig | long                                     |
| s2.sink   | basin                     | yes                               |       | s2.flink.config.S2SinkConfig   | String                                   |
| s2.sink   | stream                    | yes                               |       | s2.flink.config.S2SinkConfig   | String                                   |
| s2.source | basin                     | yes                               |       | s2.flink.config.S2SourceConfig | String                                   |
| s2.source | streams                   | either this or `discovery-prefix` |       | s2.flink.config.S2SourceConfig | List<String>                             |
| s2.source | discovery-prefix          | either this or `streams`          |       | s2.flink.config.S2SourceConfig | String                                   |
| s2.source | discovery-interval-ms     | no                                |       | s2.flink.config.S2SourceConfig | long                                     |
| s2.source | start-behavior            | no                                |       | s2.flink.config.S2SourceConfig | s2.flink.source.split.SplitStartBehavior |
| s2.source | read-session-buffer-bytes | no                                |       | s2.flink.config.S2SourceConfig | int                                      |
| s2.source | read-session-heartbeater  | no                                |       | s2.flink.config.S2SourceConfig | boolean                                  |

## Demos

See the [app](./app) submodule for some demo applications. In particular,
the [EventStreamJob](./app/src/main/java/org/example/app/eventstream/EventStreamJob.java) shows an
end-to-end example involving both source and sink (regular and upsert). A walkthrough of how to
setup that job is available in the [README](./app/README.md).