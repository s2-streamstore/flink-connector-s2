package org.example.app;

import static s2.flink.config.S2ClientConfig.S2_AUTH_TOKEN;
import static s2.flink.config.S2SinkConfig.S2_SINK_BASIN;
import static s2.flink.config.S2SinkConfig.S2_SINK_STREAM;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_BASIN;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_SPLIT_START_BEHAVIOR;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAMS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_CADENCE_MS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_PREFIX;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import s2.flink.source.S2Source;
import s2.flink.source.split.SplitStartBehavior;

public class EventStream {

  private static final String WORKING_BASIN = "sgb-eventstream-t1";

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(config);
    final StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

    final Configuration s2DataStreamSourceConfig =
        new Configuration()
            .set(S2_AUTH_TOKEN, System.getenv("S2_AUTH_TOKEN"))
            .set(S2_SOURCE_BASIN, WORKING_BASIN)
            .set(S2_SOURCE_STREAM_DISCOVERY_PREFIX, "host/")
            .set(S2_SOURCE_STREAM_DISCOVERY_CADENCE_MS, 30_000L)
            .set(S2_SOURCE_SPLIT_START_BEHAVIOR, SplitStartBehavior.NEXT);

    DataStream<String> ds =
        env.fromSource(
                new S2Source<String>(s2DataStreamSourceConfig, new SimpleStringSchema()),
                WatermarkStrategy.noWatermarks(),
                "s2-source")
            .returns(new TypeHint<String>() {});

    var userEventStream =
        ds.flatMap(
                (FlatMapFunction<String, UserInteraction>)
                    (value, out) -> {
                      try {
                        final Map<String, String> map =
                            Arrays.stream(value.split(";", 3))
                                .map(
                                    kv -> {
                                      var kvElems = kv.split("=", 2);
                                      return Map.entry(kvElems[0], kvElems[1]);
                                    })
                                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                        // Search event.
                        out.collect(
                            new UserInteraction(
                                Optional.ofNullable(map.get("action")).orElse("search"),
                                Optional.ofNullable(map.get("user"))
                                    .map(Integer::parseInt)
                                    .orElse(null),
                                Optional.ofNullable(map.get("item"))
                                    .map(Integer::parseInt)
                                    .orElse(null),
                                map.get("search")));

                      } catch (Exception e) {
                        // Silently discard any bad data.
                      }
                    })
            .returns(new TypeHint<UserInteraction>() {});

    Table userEventsTable =
        tEnv.fromDataStream(
            userEventStream,
            Schema.newBuilder()
                .column("userId", DataTypes.INT().notNull())
                .column("action", DataTypes.STRING().notNull())
                .column("itemId", DataTypes.INT())
                .column("query", DataTypes.STRING())
                .columnByExpression("pt", "PROCTIME()")
                .build());

    tEnv.createTemporaryView("userEventsTable", userEventsTable);

    String query =
        """
    SELECT
      userId,
      matchedSearchQuery,
      matchedItemId
    FROM
      userEventsTable
      MATCH_RECOGNIZE (
        PARTITION BY userId
        ORDER BY pt
        MEASURES
          A.query AS matchedSearchQuery,
          C.itemId AS matchedItemId
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (A B C) WITHIN INTERVAL '10' MINUTES
        DEFINE
          A AS A.action = 'search',
          B AS B.action = 'cart',
          C AS C.action = 'buy'
      )
    """;

    final var itemQueries =
        Schema.newBuilder()
            .column("userId", DataTypes.BIGINT().notNull())
            .column("matchedSearchQuery", DataTypes.STRING().notNull())
            .column("matchedItemId", DataTypes.BIGINT().notNull())
            .primaryKey("userId")
            .build();

    tEnv.createTemporaryTable(
        "ItemQueriesSink",
        TableDescriptor.forConnector("s2")
            .schema(itemQueries)
            .format("json")
            .option(S2_AUTH_TOKEN, System.getenv("S2_AUTH_TOKEN"))
            .option(S2_SINK_BASIN, WORKING_BASIN)
            .option(S2_SINK_STREAM, "tables/item-queries-1")
            .build());

    Table result = tEnv.sqlQuery(query);
    result.insertInto("ItemQueriesSink").execute();

    tEnv.createTemporaryTable(
        "ItemQueriesSource",
        TableDescriptor.forConnector("s2")
            .schema(itemQueries)
            .format("json")
            .option(S2_AUTH_TOKEN, System.getenv("S2_AUTH_TOKEN"))
            .option(S2_SOURCE_BASIN, WORKING_BASIN)
            .option(S2_SOURCE_STREAMS, List.of("tables/item-queries-1"))
            .option(S2_SOURCE_SPLIT_START_BEHAVIOR, SplitStartBehavior.NEXT)
            .build());

    var topQueryPerItem =
        """
  SELECT item, top_query, weight
  FROM (
    SELECT
      matchedItemId as item,
      matchedSearchQuery as top_query,
      COUNT(*) AS weight,
      ROW_NUMBER() OVER (
        PARTITION BY matchedItemId
        ORDER BY COUNT(*) DESC
      ) AS rn
    FROM ItemQueriesSource
    GROUP BY matchedItemId, matchedSearchQuery
  )
  WHERE rn = 1
""";

    Table grouped = tEnv.sqlQuery(topQueryPerItem);

    final var topQueryPerItemSchema =
        Schema.newBuilder()
            .column("item", DataTypes.BIGINT().notNull())
            .column("top_query", DataTypes.STRING().notNull())
            .column("weight", DataTypes.BIGINT().notNull())
            .primaryKey("item")
            .build();

    tEnv.createTemporaryTable(
        "TopQueryPerItemSink",
        TableDescriptor.forConnector("s2-upsert")
            .schema(topQueryPerItemSchema)
            .format("json")
            .option(S2_AUTH_TOKEN, System.getenv("S2_AUTH_TOKEN"))
            .option(S2_SINK_BASIN, WORKING_BASIN)
            .option(S2_SINK_STREAM, "tables/top-query-per-item-1")
            .build());

    grouped.insertInto("TopQueryPerItemSink").execute();
  }

  public static class UserInteraction {

    public Integer userId;
    public String action;
    public Integer itemId;
    public String query;

    // Required for flink POJO
    public UserInteraction() {}

    public UserInteraction(String action, Integer userId, Integer itemId, String query) {
      this.action = action;
      this.userId = userId;
      this.itemId = itemId;
      this.query = query;
    }

    @Override
    public String toString() {
      return String.format(
          "action=%s, userId=%s, itemId=%s, query=%s", action, userId, itemId, query);
    }
  }
}
