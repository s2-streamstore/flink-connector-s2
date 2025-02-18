package org.example.app.eventstream;

import static s2.flink.config.S2ClientConfig.S2_AUTH_TOKEN;
import static s2.flink.config.S2SinkConfig.S2_SINK_BASIN;
import static s2.flink.config.S2SinkConfig.S2_SINK_STREAM;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_BASIN;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_SPLIT_START_BEHAVIOR;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_INTERVAL_MS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_PREFIX;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
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
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import s2.flink.source.S2Source;
import s2.flink.source.split.SplitStartBehavior;

public class EventStreamJob {

  private static final String STREAM_EVENTSTREAM_PREFIX = "host/";
  private static final String STREAM_CONVERTING_QUERIES = "rollup/converting-queries-per-item";
  private static final String STREAM_TOP_QUERY_PER_ITEM =
      "feature/top-5-converting-queries-per-item";

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

    final var env =
        StreamExecutionEnvironment.getExecutionEnvironment(config).enableCheckpointing(5000);
    final StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

    if (env instanceof LocalStreamEnvironment) {
      env.setParallelism(4);
    }
    final String authToken =
        Preconditions.checkNotNull(getAuthToken(env), "auth token must be supplied");
    final String workingBasin =
        Preconditions.checkNotNull(getWorkingBasin(env), "basin must be supplied");

    final var statementSet = tEnv.createStatementSet();

    final Configuration s2DataStreamSourceConfig =
        new Configuration()
            .set(S2_AUTH_TOKEN, authToken)
            .set(S2_SOURCE_BASIN, workingBasin)
            .set(S2_SOURCE_STREAM_DISCOVERY_PREFIX, STREAM_EVENTSTREAM_PREFIX)
            .set(S2_SOURCE_STREAM_DISCOVERY_INTERVAL_MS, 30_000L)
            .set(S2_SOURCE_SPLIT_START_BEHAVIOR, SplitStartBehavior.FIRST);

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
                            Arrays.stream(value.split(";"))
                                .map(
                                    kv -> {
                                      var kvElems = kv.split("=", 2);
                                      return Map.entry(kvElems[0], kvElems[1]);
                                    })
                                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                        out.collect(
                            new UserInteraction(
                                Optional.ofNullable(map.get("action")).orElse("search"),
                                Optional.ofNullable(map.get("user"))
                                    .map(Integer::parseInt)
                                    .orElse(null),
                                Optional.ofNullable(map.get("item"))
                                    .map(Integer::parseInt)
                                    .orElse(null),
                                map.get("search"),
                                Long.parseLong(map.get("epoch_ms"))));

                      } catch (Exception e) {
                        System.err.println(e.getMessage());
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
                .column("epochMs", DataTypes.BIGINT().notNull())
                .columnByExpression("eventTime", "TO_TIMESTAMP_LTZ(epochMs, 3)")
                .watermark("eventTime", "eventTime - INTERVAL '5' SECONDS")
                .build());

    tEnv.createTemporaryView("UserEventsTable", userEventsTable);

    final String query =
        "SELECT\n"
            + "  userId,\n"
            + "  matchedSearchQuery,\n"
            + "  matchedItemId\n"
            + "FROM\n"
            + "  UserEventsTable\n"
            + "  MATCH_RECOGNIZE (\n"
            + "    PARTITION BY userId\n"
            + "    ORDER BY eventTime\n"
            + "    MEASURES\n"
            + "      A.query AS matchedSearchQuery,\n"
            + "      D.itemId AS matchedItemId\n"
            + "    ONE ROW PER MATCH\n"
            + "    AFTER MATCH SKIP TO NEXT ROW\n"
            + "    PATTERN (A B C D) WITHIN INTERVAL '10' MINUTES\n"
            + "    DEFINE\n"
            + "      A AS A.action = 'search',\n"
            + "      B AS B.action = 'view',\n"
            + "      C AS C.action = 'cart',\n"
            + "      D AS D.action = 'buy'\n"
            + "  )";

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
            .option(S2_AUTH_TOKEN, authToken)
            .option(S2_SINK_BASIN, workingBasin)
            .option(S2_SINK_STREAM, STREAM_CONVERTING_QUERIES)
            .build());

    Table result = tEnv.sqlQuery(query);
    statementSet.addInsert("ItemQueriesSink", result);
    tEnv.createTemporaryView("ItemQueries", result);

    final String top5ConvertingQueryPerItem =
        "WITH ranked AS (\n"
            + "  SELECT\n"
            + "    matchedItemId AS item,\n"
            + "    matchedSearchQuery AS top_query,\n"
            + "    ROW_NUMBER() OVER (PARTITION BY matchedItemId ORDER BY COUNT(*) DESC) AS rn\n"
            + "  FROM ItemQueries\n"
            + "  GROUP BY matchedItemId, matchedSearchQuery\n"
            + ")\n"
            + "SELECT \n"
            + "  item, \n"
            + "  ARRAY_AGG(top_query ORDER BY rn ASC) AS top_queries\n"
            + "FROM ranked\n"
            + "WHERE rn <= 5\n"
            + "GROUP BY item";

    Table grouped = tEnv.sqlQuery(top5ConvertingQueryPerItem);

    final var topQueryPerItemSchema =
        Schema.newBuilder()
            .column("item", DataTypes.BIGINT().notNull())
            .column("top_queries", DataTypes.ARRAY(DataTypes.STRING()))
            .primaryKey("item")
            .build();

    tEnv.createTemporaryTable(
        "TopQueryPerItemSink",
        TableDescriptor.forConnector("s2-upsert")
            .schema(topQueryPerItemSchema)
            .format("json")
            .option(S2_AUTH_TOKEN, authToken)
            .option(S2_SINK_BASIN, workingBasin)
            .option(S2_SINK_STREAM, STREAM_TOP_QUERY_PER_ITEM)
            .build());

    statementSet.addInsert("TopQueryPerItemSink", grouped);
    statementSet.execute();
  }

  private static String getAuthToken(StreamExecutionEnvironment env) throws IOException {
    if (env instanceof LocalStreamEnvironment) {
      return System.getenv("S2_AUTH_TOKEN");
    } else {
      return KinesisAnalyticsRuntime.getApplicationProperties()
          .get("eventstream")
          .getProperty("s2.auth-token");
    }
  }

  private static String getWorkingBasin(StreamExecutionEnvironment env) throws IOException {
    if (env instanceof LocalStreamEnvironment) {
      return System.getenv("S2_BASIN");
    } else {
      return KinesisAnalyticsRuntime.getApplicationProperties()
          .get("eventstream")
          .getProperty("s2.basin");
    }
  }

  public static class UserInteraction {

    public Integer userId;
    public String action;
    public Integer itemId;
    public String query;
    public Long epochMs;

    public UserInteraction() {}

    public UserInteraction(
        String action, Integer userId, Integer itemId, String query, Long epochMs) {
      this.action = action;
      this.userId = userId;
      this.itemId = itemId;
      this.query = query;
      this.epochMs = epochMs;
    }

    @Override
    public String toString() {
      return String.format(
          "action=%s, userId=%s, itemId=%s, query=%s, epochMs=%s",
          action, userId, itemId, query, epochMs);
    }
  }
}
