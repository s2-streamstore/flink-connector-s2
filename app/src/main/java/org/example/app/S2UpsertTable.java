package org.example.app;

import static org.apache.flink.table.api.Expressions.$;

import java.time.Duration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.example.app.utils.RandomStringSource;

public class S2UpsertTable {

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

    tEnv.createTemporaryTable(
        "S2Table",
        TableDescriptor.forConnector("s2-upsert")
            .schema(
                Schema.newBuilder()
                    .column("index", DataTypes.STRING().notNull())
                    .column("content", DataTypes.BIGINT())
                    .primaryKey("index")
                    .build())
            .format("json")
            .option("s2.auth-token", System.getenv("S2_AUTH_TOKEN"))
            .option("s2.basin", System.getenv("S2_BASIN"))
            .option("s2.stream", System.getenv("S2_STREAM"))
            .option("s2.endpoints.account", System.getenv("S2_ACCOUNT_ENDPOINT"))
            .option("s2.endpoints.basin", System.getenv("S2_BASIN_ENDPOINT"))
            .build());

    DataStream<String> source = env.addSource(new RandomStringSource(1000));
    DataStream<Tuple2<String, String>> ds =
        source
            .map(
                (MapFunction<String, Tuple2<String, String>>)
                    s -> {
                      String[] elems = s.split("=");
                      return Tuple2.of(elems[0], elems[1]);
                    })
            .returns(new TypeHint<Tuple2<String, String>>() {});

    Table dsTable =
        tEnv.fromDataStream(
            ds,
            Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .build());

    dsTable.printSchema();

    Table incrementalAgg =
        dsTable.groupBy($("f0")).select($("f0").as("index"), $("f1").count().as("count"));

    incrementalAgg.insertInto("S2Table").execute();
  }
}
