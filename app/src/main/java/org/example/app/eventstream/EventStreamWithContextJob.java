package org.example.app.eventstream;

import static s2.flink.config.S2ClientConfig.S2_AUTH_TOKEN;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_BASIN;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_SPLIT_START_BEHAVIOR;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_INTERVAL_MS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_PREFIX;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import s2.flink.source.S2Source;
import s2.flink.source.serialization.S2Context;
import s2.flink.source.serialization.S2ContextWrappingDeserializationSchema;
import s2.flink.source.split.SplitStartBehavior;

public class EventStreamWithContextJob {

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(4, config);

    final Configuration s2DataStreamSourceConfig =
        new Configuration()
            .set(S2_AUTH_TOKEN, System.getenv("S2_AUTH_TOKEN"))
            .set(S2_SOURCE_BASIN, System.getenv("S2_BASIN"))
            .set(S2_SOURCE_STREAM_DISCOVERY_PREFIX, "host/")
            .set(S2_SOURCE_STREAM_DISCOVERY_INTERVAL_MS, 30_000L)
            .set(S2_SOURCE_SPLIT_START_BEHAVIOR, SplitStartBehavior.FIRST);

    DataStream<S2Context<String>> dsWithContext =
        env.fromSource(
                new S2Source<S2Context<String>>(
                    s2DataStreamSourceConfig,
                    new S2ContextWrappingDeserializationSchema<>(new SimpleStringSchema())),
                WatermarkStrategy.noWatermarks(),
                "s2-source")
            .returns(new TypeHint<S2Context<String>>() {});

    dsWithContext.print();
    env.execute();
  }
}
