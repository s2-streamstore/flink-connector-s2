package org.example.app;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.app.utils.RandomStringSource;
import s2.flink.sink.S2Sink;
import s2.flink.sink.S2SinkBuilder;
import s2.flink.sink.S2SinkElementConverter;

public class S2DataStream {

  public static void main(String[] args) throws Exception {

    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironment(config);

    DataStream<String> sourceStream = env.addSource(new RandomStringSource(1000)).setParallelism(1);
    DataStream<Tuple2<String, String>> tupleStream =
        sourceStream
            .map(
                (MapFunction<String, Tuple2<String, String>>)
                    s -> {
                      String[] elems = s.split("=");
                      return Tuple2.of(elems[0], elems[1]);
                    })
            .returns(new TypeHint<Tuple2<String, String>>() {})
            .setParallelism(1);

    Properties properties = new Properties();
    properties.setProperty("s2.auth-token", System.getenv("S2_AUTH_TOKEN"));
    properties.setProperty("s2.endpoints.account", System.getenv("S2_ACCOUNT_ENDPOINT"));
    properties.setProperty("s2.endpoints.basin", System.getenv("S2_BASIN_ENDPOINT"));

    S2SinkBuilder<Tuple2<String, String>> sinkBuilder = S2Sink.newBuilder();
    S2Sink<Tuple2<String, String>> sink =
        sinkBuilder
            .setS2ConfigProperties(properties)
            .setBasin(System.getenv("S2_BASIN"))
            .setStream(System.getenv("S2_STREAM"))
            .setElementConverter(new S2SinkElementConverter<>(new JsonSerializationSchema<>()))
            .build();

    tupleStream.sinkTo(sink).setParallelism(1);

    env.execute();
  }
}
