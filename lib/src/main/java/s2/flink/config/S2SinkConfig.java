package s2.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

public class S2SinkConfig {

  public static final ConfigOption<String> S2_SINK_BASIN =
      ConfigOptions.key("s2.sink.basin").stringType().noDefaultValue();

  public static final ConfigOption<String> S2_SINK_STREAM =
      ConfigOptions.key("s2.sink.stream").stringType().noDefaultValue();

  public static ReadableConfig validateForSink(ReadableConfig config) {
    return config;
  }
}
