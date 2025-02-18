package s2.flink.config;

import java.util.List;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import s2.flink.source.split.SplitStartBehavior;

public class S2SourceConfig {

  public static final ConfigOption<String> S2_SOURCE_BASIN =
      ConfigOptions.key("s2.source.basin").stringType().noDefaultValue();

  public static final ConfigOption<List<String>> S2_SOURCE_STREAMS =
      org.apache.flink.configuration.ConfigOptions.key("s2.source.streams")
          .stringType()
          .asList()
          .noDefaultValue();

  public static final ConfigOption<String> S2_SOURCE_STREAM_DISCOVERY_PREFIX =
      org.apache.flink.configuration.ConfigOptions.key("s2.source.discovery-prefix")
          .stringType()
          .noDefaultValue();

  public static final ConfigOption<Long> S2_SOURCE_STREAM_DISCOVERY_INTERVAL_MS =
      org.apache.flink.configuration.ConfigOptions.key("s2.source.discovery-interval-ms")
          .longType()
          .noDefaultValue();

  public static final ConfigOption<SplitStartBehavior> S2_SOURCE_SPLIT_START_BEHAVIOR =
      org.apache.flink.configuration.ConfigOptions.key("s2.source.start-behavior")
          .enumType(SplitStartBehavior.class)
          .defaultValue(SplitStartBehavior.FIRST);

  public static final ConfigOption<Integer> S2_PER_SPLIT_READ_SESSION_BUFFER_BYTES =
      org.apache.flink.configuration.ConfigOptions.key("s2.source.read-session-buffer-bytes")
          .intType()
          .defaultValue(1024 * 1024 * 10);

  public static <T extends ReadableConfig> T validateForSource(T config) {
    if (config.getOptional(S2_SOURCE_STREAMS).isEmpty()
        && config.getOptional(S2_SOURCE_STREAM_DISCOVERY_PREFIX).isEmpty()) {
      throw new IllegalArgumentException(
          "S2 source requires either static set of streams, or a discovery prefix.");
    }
    return config;
  }
}
