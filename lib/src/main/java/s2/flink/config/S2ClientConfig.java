package s2.flink.config;

import java.time.Duration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;

public class S2ClientConfig {

  public static final ConfigOption<String> S2_AUTH_TOKEN =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.auth-token")
          .stringType()
          .noDefaultValue();

  public static ConfigOption<String> S2_ENDPOINTS_CLOUD =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.endpoints-cloud")
          .stringType()
          .noDefaultValue();

  public static ConfigOption<String> S2_ENDPOINTS_ACCOUNT =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.endpoints-account")
          .stringType()
          .noDefaultValue();

  public static ConfigOption<String> S2_ENDPOINTS_BASIN =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.endpoints-basin")
          .stringType()
          .noDefaultValue();

  public static ConfigOption<AppendRetryPolicy> S2_APPEND_RETRY_POLICY =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.append-retry-policy")
          .enumType(AppendRetryPolicy.class)
          .noDefaultValue();

  public static ConfigOption<Integer> S2_MAX_RETRIES =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.max-retries")
          .intType()
          .noDefaultValue();

  public static ConfigOption<Long> S2_RETRY_DELAY_MS =
      org.apache.flink.configuration.ConfigOptions.key("s2.client.retry-delay-ms")
          .longType()
          .noDefaultValue();

  public static Config fromConfig(ReadableConfig config) {
    final var token = config.getOptional(S2_AUTH_TOKEN);
    if (token.isEmpty()) {
      throw new RuntimeException("S2 auth token is required.");
    }
    final var builder = Config.newBuilder(token.get());
    builder.withEndpoints(
        Endpoints.manual(
            config.getOptional(S2_ENDPOINTS_CLOUD),
            config.getOptional(S2_ENDPOINTS_ACCOUNT),
            config.getOptional(S2_ENDPOINTS_BASIN)));

    config.getOptional(S2_APPEND_RETRY_POLICY).ifPresent(builder::withAppendRetryPolicy);
    config.getOptional(S2_MAX_RETRIES).ifPresent(builder::withMaxRetries);
    config
        .getOptional(S2_RETRY_DELAY_MS)
        .map(Duration::ofMillis)
        .ifPresent(builder::withRetryDelay);

    return builder.build();
  }

  public static ReadableConfig validateForSDKConfig(ReadableConfig config) {
    // TODO validate
    return config;
  }
}
