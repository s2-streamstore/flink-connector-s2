package s2.flink.properties;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.configuration.ReadableConfig;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;

public class S2Config {

  public static final String AUTH_TOKEN = "s2.auth-token";
  public static final String ENDPOINTS_CLOUD = "s2.endpoints.cloud";
  public static final String ENDPOINTS_ACCOUNT = "s2.endpoints.account";
  public static final String ENDPOINTS_BASIN = "s2.endpoints.basin";
  public static final String APPEND_RETRY_POLICY = "s2.append-retry-policy";
  public static final String MAX_RETRIES = "s2.max-retries";
  public static final String RETRY_DELAY_MS = "s2.retry-delay-ms";

  public static Properties fromReadableConfig(ReadableConfig config) {
    final Properties properties = new Properties();
    config.toMap().entrySet().stream()
        .filter(e -> e.getKey().startsWith("s2."))
        .forEach(e -> properties.put(e.getKey(), e.getValue()));
    return properties;
  }

  public static Config fromProperties(Properties configProperties) {
    final var token = configProperties.getProperty(AUTH_TOKEN);
    if (token == null) {
      throw new RuntimeException("token is required");
    }
    final var builder = Config.newBuilder(token);
    builder.withEndpoints(
        Endpoints.manual(
            Optional.ofNullable(configProperties.getProperty(ENDPOINTS_CLOUD)),
            Optional.ofNullable(configProperties.getProperty(ENDPOINTS_ACCOUNT)),
            Optional.ofNullable(configProperties.getProperty(ENDPOINTS_BASIN))));

    Optional.ofNullable(configProperties.getProperty(APPEND_RETRY_POLICY))
        .map(AppendRetryPolicy::valueOf)
        .ifPresent(builder::withAppendRetryPolicy);

    Optional.ofNullable(configProperties.getProperty(MAX_RETRIES))
        .map(Integer::parseInt)
        .ifPresent(builder::withMaxRetries);

    Optional.ofNullable(configProperties.getProperty(RETRY_DELAY_MS))
        .map(Long::parseLong)
        .map(Duration::ofMillis)
        .ifPresent(builder::withRetryDelay);

    return builder.build();
  }
}
