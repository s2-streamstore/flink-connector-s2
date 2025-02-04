package s2.flink.table;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import s2.flink.properties.S2Config;

public class S2DynamicTableFactory extends AsyncDynamicTableSinkFactory {

  public static final ConfigOption<String> BASIN =
      ConfigOptions.key("s2.basin").stringType().noDefaultValue().withDescription("S2 basin name");

  public static final ConfigOption<String> STREAM =
      ConfigOptions.key("s2.stream")
          .stringType()
          .noDefaultValue()
          .withDescription("S2 stream name");

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

    final var prop = factoryContext.getTableOptions();
    final var builder = new S2DynamicTableSink.S2DynamicTableSinkBuilder();
    var asyncSinkConfig = new AsyncSinkConfigurationValidator(prop).getValidatedConfigurations();
    addAsyncOptionsToBuilder(asyncSinkConfig, builder);
    return builder
        .setConsumedDataType(factoryContext.getPhysicalDataType())
        .setEncodingFormat(factoryContext.getEncodingFormat())
        .setS2ClientProperties(S2Config.fromReadableConfig(prop))
        .setBasin(prop.get(BASIN))
        .setStream(prop.get(STREAM))
        .build();
  }

  @Override
  public String factoryIdentifier() {
    return "s2";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(BASIN, STREAM);
  }
}
