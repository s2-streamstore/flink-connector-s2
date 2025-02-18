package s2.flink.table;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static s2.flink.config.S2ClientConfig.S2_AUTH_TOKEN;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class S2DynamicTableFactory extends AsyncDynamicTableSinkFactory
    implements DynamicTableSourceFactory {

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final AsyncDynamicSinkContext factoryContext = new AsyncDynamicSinkContext(this, context);

    final var prop = factoryContext.getTableOptions();
    final var builder = new S2DynamicTableSink.S2DynamicTableSinkBuilder();

    final var asyncSinkConfig =
        new AsyncSinkConfigurationValidator(prop).getValidatedConfigurations();
    addAsyncOptionsToBuilder(asyncSinkConfig, builder);

    return builder
        .setConsumedDataType(factoryContext.getPhysicalDataType())
        .setEncodingFormat(factoryContext.getEncodingFormat())
        .setClientConfiguration(prop)
        .build();
  }

  @Override
  public String factoryIdentifier() {
    return "s2";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(S2_AUTH_TOKEN);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {

    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    return S2DynamicTableSource.newBuilder()
        .setPhysicalDataType(context.getPhysicalRowDataType())
        .setProducedDataType(context.getPhysicalRowDataType())
        .setDecodingFormat(
            helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT))
        .setSourceConfig(Configuration.fromMap(helper.getOptions().toMap()))
        .build();
  }
}
