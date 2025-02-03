package s2.flink.table;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static s2.flink.config.S2ClientConfig.S2_AUTH_TOKEN;
import static s2.flink.config.S2SinkConfig.S2_SINK_BASIN;
import static s2.flink.config.S2SinkConfig.S2_SINK_STREAM;

import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
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
    var asyncSinkConfig = new AsyncSinkConfigurationValidator(prop).getValidatedConfigurations();
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
    return Set.of(S2_SINK_BASIN, S2_SINK_STREAM, S2_AUTH_TOKEN);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {

    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    ReadableConfig tableOptions = helper.getOptions();

    DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
        helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

    ResolvedCatalogTable catalogTable = context.getCatalogTable();

    S2DynamicTableSource source =
        new S2DynamicTableSource(
            context.getPhysicalRowDataType(),
            Configuration.fromMap(tableOptions.toMap()),
            decodingFormat,
            context.getPhysicalRowDataType());
    return source;
  }
}
