package s2.flink.table.upsert;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.table.AsyncDynamicTableSinkFactory;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;
import s2.flink.properties.S2Config;
import s2.flink.table.upsert.S2UpsertDynamicTableSink.S2UpsertDynamicTableSinkBuilder;

public class S2UpsertDynamicTableFactory extends AsyncDynamicTableSinkFactory {

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
    final S2UpsertDynamicTableSink.S2UpsertDynamicTableSinkBuilder builder =
        new S2UpsertDynamicTableSinkBuilder();

    var asyncSinkConfig = new AsyncSinkConfigurationValidator(prop).getValidatedConfigurations();
    addAsyncOptionsToBuilder(asyncSinkConfig, builder);

    final DataType physicalDataType = context.getPhysicalRowDataType();
    final ResolvedCatalogTable catalogTable = context.getCatalogTable();
    final ResolvedSchema resolvedSchema = catalogTable.getResolvedSchema();

    List<String> keyFields =
        resolvedSchema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of());
    // We need projections, meaning:
    // arrays with the index of physical columns corresponding to key, and to value (should be
    // disjoint)
    //

    final int[] keyPhysicalIndices = keyPhysicalIndices(keyFields, physicalDataType);
    final int[] valuePhysicalIndices = valuePhysicalIndices(keyPhysicalIndices, physicalDataType);
    Preconditions.checkArgument(keyPhysicalIndices.length > 0, "Must be at least one key field.");
    Preconditions.checkArgument(
        valuePhysicalIndices.length > 0, "Must be at least one value field.");

    builder
        .setConsumedDataType(factoryContext.getPhysicalDataType())
        .setEncodingFormat(factoryContext.getEncodingFormat())
        .setKeyValueRowIndices(keyPhysicalIndices, valuePhysicalIndices)
        .setS2ClientProperties(S2Config.fromReadableConfig(prop))
        .setBasin(prop.get(BASIN))
        .setStream(prop.get(STREAM));

    return builder.build();
  }

  private int[] keyPhysicalIndices(
      List<String> keyPhysicalFieldNames, DataType rowPhysicalDataType) {
    final LogicalType rowType = rowPhysicalDataType.getLogicalType();
    Preconditions.checkArgument(rowType.is(LogicalTypeRoot.ROW), "Row data type expected.");
    final List<String> physicalFields = LogicalTypeChecks.getFieldNames(rowType);
    return keyPhysicalFieldNames.stream()
        .mapToInt(
            fieldName -> {
              final var index = physicalFields.indexOf(fieldName);
              Preconditions.checkArgument(index >= 0, "Missing key field name: " + fieldName);
              return index;
            })
        .toArray();
  }

  private int[] valuePhysicalIndices(int[] keyPhysicalIndices, DataType rowPhysicalDataType) {
    final var numFields = LogicalTypeChecks.getFieldCount(rowPhysicalDataType.getLogicalType());
    final Set<Integer> keyFieldSet =
        Arrays.stream(keyPhysicalIndices).boxed().collect(Collectors.toSet());
    return IntStream.range(0, numFields).filter(index -> !keyFieldSet.contains(index)).toArray();
  }

  @Override
  public String factoryIdentifier() {
    return "s2-upsert";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(BASIN, STREAM);
  }
}
