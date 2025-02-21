package s2.flink.table.upsert;

import javax.annotation.Nullable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import s2.flink.config.S2ClientConfig;
import s2.flink.config.S2SourceConfig;
import s2.flink.source.S2Source;

public class S2UpsertDynamicTableSource implements ScanTableSource {

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType physicalDataType;
  private final Configuration sourceConfig;
  private final DataType producedDataType;
  private final Tuple2<int[], int[]> kvRowIndices;

  private S2UpsertDynamicTableSource(
      DataType physicalDataType,
      Configuration sourceConfig,
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType,
      Tuple2<int[], int[]> kvRowIndices) {
    this.physicalDataType = physicalDataType;
    this.sourceConfig = sourceConfig;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
    this.kvRowIndices = kvRowIndices;
  }

  public static S2UpsertDynamicTableSourceBuilder newBuilder() {
    return new S2UpsertDynamicTableSourceBuilder();
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.upsert();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

    var keyDeserializer =
        createDeserialization(runtimeProviderContext, this.decodingFormat, this.kvRowIndices.f0);
    var valueDeserializer =
        createDeserialization(runtimeProviderContext, this.decodingFormat, this.kvRowIndices.f1);

    TypeInformation<RowData> typeInfo =
        runtimeProviderContext.createTypeInformation(producedDataType);

    var upsertDeserializer =
        new UpsertRowDataDeserializer(
            physicalDataType,
            keyDeserializer,
            valueDeserializer,
            this.kvRowIndices.f0,
            this.kvRowIndices.f1,
            typeInfo);

    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        return execEnv.fromSource(
            new S2Source<RowData>(sourceConfig, upsertDeserializer),
            WatermarkStrategy.noWatermarks(),
            "S2UpsertSource");
      }

      @Override
      public boolean isBounded() {
        return false;
      }
    };
  }

  private @Nullable DeserializationSchema<RowData> createDeserialization(
      DynamicTableSource.Context context,
      @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
      int[] projection) {
    if (format == null) {
      return null;
    }
    DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
    return format.createRuntimeDecoder(context, physicalFormatDataType);
  }

  @Override
  public DynamicTableSource copy() {
    return new S2UpsertDynamicTableSource(
        physicalDataType, sourceConfig, decodingFormat, producedDataType, kvRowIndices);
  }

  @Override
  public String asSummaryString() {
    return "S2UpsertDynamicTableSource";
  }

  public static class S2UpsertDynamicTableSourceBuilder {

    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType physicalDataType;
    private Configuration sourceConfig;
    private DataType producedDataType;
    private int[] keyIndices = new int[0];
    private int[] valueIndices = new int[0];

    public S2UpsertDynamicTableSourceBuilder setDecodingFormat(
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
      this.decodingFormat = decodingFormat;
      return this;
    }

    public S2UpsertDynamicTableSourceBuilder setPhysicalDataType(DataType physicalDataType) {
      this.physicalDataType = physicalDataType;
      return this;
    }

    public S2UpsertDynamicTableSourceBuilder setSourceConfig(Configuration sourceConfig) {
      this.sourceConfig = sourceConfig;
      return this;
    }

    public S2UpsertDynamicTableSourceBuilder setProducedDataType(DataType producedDataType) {
      this.producedDataType = producedDataType;
      return this;
    }

    public S2UpsertDynamicTableSourceBuilder setKeyValueRowIndices(
        int[] keyIndices, int[] valueIndices) {
      this.keyIndices = keyIndices;
      this.valueIndices = valueIndices;
      return this;
    }

    public S2UpsertDynamicTableSource build() {
      Preconditions.checkArgument(keyIndices.length > 0);
      Preconditions.checkArgument(valueIndices.length > 0);
      return new S2UpsertDynamicTableSource(
          Preconditions.checkNotNull(physicalDataType, "physicalDataType must be provided"),
          S2SourceConfig.validateForSource(
              S2ClientConfig.validateForSDKConfig(
                  Preconditions.checkNotNull(sourceConfig, "sourceConfig must be provided"))),
          Preconditions.checkNotNull(decodingFormat, "decodingFormat must be provided"),
          Preconditions.checkNotNull(producedDataType, "producedDataType must be provided"),
          Tuple2.of(keyIndices, valueIndices));
    }
  }
}
