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
import s2.flink.source.S2Source;

public class S2UpsertDynamicTableSource implements ScanTableSource {

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType physicalDataType;
  private final Configuration sourceConfig;
  private final DataType producedDataType;
  private final Tuple2<int[], int[]> kvRowIndices;

  public S2UpsertDynamicTableSource(
      @Nullable DataType physicalDataType,
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

        final S2Source<RowData> source = new S2Source<>(sourceConfig, upsertDeserializer);

        return execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "S2UpsertSource");
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
    return null;
  }

  @Override
  public String asSummaryString() {
    return "S2UpsertDynamicTableSource";
  }
}
